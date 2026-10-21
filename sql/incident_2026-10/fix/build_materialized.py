"""Build materialized versions of heavy model queries - same logic, heavy CTEs computed once.

Why: SQL Server does not cache a CTE's result; every reference re-runs it. A CTE that is
referenced several times (directly or through other CTEs) is executed several times, and every
execution re-reads the source tables - including the expense tables (HOTSAOT_*) that the client
saw locked. The fix splits the single WITH into several statements, each filling a temp table
that later statements read. No CTE body is edited; only references to a materialized CTE are
renamed to its temp table.

  factGain              270s -> ~100s on STG (2026-10-12)
  factPurchaseExpenses  see README.md

Source is the CTE version as committed before the fix (the working-tree TMDL may already hold
the fixed query). Output: fix/<table>__fixed.sql - apply it with apply_to_tmdl.py.

Usage (from the repo root):  python sql/incident_2026-10/fix/build_materialized.py [table ...]
"""
import re
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[2]
sys.path.insert(0, str(HERE.parent))
from extract_queries import m_query_to_sql, CURRENT_DIR  # noqa: E402

# Each statement: (CTEs it defines, CTE whose result goes into #temp - None for the final
# statement, CTEs it reads from earlier temp tables). CTEs needed by more than one statement
# (e.g. CurrencyConvertion, a cheap one) are simply repeated.
CONFIG = {
    "factGain": {
        "source_commit": "96f01d2",
        "statements": [
            (["CurrencyConvertion", "totals_raw", "totals", "exchange_movements"], "exchange_movements", []),
            (["CurrencyConvertion", "exchange_p_orders", "exchange_priced", "Swap_Moves", "Swap_RootSupplier",
              "purchase_orders"], "purchase_orders", ["exchange_movements"]),
            (["po_doctype", "Swap_WinningMatch", "P_costs"], "P_costs", ["purchase_orders"]),
            (["CurrencyConvertion", "inv", "sales", "base_link", "CIF_Qty", "WH_sales", "WH_prices"], None,
             ["P_costs"]),
        ],
    },
    "factPurchaseExpenses": {
        "source_commit": "96f01d2",
        "statements": [
            (["CurrencyConvertion", "totals_raw", "totals", "main", "base"], "base", []),
            (["purchase_orders", "final", "final2"], "final2", ["base"]),
            (["LastCreation", "LastVersion", "OrderPrices"], "OrderPrices", []),
            (["CurrencyConvertion", "sales", "base_link", "CIF_Qty"], "CIF_Qty", []),
            (["CurrencyConvertion", "base_expenses"], "base_expenses", ["OrderPrices", "final2"]),
            (["PO_OrderQty"], None, ["CIF_Qty", "base_expenses"]),
        ],
    },
}

CTE_START = re.compile(r"^\s*(?:with\s+|,\s*)?(\w+)\s+as\s*\(\s*$", re.IGNORECASE)


def split(sql: str, needed):
    """Return (prelude, {cte_name: 'name AS ( ... )'}, final_select)."""
    lines = sql.split("\n")
    starts = [(m.group(1), i) for i, line in enumerate(lines) if (m := CTE_START.match(line))]
    missing = set(needed) - {n for n, _ in starts}
    if missing:
        raise SystemExit(f"CTE(s) not found: {missing}")

    prelude = "\n".join(lines[:starts[0][1]])
    ctes = {}
    for k, (name, s) in enumerate(starts):
        e = starts[k + 1][1] if k + 1 < len(starts) else len(lines)
        seg = lines[s:e]
        seg[0] = f"{name} AS ("
        ctes[name] = seg

    # The last CTE is followed by the final SELECT: split at the line that closes it.
    last = ctes[starts[-1][0]]
    close = next(i for i, l in enumerate(last) if i > 0 and re.match(r"^\s*\)\s*$", l))
    final = "\n".join(last[close + 1:])
    ctes[starts[-1][0]] = last[:close + 1]

    # Drop the trailing comma that separated CTEs (the last ')' of a body may be '),').
    for name, seg in ctes.items():
        for i in range(len(seg) - 1, -1, -1):
            code = seg[i].split("--")[0].rstrip()
            if code:
                if code.endswith(","):
                    seg[i] = seg[i].replace(code, code[:-1], 1)
                break
        ctes[name] = "\n".join(seg).rstrip()
    return prelude, ctes, final


def rename(text: str, names) -> str:
    """Point table references (after FROM / JOIN) at the temp table. A CTE name can also be a
    column name (CIF_Qty is both), so bare occurrences are left alone."""
    for n in names:
        text = re.sub(rf"(\b(?:FROM|JOIN)\s+){n}\b", rf"\1#{n}", text, flags=re.IGNORECASE)
    return text


def build(table: str) -> str:
    cfg = CONFIG[table]
    statements = cfg["statements"]
    temps = [s[1] for s in statements if s[1]]
    tmdl = subprocess.run(
        ["git", "-C", str(REPO), "show", f"{cfg['source_commit']}:{CURRENT_DIR}/{table}.tmdl"],
        check=True, capture_output=True,
    ).stdout.decode("utf-8")
    prelude, ctes, final = split(m_query_to_sql(tmdl), {c for s in statements for c in s[0]})

    out = [
        f"-- {table} - materialized version (see sql/incident_2026-10/fix/build_materialized.py).",
        "-- Same logic as the CTE version; CTEs used more than once are computed once into temp tables.",
        "SET NOCOUNT ON;",
    ]
    out += [f"IF OBJECT_ID('tempdb..#{t}') IS NOT NULL DROP TABLE #{t};" for t in temps]
    out.append(prelude.strip())
    out.append("")

    for cte_names, target, reads in statements:
        # Comma on its own line: a CTE body can end with a comment line, which would swallow it.
        body = "\n,\n".join(rename(ctes[n], reads) for n in cte_names)
        out.append(f"-- {'=' * 70}")
        if target:
            out.append(f"-- Step: materialize {target}")
            out.append(f";WITH {body}\nSELECT * INTO #{target} FROM {target};\n")
        else:
            out.append("-- Step: final result")
            out.append(f";WITH {body}\n{rename(final, reads)}")
    return "\n".join(out).rstrip() + "\n"


if __name__ == "__main__":
    for t in sys.argv[1:] or CONFIG:
        text = build(t)
        path = HERE / f"{t}__fixed.sql"
        path.write_text(text, encoding="utf-8-sig")
        print(f"wrote {path.relative_to(REPO)} ({text.count(chr(10))} lines)")
