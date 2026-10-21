"""Add WITH (NOLOCK) to the expense-table references in the fact-table queries of the semantic model.

Project rule: fact queries read the expense tables (HOTSAOT_*, see EXPENSE_TABLES) with NOLOCK, so the
refresh never blocks, or is blocked by, the client's writers (see sql/incident_2026-10/). Other tables
are left as they are. Run this after adding/changing any fact table; it is idempotent (tables that
already carry a hint are left alone).

Skipped on purpose: CTE names, #temp tables, subqueries "( ... )", table functions, VALUES,
and references that already have a WITH (...) hint.

Usage (from the repo root):
    python sql/add_nolock.py            # all fact*.tmdl in the model
    python sql/add_nolock.py factGain   # only some tables
    python sql/add_nolock.py --check    # report only, write nothing
    python sql/add_nolock.py --strip    # first remove every WITH (NOLOCK), then add the expense-table ones
"""
import re
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "sql" / "incident_2026-10"))
sys.path.insert(0, str(REPO / "sql" / "incident_2026-10" / "fix"))
from extract_queries import m_query_to_sql, CURRENT_DIR  # noqa: E402
from apply_to_tmdl import sql_to_m  # noqa: E402

HINT = " WITH (NOLOCK)"
EXPENSE_TABLES = re.compile(r"^(\w+\.)*HOTSAOT_\w+$", re.I)  # last part of the name, brackets removed
BOM, LF, CRLF = chr(0xFEFF), chr(10), chr(13) + chr(10)
# words that can follow a table name but are not an alias
NOT_ALIAS = {
    "where", "group", "order", "having", "union", "inner", "left", "right", "full", "cross", "outer",
    "join", "on", "with", "option", "for", "except", "intersect", "pivot", "unpivot", "tablesample",
    "select", "set", "using", "when", "then", "else", "end", "and", "or", "from", "into", "go",
}
IDENT = re.compile(r'\[[^\]]*\]|"[^"]*"|[#@]?[A-Za-z_\u0080-￿][\w$#@\u0080-￿]*')


def mask(sql: str) -> str:
    """Same length as sql, with comments and string literals blanked out (newlines kept)."""
    out, i, n = [], 0, len(sql)
    while i < n:
        two = sql[i:i + 2]
        if two == "--":
            j = sql.find("\n", i)
            j = n if j < 0 else j
            out.append(" " * (j - i)); i = j
        elif two == "/*":
            j = sql.find("*/", i + 2)
            j = n if j < 0 else j + 2
            out.append("".join(c if c == "\n" else " " for c in sql[i:j])); i = j
        elif sql[i] == "'":
            j = i + 1
            while j < n:
                if sql[j] == "'":
                    if sql[j + 1:j + 2] == "'":
                        j += 2; continue
                    break
                j += 1
            j = min(j + 1, n)
            out.append("".join(c if c == "\n" else " " for c in sql[i:j])); i = j
        else:
            out.append(sql[i]); i += 1
    return "".join(out)


def add_nolock(sql: str):
    m = mask(sql)
    ctes = {x.lower() for x in re.findall(r"(?:\bwith\b|,)\s*(\w+)\s+as\s*\(", m, re.I)}
    inserts, skipped, warnings = [], [], []
    for kw in re.finditer(r"\b(from|join)\b", m, re.I):
        pos = kw.end()
        # FROM inside a function call, e.g. EXTRACT(x FROM y) / TRIM(c FROM s): previous unmatched "(" is not a subquery
        if kw.group(1).lower() == "from":
            depth, k = 0, kw.start() - 1
            while k >= 0:
                c = m[k]
                if c == ")": depth += 1
                elif c == "(":
                    if depth == 0: break
                    depth -= 1
                k -= 1
            if k >= 0 and not re.match(r"\s*(select|with)\b", m[k + 1:], re.I):
                continue
        p = pos
        while p < len(m) and m[p].isspace(): p += 1
        if p >= len(m) or m[p] == "(":
            continue  # subquery / derived table
        # dotted multi-part name
        end, first = p, None
        while True:
            t = IDENT.match(m, end)
            if not t: break
            first = first or t.group(0)
            end = t.end()
            if m[end:end + 1] == ".": end += 1
            else: break
        if first is None:
            continue
        name = m[p:end]
        if m[end:end + 1] == "(" or re.match(r"\s*\(", m[end:]):
            skipped.append(name + "()"); continue  # table function
        bare = name.strip("[]\"").lower()
        if name.startswith("#") or name.startswith("@") or bare in ctes or bare == "values" or bare.startswith("sys."):
            skipped.append(name); continue
        if not EXPENSE_TABLES.match(bare.replace("].[", ".").replace("[", "").replace("]", "")):
            continue  # only the expense tables get the hint
        # optional alias
        q = end
        while q < len(m) and m[q].isspace(): q += 1
        if re.match(r"as\b", m[q:], re.I):
            q += 2
            while q < len(m) and m[q].isspace(): q += 1
            a = IDENT.match(m, q)
            if a: end = a.end()
        else:
            a = IDENT.match(m, q)
            if a and a.group(0).lower() not in NOT_ALIAS and not a.group(0).startswith("#"):
                end = a.end()
        if re.match(r"\s*with\s*\(", m[end:], re.I):
            continue  # already hinted
        if re.match(r"\s*,", m[end:]) and kw.group(1).lower() == "from":
            warnings.append(f"comma-joined FROM list near: {sql[p:p + 60]!r}")
        inserts.append((end, name))
    for end, _ in sorted(inserts, reverse=True):
        sql = sql[:end] + HINT + sql[end:]
    return sql, [n for _, n in inserts], skipped, warnings


def process(table: str, write: bool) -> None:
    path = REPO / CURRENT_DIR / f"{table}.tmdl"
    tmdl = path.read_bytes().decode("utf-8")  # keep CRLF as is
    if '[Query="' not in tmdl:
        print(f"{table}: no embedded query, skipped"); return
    sql = m_query_to_sql(tmdl)
    if "--strip" in sys.argv:
        sql = sql.replace(HINT, "")
    new_sql, added, skipped, warnings = add_nolock(sql)
    print(f"{table}: +{len(added)} NOLOCK  ({', '.join(sorted(set(added)))})")
    print(f"    left alone: {', '.join(sorted(set(skipped))) or '-'}")
    for w in warnings: print("    WARNING:", w)
    if (not added and new_sql == m_query_to_sql(tmdl)) or not write:
        return
    start = tmdl.index('[Query="') + len('[Query="')
    i = start
    while True:
        if tmdl[i] == '"':
            if tmdl[i + 1] == '"': i += 2; continue
            break
        i += 1
    out = tmdl[:start] + sql_to_m(new_sql) + tmdl[i:]
    if m_query_to_sql(out) != new_sql:
        raise SystemExit(f"{table}: round trip mismatch - not written")
    path.write_bytes(out.encode("utf-8"))


def _read(path: Path):
    """(text with LF newlines, had BOM, had CRLF) - so a file can be written back in its own style."""
    raw = path.read_bytes().decode("utf-8")
    return raw.lstrip(BOM).replace(CRLF, LF), raw.startswith(BOM), CRLF in raw


def _write(path: Path, text: str, bom: bool, crlf: bool) -> None:
    path.write_bytes(((BOM if bom else "") + (text.replace(LF, CRLF) if crlf else text)).encode("utf-8"))


def process_reference_copies() -> None:
    """Keep the readable reference copies (see PROJECT_SCOPE.md) in sync with the TMDL partitions."""
    for table in ("factGain", "factPurchaseExpenses"):  # fix/<table>__fixed.sql mirrors the partition
        ref = REPO / "sql" / "incident_2026-10" / "fix" / f"{table}__fixed.sql"
        if ref.exists():
            old, bom, crlf = _read(ref)
            sql = m_query_to_sql((REPO / CURRENT_DIR / f"{table}.tmdl").read_bytes().decode("utf-8"))
            _write(ref, sql + (LF if old.endswith(LF) else ""), bom, crlf)
            print(f"synced {ref.relative_to(REPO)}")
    d = REPO / "sql" / "factLastQtySteps"  # standalone query: .sql is the source, .m is generated from it
    sql_path, m_path = d / "factLastQtySteps.sql", d / "factLastQtySteps.m"
    old, bom, crlf = _read(sql_path)
    new_sql, added, _, warnings = add_nolock(old.replace(HINT, "") if "--strip" in sys.argv else old)
    for w in warnings: print("    WARNING:", w)
    if new_sql != old:
        _write(sql_path, new_sql, bom, crlf)
        mt, mbom, mcrlf = _read(m_path)
        start = mt.index('[Query="') + len('[Query="')
        i = start
        while True:
            if mt[i] == '"':
                if mt[i + 1] == '"': i += 2; continue
                break
            i += 1
        _write(m_path, mt[:start] + sql_to_m(new_sql) + mt[i:], mbom, mcrlf)
    print(f"factLastQtySteps.sql/.m: +{len(added)} NOLOCK")


if __name__ == "__main__":
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    tables = args or sorted(p.stem for pat in ("fact*.tmdl", "dim*.tmdl") for p in (REPO / CURRENT_DIR).glob(pat))
    for t in tables:
        process(t, write="--check" not in sys.argv)
    if not args and "--check" not in sys.argv:
        process_reference_copies()
