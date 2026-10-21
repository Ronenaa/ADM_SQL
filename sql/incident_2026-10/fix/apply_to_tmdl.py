"""Write fix/<table>__fixed.sql into that table's partition in the semantic model (TMDL).

Encodes the SQL as an M text literal (" -> "", newline -> #(lf), tab -> #(tab)), replaces the
existing [Query="..."] string, and checks the round trip decodes back to the same SQL.

Usage (from the repo root):
    python sql/incident_2026-10/fix/apply_to_tmdl.py factGain factPurchaseExpenses
"""
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[2]
sys.path.insert(0, str(HERE.parent))
from extract_queries import m_query_to_sql, CURRENT_DIR  # noqa: E402


def sql_to_m(sql: str) -> str:
    if "#(" in sql:
        raise SystemExit("SQL contains '#(' - would be read as an M escape; escape it first")
    return sql.replace('"', '""').replace("\n", "#(lf)").replace("\t", "#(tab)")


def apply(table: str) -> None:
    tmdl_path = REPO / CURRENT_DIR / f"{table}.tmdl"
    sql = (HERE / f"{table}__fixed.sql").read_text(encoding="utf-8-sig").replace("\r\n", "\n").rstrip("\n")
    tmdl = tmdl_path.read_text(encoding="utf-8")

    start = tmdl.index('[Query="') + len('[Query="')
    i = start
    while True:  # find the closing quote of the M string ("" is an escaped quote)
        if tmdl[i] == '"':
            if tmdl[i + 1] == '"':
                i += 2
                continue
            break
        i += 1
    new = tmdl[:start] + sql_to_m(sql) + tmdl[i:]

    if m_query_to_sql(new) != sql:
        raise SystemExit(f"{table}: round trip mismatch - TMDL not written")
    tmdl_path.write_text(new, encoding="utf-8")
    print(f"updated {tmdl_path.relative_to(REPO)}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        raise SystemExit(__doc__)
    for t in sys.argv[1:]:
        apply(t)
