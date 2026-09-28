# factLastQtySteps — working files

Average price of the last N units sold, in 500-unit steps (500 … 10,000),
pooled per Article. Feeds the matrix on the Sales report's Product Risk
Analysis page.

## IMPORTANT: these files are reference copies, NOT the live source

The live query is the M partition inside
`PBI/SM/ADM - DS.SemanticModel/definition/tables/factLastQtySteps.tmdl`,
per the convention in PROJECT_SCOPE.md §3 (the .tmdl partition is the single
source of truth for every table's SQL).

These copies exist because the query is ~570 lines and is far easier to read,
diff and run standalone here than as an escaped one-line M string. **If you
change one, change the other**, exactly as factGain / factPurchaseExpenses
already do for their shared CTEs.

| File | Purpose |
|---|---|
| `factLastQtySteps.sql` | The query, readable. Run it standalone (see below). |
| `factLastQtySteps.m`   | The same query wrapped as the M partition expression. Generated from the .sql — do not hand-edit. |
| `qa_factLastQtySteps.sql` | Six automated correctness checks. All must return 0 violations. |
| `qa_compare.sql` | Step ladder vs a plain all-history average, per article. Eyeball check. |

## Running them

```
powershell -ExecutionPolicy Bypass -File .\scratch_query.ps1 .\sql\factLastQtySteps\factLastQtySteps.sql
powershell -ExecutionPolicy Bypass -File .\scratch_query.ps1 .\sql\factLastQtySteps\qa_factLastQtySteps.sql
```

Runtime ~25s. Expect 261 rows (13 articles x 20 steps).

## Notes on the logic

- Tail is pooled **per Article**, not per ItemKey — "last 500 of Corn" means
  500 units across all Corn item codes combined.
- Sorted by `OrderCreateTime DESC, OrderID DESC`. OrderID alone disagrees with
  the true creation order on 10 rows (both dates in 2021, outside any tail's
  reach), which is why the timestamp is the primary key.
- Rows with no price for a given view are skipped entirely — they consume no
  tail position and appear in neither numerator nor denominator.
- One shared `Qty` column (the Order-view unit count) divides all three price
  views. Where a CIF or FOT price is missing on some rows that view is
  understated; currently affects Oils (FOT) only, and BPP has no CIF prices.
- The `#am` / `#raw` / `#ob` temp tables are required for performance, not
  cosmetic: without them the query times out past 180s. See the comments
  inline.
