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

## DAX twin on factOrders (`Last Qty Steps (Orders)`)

Same ladder, computed by measures over `factOrders`, so selecting a step in the
matrix can filter the orders table (`[In Last Qty Step] = 1` visual filter).

- **SQL helper:** the factOrders partition adds `LQ_RunQtyBefore` — Order-view
  priced units sold in the same Article *after* this order (same ORDER BY as
  here). Blank for rows outside the tail (unpriced, or no Article). The
  article map is a CTE copy of `#am`; the full factOrders query still runs in ~1 min.
- **Per-view tail:** the three SQL columns above feed the measures; `[LQO Tail Total Qty]`,
  `[Total Last Qty Value (Orders)]`, `[Last Qty Tail Anchor (Orders)]`, `[In Last Qty Step]`
  and `[Short AVG Last Qty Price (Orders)]` each `SWITCH` on `MIN(PriceType[ID])` (2 = CIF, 3 = FOT, else Order).
- **Measures:** `AVG Last Qty Price - Risk Plan (Orders)` (bind this),
  `Total Last Qty Value/Quantity (Orders)`, `Last Qty Tail Anchor (Orders)`,
  `Last Qty Step Note (Orders)`, `In Last Qty Step`. The step comes from the
  disconnected `dimQtyStep`; only Article and PriceType filters are honoured.
  Single article per evaluation (the Risk Plan wrapper defaults to Corn), so no
  iterators.
- **One tail per price view (changed 2026-10-21).** Each view has its own tail
  position column on factOrders: `LQ_RunQtyBefore` (Order, `PricedQuantity`),
  `LQ_RunQtyBeforeCF` (`PricedQuantityCF`), `LQ_RunQtyBeforeFOT`
  (`PricedQuantityFOT`). An order with a blank/zero price in the selected view is
  skipped in that view: no tail position, nothing in numerator or denominator —
  the same rule as this table. The measures pick the column from `PriceType[ID]`.
  Before this change all three views shared the Order-view tail, so an order with
  no CIF price (e.g. 147756, 38 t) sat in the tail at value 0 and dragged the CIF
  average down (CGFP last 500: $310 instead of ~$335). QA on 2026-10-05
  (before the change) had shown CIF/FOT differing from this table only for
  FW CIF and Oils CIF/FOT — those gaps were this same effect.

## Short average price (card next to the step matrix)

The same tail, but N = the short instead of a 500-unit step. The short is
`-[Remain - Risk Plan]` when Remain is negative. Example: Remain total = (2,270),
so the card shows the average price of the last 2,270 units.

- **Measures (factOrders, same folder):** `Short Qty (Orders)` (hidden helper),
  `Short AVG Last Qty Price (Orders)` (core, single article),
  `Short AVG Last Qty Price - Risk Plan (Orders)` (bind this one; Corn default
  matches `[Switch Measures Show]`), `Short Last Qty Title (Orders)` (dynamic
  card title: "Short Avg - last X t", or "No Short").
- **Card (Sales report, Product Risk Analysis):** visual `5c0a7d2e9f1b3a4c6d8e`,
  right of the step matrix. It carries the same `Show Next 6 Months = true`
  visual filter as the Stock/Purchase/Sales/Remain matrix, so the short equals that
  matrix's Remain **Total** column. Transaction Type / Customer slicers affect the
  short (via Remain) but not the tail prices, as with the steps.
- If the article's priced history is shorter than the short, the whole history
  is averaged and the title adds "(only Y sold)".
- Blank price with title "No Short" when Remain >= 0.
