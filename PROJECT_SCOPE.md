# ADM — Project Scope

> **Keep this file updated.** Edit it whenever files are added/removed, or FactGain logic changes.

---

## 1. Project Purpose

ADM is a data-warehouse / BI project. This repository holds the SQL that builds the fact and
dimension queries feeding a Power BI **semantic model** (`PBI/SM/`) and a set of standalone Power BI
**reports** (`PBI/Reports/`). The SQL layer is the source of truth for the data; the semantic model
exposes it for analysis, and the reports consume the semantic model (or, in some cases, ship as
self-contained PBIP reports).

## 2. Repository Structure

```
ADM_SQL/
├── PROJECT_SCOPE.md          this file
├── FactGain_scope.md         FactGain CTE-by-CTE working notes (moved from sql/facts/, see below)
├── scratch_query.ps1         ad-hoc runner for testing any .sql file against the live DB
└── PBI/
    ├── SM/
    │   └── ADM - DS.*/        Power BI semantic model project (PBIP + TMDL) and its bound report
    └── Reports/
        ├── Sales.*/            standalone Power BI report
        └── Purchase Expenses.*/  standalone Power BI report (new)
```

---

## 3. SQL queries — no standalone mirror folder

There used to be a `sql/facts/` and `sql/dimension/` folder mirroring each fact/dimension table's
query as a standalone `.sql` file. Both were removed: Power BI never read from them (each table's
real query is the literal SQL string embedded in that table's `partition ... = m` block in its
`.tmdl` file), and keeping a hand-synced copy in two places was a recurring source of drift — e.g.
`factOrders.sql` had gone stale and silently held an unrelated query. **The `.tmdl` partition is
now the single source of truth for every table's SQL.** To read or test a table's query, open its
`.tmdl` file's `partition` block, or copy the query out to a scratch `.sql` file for ad-hoc testing
(e.g. via `scratch_query.ps1`) and discard it afterward rather than committing it back to a mirror
folder.

### Exception: `sql/factLastQtySteps/`

One folder does exist under `sql/`, added for `factLastQtySteps`. Its query is ~570 lines (it
embeds the whole factOrders base as a CTE, then a window-function step ladder) and is effectively
unreadable and undiffable as an escaped one-line M string. The folder holds a readable copy of the
query, the generated `.m`, and two QA scripts.

**This does not change the rule above.** The `.tmdl` partition is still the live source; the `.sql`
is a reference copy and the two must be kept in sync, exactly as `factGain` / `factPurchaseExpenses`
already duplicate their shared CTEs. See [sql/factLastQtySteps/README.md](sql/factLastQtySteps/README.md).

**DAX twin on factOrders (2026-10-05).** The same step ladder also exists as measures on
`factOrders` (display folder `Last Qty Steps (Orders)`), so the step matrix can filter the orders
table on the Product Risk Analysis page. The factOrders partition carries one helper column,
`LQ_RunQtyBefore` (running Order-view priced quantity per Article, newest first), and the
measures read `dimQtyStep` (disconnected from factOrders) for the step. Details and the one known
difference from the SQL table are in the README above.

**Short average price (2026-10-05).** A card next to the step matrix ("Recent Orders" panel) shows
the average price of the last N units sold, where N is the short: the negative Remain total
of the Stock/Purchase/Sales/Remain matrix. Same tail logic as the steps, with N in place of the
`dimQtyStep` step. Measures and card details are in the README.

### Rule: expense tables are read with `WITH (NOLOCK)` in fact and dim queries (2026-10-18)

In fact and dimension table queries, every reference (`FROM` / `JOIN`) to an expense table - `HOTSAOT_COTROT`,
`HOTSAOT_SHOROT`, `HOTSAOT_SHROTIM_New` (the tables the client's writers lock, see the lock incident
below) - carries `WITH (NOLOCK)`. All other tables are read normally. (Briefly all 13 fact tables got
NOLOCK on every table the same day; narrowed to the expense tables at the user's request.) Applies to
**every new fact/dim table and every query change**: write the query, then run

```
python sql/add_nolock.py            # all fact*.tmdl and dim*.tmdl (+ syncs the reference copies)
python sql/add_nolock.py --check    # report only
python sql/add_nolock.py --strip    # first remove every WITH (NOLOCK), then re-add the expense-table ones
```

Today it touches `factGain` (7 references), `factPurchaseExpenses` (8), `dimItems` (2),
`dimItemsSimple` (1), `dimPNL` (1), `dimPurchaseOrderNumber` (3) and `dimSubPurchaseOrderNumber` (2);
no other table reads an expense table. To cover more tables edit `EXPENSE_TABLES` in the script.

The script edits the `.tmdl` partitions in place (CRLF kept, idempotent, round-trip checked). It
skips CTE names, `#temp` tables, subqueries, table functions and `sys.*`, and syncs
`fix/factGain|factPurchaseExpenses__fixed.sql` and `sql/factLastQtySteps/`. Versus the previous commit
the only change is the added hints. Syntax checked on STG with `SET PARSEONLY ON`.

- `fix/build_materialized.py` rebuilds those two queries from commit `96f01d2`, which has no
  hints - **re-run `add_nolock.py` after it**.
- The incident files (`queries/*`, `02_measure_all.sql`, `03b_*`) were left as generated, so the
  before/current timings stay comparable.
- NOLOCK allows dirty reads; accepted for these reads.

### Exception: `sql/incident_2026-10/` (lock incident investigation)

Added 2026-10-12 after the client KILLed a blocking `BIX` session on the live server (expense
tables `HOTSAOT_*`, 4 times in ~1.5 weeks). Holds SSMS-ready scripts for the investigation:
environment check (`01`), before/current timing of the 5 queries that touch the expense tables or
`tblOrderPriceS` (`02`), a lock simulation on STG (`03a`–`03d`), and a blocking monitor
to hand to the client's DBA (`03c`, needs VIEW SERVER STATE, which BIX lacks). `queries/*.sql`,
`02_measure_all.sql` and `03b_reader_*.sql` are **generated** by `extract_queries.py` from the TMDL
partitions (current) and commit `5b60782` (before the Aug-30 changes), with every query inlined so
no SQLCMD mode is needed: regenerate, don't hand-edit.
Write scripts guard on `@@SERVERNAME = ADM-SQL2020\ADMNEW` (STG). Note: `SQL Address` in the model
(213.8.5.61) is STG; the published model is pointed at the client's live server, so STG numbers
are relative only. Plan and results tables: [sql/incident_2026-10/README.md](sql/incident_2026-10/README.md).

`fix/` holds the fix for `factGain` and `factPurchaseExpenses`: CTEs referenced more than once are
materialized into temp tables, no CTE body changed (`build_materialized.py` → `*__fixed.sql` →
`apply_to_tmdl.py`). STG: factGain 270s → ~100s, factPurchaseExpenses 295s → ~54s. Both queries
have **pre-existing** non-deterministic `ROW_NUMBER` ties (factGain `inv`; factPurchaseExpenses
`final`, month-level `Purchase_Date`) that need a business tie-break rule — see the README.

`FactGain_scope.md` (CTE-by-CTE design rationale for the Gain query) was kept and moved to the
repo root, since it's working documentation, not a SQL mirror — see [FactGain_scope.md](FactGain_scope.md).

---

## 5. FactGain — CTE Chain

```
CurrencyConvertion
  └─ totals_raw → totals
       └─ exchange_movements → purchase_orders → exchange_priced
            └─ Purchase_Exchange (Invoice | Import | Exchange branches)
                 └─ inv
                      └─ P_costs
                           └─ sales (Invoices | Delivery Notes)
                                └─ base_link
                                     └─ WH_sales → WH_prices
                                          └─ final SELECT: UNION ALL of
                                               Branch 1 (Import/Exchange, cost from P_costs)
                                               Branch 2 (Warehouse, cost from inv)
```

### Backup convention (historical)

`FactGain.sql`/`FactGain_BU.sql` no longer exist as standalone files (see [Fact-table
queries](#3-fact-table-queries) above) — `factGain.tmdl`'s partition is now the only copy of this
query, and git history is the backup. This section is kept for context on the old workflow: before
pushing a change to `FactGain.sql`, the convention was to first overwrite `FactGain_BU.sql` with
the currently committed version of `FactGain.sql`, so `_BU` held the version live immediately
before the incoming change — a one-step-back safety copy, not a fixed historical snapshot.

## 6. FactGain — Change Log

### 13. Last Qty Steps (Orders): CIF/FOT tail skips unpriced orders (2026-10-21 — committed; needs model refresh + publish)
An order with no CIF price (e.g. 147756, 38 t) was in the shared Order-view tail at value 0, so "Last 500" CIF read ~$310 instead of ~$335. `factOrders` SQL now adds `LQ_RunQtyBeforeCF` / `LQ_RunQtyBeforeFOT` (tail positions over `PricedQuantityCF` / `PricedQuantityFOT`); the Last Qty Steps (Orders) measures `SWITCH` on `PriceType[ID]` to use the view's own column. Order view is unchanged (verified: 149,120 before = after for Corn step 500). Details in `sql/factLastQtySteps/README.md`. The generated mirrors `sql/incident_2026-10/queries/factOrders__*.sql` and `02_measure_all.sql` were not regenerated.

### 12. Swap source must be a real ship purchase (2026-10-21, PREPARED — not committed, pending client approval)
Internal swap POs such as 20007741 (`2000%`, supplier 146, NIS, no ship) were treated as 'P' purchases and could win `exchange_priced` as a swap's source, instead of the last real ship (e.g. 20007011, MARO MARO). Fix: `exchange_p_orders` now keeps only orders that join `PurchaseOrderLines → ShipsArrivals → ShipList` (same join as `ship_sub`). Swaps whose only candidates have no ship now get no price. Edited `fix/factGain__fixed.sql` directly and pushed to the TMDL with `apply_to_tmdl.py` — re-running `build_materialized.py` regenerates from commit 96f01d2 and would drop it. The diagnostic queries used to find the cause were temporary and removed. Verify on STG: swap row's `MatchedImportPO` for 20007741-style cases is now the ship-backed order.

### 11. Heavy CTEs materialized into temp tables (2026-10-12)
Lock-incident fix (see `sql/incident_2026-10/`). SQL Server re-runs a CTE at every reference, and
`purchase_orders` (with the whole `totals` → `exchange_movements` → `exchange_priced` chain and the
non-sargable `CONCAT` join to `PurchaseOrderLines`) was referenced by `po_doctype`,
`Swap_WinningMatch` and `P_costs`, with `P_costs` joined twice in the final SELECT. The query is now
four statements: `#exchange_movements`, `#purchase_orders`, `#P_costs`, then the final result
(`SET NOCOUNT ON`, temp tables dropped up front). **No CTE body changed.** The partition is
generated by `sql/incident_2026-10/fix/build_materialized.py` (from commit `96f01d2`) and
`apply_to_tmdl.py`. STG: 270s → ~100s. QA vs the CTE version: same 105,427 rows; differences only
float noise and 411 Warehouse rows from a **pre-existing** non-deterministic tie in `inv`
(`month_rn` orders by `[Date]` only; 14 item-months have same-day rows with different prices).

### 10. Removed temporary `test` wrapper CTE
Dropped the ad-hoc `,test as ( ... )` CTE and trailing `SELECT * FROM test` that had been added to
isolate/debug the two-branch union during development. The query now again ends directly on the
Branch 1 / Branch 2 `UNION ALL` — no logic change, debugging cleanup only.

### 9. `inv` decoupled from supplier + `WH_prices` CTE + price columns renamed

- **`inv` keyed by item + month only**: dropped `SupplierKey` from both the output and the
  `PARTITION BY` (and from the regular-warehouse and 1144 source subqueries). Price is set by
  item+date, not by warehouse — any warehouse shares the same item price for a given month.
  Output is now just `ItemKey, [Date], WH_Price`.
- **Warehouse branch join simplified**: Branch 2 now joins `inv` on `ItemKey` + month only
  (removed `inv.SupplierKey = s.SupplierWarehouse`). The redundant `LEFT JOIN CurrencyConvertion CC`
  in Branch 2 was removed.
- **New `WH_prices` CTE** (after `WH_sales`): derives three price columns from the aggregated
  warehouse sale —
  - `Item_Price` = `(LineTotalNet_USD - AdditionalLineCost) / Quantity`
  - `Storage_Price` = `AdditionalLineCost / AdditionalQuantity`
  - `Total_Price` = `Item_Price + Storage_Price` (NULL when `AdjustmentFlag = '1'`)
- **`AdditionalQuantity`** added to `WH_sales` (`SUM(s.AdditionalQuantity)`) to support `Storage_Price`.
- **Price column rename**: single `price_usd` column replaced by `Item_Price`, `Storage_Price`,
  `Total_Price` in both branches. Branch 1 sets `Item_Price`/`Storage_Price` to NULL and maps the
  old per-unit calc to `Total_Price`; Branch 2 passes the three values through from `WH_prices`.

### 7. Warehouse branch — field improvements

- **`ShipID`** (boat): NULL — not applicable for warehouse rows
- **`Qty_flag`**: `ROW_NUMBER() OVER (PARTITION BY SupplierWarehouse, ItemKey, inv.YearMonth ORDER BY inv.YearMonth DESC)` — flags the first row per warehouse/item/month combination
- **`WarehouseName`** and GORMIM W join in `WH_sales`: currently commented out
- **`ValueDate`**: `CAST(inv.YearMonth + '-01' AS DATE)` — first day of the inventory month; NULL if no inv match
- **`[Purchase Quantity]`**: `SUM(Quantity) OVER (PARTITION BY SupplierWarehouse, [Year-Month])` — total qty shipped out of that warehouse in that month
- **`CIF_Purchase`**: NULL (no CIF concept for warehouse sales)
- **`DischargeCost`**: hardcoded `50` for all warehouse rows
- **`TransactionType`**: added to `sales` delivery note branch and propagated through `WH_sales` to final output. Logic: `MCHIR_ICH=0 AND W.QOD_GORM IS NOT NULL → G.AOPI_PEILOT`, `MCHIR_ICH=0 AND W.QOD_GORM IS NULL → 'החלפה'`, else NULL. Invoice branch = NULL.
- **Internal doc filter**: `AND TM.QOD_SHOLCH <> TM.QOD_MQBL` added to delivery note WHERE — removes rows where source = destination (internal transfers)
- **`WH_sales` no WHERE filter**: all sales rows are included in `WH_sales`; warehouse identification is done downstream via the `base_link` join. `WHERE bl.DeliveryNote IS NULL` is intentionally not used here.

### 1. Warehouse Sales branch added
- **`WH_sales` CTE** (after `base_link`): identifies warehouse delivery notes by LEFT OUTER JOIN to `base_link` — rows where `bl.DeliveryNote IS NULL` have no purchase order link and therefore come from a warehouse.
- Aggregates multiple lines per delivery note into one row: `ItemKey`, `UnitNetPriceUSD`, `SalesType` from `'Item'` line only; `Quantity` = item qty only; `LineTotalNet_USD` = sum of all lines (item + storage fees).
- **`MultiLineFlag`**: `1` if delivery note had >1 original sales lines (e.g. item + storage fee), `0` if single line.
- Cost basis: `inv.LastCFPrice` (C&F flat price from inventory).

### 2. Final `UNION ALL` result
Two branches, identical 31-column schema:

| Branch | Source | Cost basis | PurchaseOrderID |
|---|---|---|---|
| Import / Exchange | `sales INNER JOIN base_link → P_costs` | `P_costs.Cif_price` / FOT formula | from base_link |
| Warehouse | `WH_sales LEFT JOIN inv` | `inv.LastCFPrice` | NULL |

- Branch 1 has **no** `WHERE PC.ValueDate IS NOT NULL` filter — rows linked to a purchase order but with no matching P_costs entry still appear with NULL cost columns rather than being silently dropped.
- `Qty_flag` = row_number per PurchaseOrderID in Branch 1; hardcoded `'0'` in Branch 2.

### 3. Date filter — delivery notes from 2025 onwards
`sales` CTE both branches: `>= 2025` on invoice date (`T_CHSHBONIT`) and delivery date (`TARIKH_MSHLOCH`). Purchase-side CTEs left broader (>= 2018 / >= 2024) so older purchases can match 2025 sales.

### 4. Removed duplicate currency subquery
Delivery note branch of `sales` previously re-computed the full `SHERI_MTBE` window function inline. Replaced with `LEFT JOIN CurrencyConvertion SM ON SM.TARIKH = HZ.T_HZMNH`.

### 5. Exchange CTEs consolidated: 5 → 3

| Removed | Replaced by |
|---|---|
| `main` + `base` | `exchange_movements` — raw free shipments enriched with `totals` cost in one pass |
| `final` + `final2` | `exchange_priced` — ROW_NUMBER + rn=1 filter in a single CTE using an inner subquery |

`Purchase_Exchange` Exchange branch updated: `final2` → `exchange_priced`.

### 8. `DocName` in P_costs — replaced MAX with `po_doctype` CTE

`MAX(DocName)` was unreliable because a single PurchaseOrderID can have rows of multiple doc types (e.g. Import rows + Invoice rows). Added `po_doctype` CTE before `P_costs` that uses `EXISTS` checks with explicit priority: **Import > Exchange > Invoice**. `P_costs` now joins to `po_doctype` instead of aggregating DocName directly.

### 6. `Purchase_DocName` values — renamed in `Purchase_Exchange` CTE

| Old | New |
|---|---|
| `'Orders'` | `'Exchange'` |
| `'Order Expenses'` | `'Import'` |
| `'Invoice'` | unchanged |
| `'Warehouse'` | unchanged (Branch 2 only) |

Inline comments in the CTE mark the original values.

---

## 7. Power BI Semantic Model (`PBI/SM/`)

`PBI/SM/ADM - DS.pbip` is a Power BI project in PBIP/TMDL format, containing:
- `ADM - DS.SemanticModel/` — the data model itself: table definitions (`definition/tables/*.tmdl`,
  ~90 tables), relationships, roles (RLS: `Agent`, `Managment`), expressions (Power Query M
  source), and culture/translation files. **`factGain.tmdl`**'s partition holds the Gain query
  directly (see [SQL queries](#3-sql-queries--no-standalone-mirror-folder)) — its CTE-by-CTE design
  rationale is in [FactGain_scope.md](FactGain_scope.md).
- `ADM - DS.Report/` — the report bound to this semantic model.

**Convention: make semantic-model changes (tables, measures, relationships, roles, etc.) through
the `powerbi-modeling-mcp` tools rather than hand-editing `.tmdl` files directly.** This keeps the
model internally consistent and avoids malformed TMDL from manual edits.

### factOrders — Avg CF/FOT Price numerator fix (2026-10-28, edited in TMDL, not yet published)
`AVG Price Switch`/`AVG Price Supply Date Switch` (PriceType 1/2/3 = Order/CF/FOT) disagreed between
price types: Order matched across both switch chains, but CF and FOT didn't. Cause: `Avg Unit Price`
(Order, PriceType 1) already uses the Basis-split-aware `Total Sales Flat + FlatValue Switch`
(`LineTotalFlatValueCFFUSD`/`FOTUSD`) for its CF/FOT legs too, but the standalone `Avg CF Price` /
`Avg FOT Price` measures (and their "By Supply Date" twins) still divided by the older
`Total Sales CF`/`Total Sales FOT` (`LineTotalCF_USD`/`LineTotalFOT_USD`), which don't apply the
PremiumFlag FlatQTY/BasisQTY split. Fixed `Avg CF Price`/`Avg FOT Price` to use
`Total Sales CF FlatValue`/`Total Sales FOT FlatValue` instead (already-existing measures reading
the Basis-aware columns), and made the "By Supply Date" versions wrap `[Avg CF Price]`/`[Avg FOT Price]`
under `CALCULATE(...,USERELATIONSHIP(...))` rather than recomputing the divide — same pattern as
`Avg Unit Price By Supply Date`. Two other unused measures (`Avg CF/FOT Price FlatValue By Supply Date`)
have the same latent bug (denominator `[Total Quantity]` instead of priced quantity) but aren't
referenced anywhere, so left alone. Not yet tested on STG/Prod or published.

## 8. Reports (`PBI/Reports/`)

Standalone Power BI reports, separate from the `PBI/SM/` semantic-model bundle:

| Report | Status |
|---|---|
| `Sales` | Existing. |
| `Purchase Expenses` | Newly added. |

More standalone reports are expected to be added here over time.

---

## 9. Testing reference
```sql
-- Test single delivery note (known multi-line case):
WHERE s.DeliveryNote = 520299   -- uncomment at bottom of final SELECT

-- Isolate warehouse rows:
WHERE PurchaseOrderID IS NULL

-- Find multi-line aggregated rows:
WHERE MultiLineFlag = 1
```
