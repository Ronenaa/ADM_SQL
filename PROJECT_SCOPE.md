# ADM — Project Scope

> **Keep this file updated.** Edit it whenever files are added/removed, or FactGain logic changes.

---

## 1. Project Purpose

ADM is a data-warehouse / BI project. This repository holds the SQL that builds the fact and
dimension queries feeding a Power BI **semantic model** (`SM/`) and a set of standalone Power BI
**reports** (`Reports/`). The SQL layer is the source of truth for the data; the semantic model
exposes it for analysis, and the reports consume the semantic model (or, in some cases, ship as
self-contained PBIP reports).

## 2. Repository Structure

```
ADM_SQL/
├── PROJECT_SCOPE.md          this file
├── sql/
│   ├── facts/                 fact-table queries (sales, gain, purchasing, inventory, ...)
│   └── dimension/              dimension/lookup queries
├── SM/
│   └── ADM - DS.*/            Power BI semantic model project (PBIP + TMDL) and its bound report
└── Reports/
    ├── Sales.*/                standalone Power BI report
    └── Purchase Expenses.*/    standalone Power BI report (new)
```

---

## 3. `sql/facts/`

| File | Purpose |
|---|---|
| `FactGain.sql` | **Active / production.** Profit/gain per sales line. See CTE chain & change log below. |
| `FactGain_BU.sql` | **Backup.** Holds the last *committed* version of `FactGain.sql` from before the most recent push — see [Backup convention](#backup-convention) below. |
| `factSales.sql` | All sales — invoices (CHSHBONIOT) + open delivery notes (TEODOT_MSHLOCH). Includes WarehouseFlag, ActionType, QuantityCategory. Date range: 2018 → current year. |
| `factInventory.sql` | Inventory snapshot — quantity, avg price, FOT and CF prices per item/supplier/month. |
| `factInventoryActivity.sql` | Inventory movement activity. |
| `factInventoryAllocation.sql` | Inventory allocation records. |
| `factOrders.sql` | Purchase orders. |
| `factOrdersStepTest.sql` | Scratch/step-through test query for purchase-order logic. |
| `factPurchaseOrder.sql` | Purchase order header/line data. |
| `factPurchaseExpenses.sql` | Expenses linked to purchase orders. |
| `factPurchaseIncome.sql` | Purchase income records. |
| `factCollection.sql` | Collections / receivables. |
| `factDailyPrices.sql` | Daily price data. |
| `FactLimits.sql` | Credit / quantity limits. |
| `factTargets.sql` | Sales targets. |

## 4. `sql/dimension/`

| File | Purpose |
|---|---|
| `dimActionType.sql` | Action type lookup (FOT, CIF, Exchange, etc.). |
| `dimItems.sql` | Items/products dimension. |
| `dimItemsSimple.sql` | Simplified items/products dimension. |
| `dimPurchaseOrderNumber.sql` | Purchase order number dimension. |
| `dimSubPurchaseOrderID.sql` | Sub purchase order ID dimension. |
| `dimWarehouses.sql` | Warehouses dimension. |

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

### Backup convention

Before pushing a change to `FactGain.sql`, first overwrite `FactGain_BU.sql` with the **currently
committed** version of `FactGain.sql` (`git show HEAD:sql/facts/FactGain.sql`), so that `_BU`
always holds the version that was live immediately before the incoming change — a one-step-back
safety copy, not a fixed historical snapshot.

## 6. FactGain — Change Log

### 10. Removed temporary `test` wrapper CTE (latest)
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

## 7. Power BI Semantic Model (`SM/`)

`SM/ADM - DS.pbip` is a Power BI project in PBIP/TMDL format, containing:
- `ADM - DS.SemanticModel/` — the data model itself: table definitions (`definition/tables/*.tmdl`,
  ~90 tables), relationships, roles (RLS: `Agent`, `Managment`), expressions (Power Query M
  source), and culture/translation files. **`factGain.tmdl`** is the Power BI-side counterpart
  table to `sql/facts/FactGain.sql` — when the SQL changes, the model table typically needs a
  matching update.
- `ADM - DS.Report/` — the report bound to this semantic model.

**Convention: make semantic-model changes (tables, measures, relationships, roles, etc.) through
the `powerbi-modeling-mcp` tools rather than hand-editing `.tmdl` files directly.** This keeps the
model internally consistent and avoids malformed TMDL from manual edits.

## 8. Reports (`Reports/`)

Standalone Power BI reports, separate from the `SM/` semantic-model bundle:

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
