# ADM_SQL — Folder Reference

> **Keep this file updated.** Edit it whenever changes are made to any file in this folder.

---

## Folder Contents

### Active / Production queries
| File | Purpose |
|---|---|
| `factSales.sql` | All sales — invoices (CHSHBONIOT) + open delivery notes (TEODOT_MSHLOCH). Includes WarehouseFlag, ActionType, QuantityCategory. Date range: 2018 → current year. |
| `factGain_v1.sql` | **Main working file.** Profit/gain per sales line. See full change log below. |
| `FactGain.sql` | Earlier version of the gain query (reference / source). Not the active working file. |
| `factInventory.sql` | Inventory snapshot — quantity, avg price, FOT and CF prices per item/supplier/month. |
| `factOrders.sql` | Purchase orders. |
| `factPurchaseOrder.sql` | Purchase order header/line data. |
| `factPurchaseExpenses.sql` | Expenses linked to purchase orders. |
| `factPurchaseIncome.sql` | Purchase income records. |
| `factCollection.sql` | Collections / receivables. |
| `factDailyPrices.sql` | Daily price data. |
| `factInventoryActivity.sql` | Inventory movement activity. |
| `factInventoryAllocation.sql` | Inventory allocation records. |
| `FactLimits.sql` | Credit / quantity limits. |
| `factTargets.sql` | Sales targets. |
| `Warehouse.sql` | Warehouse entity data. |

### Dimension tables
| File | Purpose |
|---|---|
| `dimActionType.sql` | Action type lookup (FOT, CIF, Exchange, etc.). |
| `dimItemsSimple.sql` | Simplified items/products dimension. |
| `dimPurchaseOrderNumber.sql` | Purchase order number dimension. |
| `dimSubPurchaseOrderID.sql` | Sub purchase order ID dimension. |

### Exchange fact (versioned)
`New_fact_exchange.sql` through `New_fact_exchange_v5.sql` — iterative versions of the exchange fact table query.

---

## factGain_v1.sql — CTE Chain

```
CurrencyConvertion
  └─ totals_raw → totals
       └─ exchange_movements → purchase_orders → exchange_priced
            └─ Purchase_Exchange (Invoice | Import | Exchange branches)
                 └─ inv
                      └─ P_costs
                           └─ sales (Invoices | Delivery Notes)
                                └─ base_link
                                     └─ WH_sales
                                          └─ gain (Branch 1: Import/Exchange | Branch 2: Warehouse)
                                               └─ SELECT * FROM gain
```

---

## factGain_v1.sql — Full Change Log

### 7. Warehouse branch — field improvements (latest)

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

---

### 1. Warehouse Sales branch added
- **`WH_sales` CTE** (after `base_link`): identifies warehouse delivery notes by LEFT OUTER JOIN to `base_link` — rows where `bl.DeliveryNote IS NULL` have no purchase order link and therefore come from a warehouse.
- Aggregates multiple lines per delivery note into one row: `ItemKey`, `UnitNetPriceUSD`, `SalesType` from `'Item'` line only; `Quantity` = item qty only; `LineTotalNet_USD` = sum of all lines (item + storage fees).
- **`MultiLineFlag`**: `1` if delivery note had >1 original sales lines (e.g. item + storage fee), `0` if single line.
- Cost basis: `inv.LastCFPrice` (C&F flat price from inventory).

### 2. `gain` CTE — UNION ALL final result
Two branches, identical 31-column schema:

| Branch | Source | Cost basis | PurchaseOrderID |
|---|---|---|---|
| Import / Exchange | `sales INNER JOIN base_link → P_costs` | `P_costs.Cif_price` / FOT formula | from base_link |
| Warehouse | `WH_sales LEFT JOIN inv` | `inv.LastCFPrice` | NULL |

- Branch 1 has **no** `WHERE PC.ValueDate IS NOT NULL` filter — rows linked to a purchase order but with no matching P_costs entry still appear with NULL cost columns rather than being silently dropped.
- `Qty_flag` = row_number per PurchaseOrderID in Branch 1; hardcoded `'0'` in Branch 2.

### 3. Date filter — delivery notes from 2025 onwards
`sales` CTE both branches: `>= 2025` on invoice date (`T_CHSHBONIT`) and delivery date (`TARIKH_MSHLOCH`). Purchase-side CTEs left broader (>= 2018 / >= 2024) so older purchases can match 2025 sales.

### 4. Improvement — removed duplicate currency subquery
Delivery note branch of `sales` previously re-computed the full `SHERI_MTBE` window function inline. Replaced with `LEFT JOIN CurrencyConvertion SM ON SM.TARIKH = HZ.T_HZMNH`.

### 5. Exchange CTEs consolidated: 5 → 3

| Removed | Replaced by |
|---|---|
| `main` + `base` | `exchange_movements` — raw free shipments enriched with `totals` cost in one pass |
| `final` + `final2` | `exchange_priced` — ROW_NUMBER + rn=1 filter in a single CTE using an inner subquery |

`Purchase_Exchange` Exchange branch updated: `final2` → `exchange_priced`.

### 8. `DocName` in P_costs — replaced MAX with `po_doctype` CTE

`MAX(DocName)` was unreliable because a single PurchaseOrderID can have rows of multiple doc types (e.g. Import rows + Invoice rows). Added `po_doctype` CTE before `P_costs` that uses `EXISTS` checks with explicit priority: **Import > Exchange > Invoice**. `P_costs` now joins to `po_doctype` instead of aggregating DocName directly.

---

### 6. `Purchase_DocName` values — renamed in `Purchase_Exchange` CTE

| Old | New |
|---|---|
| `'Orders'` | `'Exchange'` |
| `'Order Expenses'` | `'Import'` |
| `'Invoice'` | unchanged |
| `'Warehouse'` | unchanged (Branch 2 only) |

Inline comments in the CTE mark the original values.

---

## Testing reference
```sql
-- Test single delivery note (known multi-line case):
WHERE s.DeliveryNote = 520299   -- uncomment at bottom of gain CTE

-- Isolate warehouse rows:
WHERE PurchaseOrderID IS NULL

-- Find multi-line aggregated rows:
WHERE MultiLineFlag = 1
```
