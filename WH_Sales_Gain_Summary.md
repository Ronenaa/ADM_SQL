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
