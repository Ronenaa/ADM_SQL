# FactGain — Object-by-Object Scope

> Working notes for `test_factGain.sql` (sandbox rebuild of `FactGain.sql`). Describes exactly what
> each CTE computes, why, and the gotchas that are easy to lose when editing this file — especially
> divisor/timing rules that only make sense once you know what a *later* CTE does with the number.
> Keep this updated as `test_factGain.sql` changes; once promoted to `FactGain.sql` this becomes the
> living reference for that file.

## Business rule this file exists to compute

Gain = sale price − true cost of the goods sold, per sales line. "True cost" depends on how the
goods reached the customer:

- **Import** — cost = CIF price + demurrage/despatch + discharge (pro-rated), from the import PO.
- **Invoice** (direct purchase, no import) — cost = the invoice's own PO-line cost.
- **Swap/Exchange** — the swap order itself has no purchase price. Its cost must be inherited from
  the *specific* import PO (same ItemKey, same supplier/warehouse) that originally supplied the
  stock being swapped out. Example: import #20004101 lands at CIF $110 / FOT $115. Some of that
  stock isn't sold immediately — instead it's loaned/swapped to supplier '110'. When '110' later
  sells it at $120 FOT, the swap's gain = $120 − $115 (import 20004101's FOT), not some other price.
- **Warehouse** — sales with no purchase-order link at all (came from long-term warehouse stock);
  cost = `inv.WH_Price` (monthly FOT/weighted price per item), not tied to any specific PO.

## Cost-code vocabulary (used throughout)

| Code | Field | Meaning |
|---|---|---|
| `PNLKey = 999` | — | "commodity" bucket — base CIF cost of the goods themselves |
| `[PNL Code] = 1010` | — | default/base line (no freight-type classification) |
| `[PNL Code] = 2270` | DischargeCost(s) | port discharge cost |
| `[PNL Code] = 1201` | `[demurrage / Despatch]` | demurrage / despatch |
| `[PNL Code] = 1111` | Shortage | cargo shortage cost |
| `[PNL Code] not in (1010,2270,1201,1111)` | Other_Expenses | anything else |

`PNLKey` is the **coarse** bucket (used to isolate the commodity cost, `= 999`, from all freight-type
costs). `[PNL Code]` is the **fine-grained** code used to split freight-type costs into discharge /
demurrage / shortage / other. Both come from the same source join: `HOTSAOT_SHROTIM_New HST ON
HS.QOD_SHROT = HST.QOD_SHROT` — `HST.QOD_SHROT` (via the `PNLKey` CASE) and `HST.PNL` (renamed
`[PNL Code]`) respectively.

## Discharge/demurrage divisor — the rule that's easy to get wrong

`DischargeCosts` and `DemurrageCosts` are **raw sums** in `P_costs` — they are *not* divided by
quantity at the point they're summed. They only get divided at the point of use, and the divisor is
**not** total order quantity — it's `orderquantity - CIF_Qty` (order quantity minus the portion of
that PO's quantity that was sold CIF). Reason: CIF-sold quantity doesn't bear these costs (it's
priced to already include delivery), so it must be excluded from the per-unit allocation, or FOT
buyers would be overcharged for a cost CIF buyers already covered elsewhere.

`CIF_Qty` itself is only known once `sales`/`base_link` exist (`CIF_Qty` CTE, built from `sales`
lines flagged `qty_cif`), which is *after* `totals`/`P_costs` in the build order. **Any expression
that wants a per-unit discharge or demurrage cost must carry the raw sum forward and divide only
once `CIF_Qty` is available** — never pre-divide by plain order quantity inside `P_costs`. Both
`DischargeCosts` and `DemurrageCosts` follow this pattern; the final SELECT applies the
`(orderquantity - CIF_Qty)` divisor to each, in the `[demurrage / Despatch]`/`DischargeCost` output
columns and inline in `FOT_Purchase`, `Gain` and `TotalGain`.

`Shortage` and `Other_Expenses`, by contrast, **are** divided by plain `orderquantity` at
aggregation time in `P_costs` — no `CIF_Qty` exclusion for those two.

The same rule is mirrored in DAX on the purchase-expenses side: `factPurchaseExpenses[Cost per ton]`
applies `[Order Qty] - [CIF Qty (for PO)]` as the divisor for PNL Code 2270 (discharge) and 1201
(demurrage), and plain `[Order Qty]` for everything else.

## CTE-by-CTE

### `CurrencyConvertion`
Fills gaps in `SHERI_MTBE`'s USD/EUR rates: when a day's rate is `0`, forward-fills the last known
non-zero rate using `FIRST_VALUE` over a running "gap group" partition. Output: `new_sher`,
`new_sherEuro`. Reused everywhere money needs converting to USD. No changes planned.

### `totals_raw` → `totals`
Per-PO-line purchase expense, in USD, classified by both `PNLKey` (coarse) and `[PNL Code]` (fine).
`totals` aggregates per `(PurchaseOrderID, ItemKey)` into:
- `UnitNetPriceUSD` — blended average (kept only for existing consumers that haven't been migrated
  off it yet; **do not use this for new CIF/FOT-sensitive logic**)
- `Cif_price` — same formula `P_costs` uses (`PNLKey = 999` bucket, price per unit)
- `DischargeCostTotal` — raw sum, see divisor rule above
- `[demurrage / Despatch]`, `Shortage` — pre-divided by plain order quantity, per the rule above

**Why this matters for Swap pricing**: this is the CTE that used to collapse everything into one
blended number before it ever reached the exchange-matching chain. Preserving the split here is
what lets a matched Swap event inherit the *specific* import's true CIF/FOT breakdown instead of a
blended average — see "Swap pricing — resolved" below for how that breakdown now reaches the Swap
branch.

### `exchange_movements`
Free/loaned shipments (`TEODOT_MSHLOCH` rows with `MCHIR_ICH = 0`), enriched with the matched
purchase price from `totals`. `DeliveredFrom` = the party physically shipping stock *out* (the
lender in a swap); `DeliveredTo` = the receiver. `Order_Type = 'P'` flags rows whose resolved
PurchaseOrderID looks like an import (`LIKE '2000%'`) — these are the "candidate imports" a swap
can be matched against.
Now carries `Cif_price`, `DischargeCostTotal`, `[demurrage / Despatch]` forward from `totals`
(instead of the old blended `UnitNetPriceUSD`/`LineTotalNetUSD`) — see "Swap pricing — resolved".

### `exchange_p_orders`
Filters `exchange_movements` to `Order_Type = 'P'` (candidate imports only) and aggregates by
`(DeliveredFrom, PurchaseOrderID, ItemKey, DeliveredTo, [Date])`. `DeliveredTo` on a `'P'` row means
"the supplier/warehouse that received this import" — confirmed correct direction for the matching
join in `exchange_priced`.

### `exchange_priced`
Matches each swap-out movement (`b`) to its best candidate import (`p`) via `b.DeliveredFrom =
p.DeliveredTo AND b.ItemKey = p.ItemKey` (same supplier, same item — matches the business rule).
`ROW_NUMBER` partitions by `(DeliveredFrom, PurchaseOrderID, ItemKey, [Date])` and ranks
nearest-past import first, falling back to nearest-future import only when no past import exists.
Now carries `Cif_price`/`DischargeCostTotal`/`[demurrage / Despatch]` straight through from
`exchange_p_orders`'s `MAX(...)` aggregates — see "Swap pricing — resolved".

### `purchase_orders`
`UNION ALL` of three branches — Invoice, Import, Swap — each producing PO-line-level rows with
`PNLKey`/`[PNL Code]`/`LineTotalNetUSD`. Only the Swap branch reads `exchange_priced` (via `t`,
`rn = 1`). **Structural constraint**: this CTE cannot reference itself, so the Swap branch cannot
join `P_costs` (built *from* `purchase_orders`) or the Import branch's own rows — any fix that wants
the Swap branch to carry a true CIF/FOT split must get that data from somewhere built *before*
`purchase_orders`, i.e. from `totals`/`exchange_priced`, not from `P_costs`.

### `inv`
Monthly warehouse price per item (last day of month, FOT flat or weighted-expense fallback), for
two source paths (regular warehouses via `tblInventory`, warehouse 1144 via BT movements). Used
only by Branch 2 (Warehouse gain). No changes planned.

### `po_doctype` → `P_costs`
`po_doctype` resolves one `DocName` per `PurchaseOrderID` (priority Import > Swap > Invoice) via a
self-referencing `EXISTS` against `purchase_orders`. `P_costs` aggregates `purchase_orders` per PO
into `Cif_price`, `DischargeCosts` (raw sum), `[demurrage / Despatch]`, `Shortage`, `Other_Expenses`
— the canonical cost breakdown for Import (and, currently, Swap) orders. **This is the CTE whose
math `totals` now duplicates for Swap purposes**, because the Swap branch can't reach `P_costs`
directly (see constraint above).

### `sales`
`UNION ALL` of Invoice lines and open Delivery Note lines into one sales-line fact. Each line
carries `SalesType` (`FOT`/`CIF`/`FOT Premium`), `qty_cif`/`qty_fot`, and `DeliveryNote` (the join
key to `base_link`). `[מוצר על]` (master-product key) is now computed inline from `MOTSRIM` in both
branches (the `Items_Family` CTE was removed — it had exactly these two consumers).

### `base_link` → `CIF_Qty`
`base_link` maps `DeliveryNote → PurchaseOrderID` via the receipt/shipment linkage tables — the
join that decides whether a sale is tied to a specific PO (Import/Exchange) or not (Warehouse).
`CIF_Qty` sums `qty_cif` per PO — **this is the value the discharge-cost divisor needs** (see rule
above); it can't exist before `sales`/`base_link` do.

### `WH_sales` → `WH_prices`
Aggregates `sales` rows with no `base_link` match (warehouse-only sales) per delivery note, then
derives `Item_Price`/`Storage_Price`/`Total_Price` from the aggregate. Feeds Branch 2 only.

### Final `SELECT` — Branch 1 (Import/Exchange) vs Branch 2 (Warehouse)
Branch 1 joins `sales` (via `base_link`) to `P_costs`/`CIF_Qty` and computes `Gain`/`TotalGain` as
sale price minus `Cif_price` (CIF sales) or `Cif_price + demurrage + discharge/(orderqty−CIF_Qty)`
(FOT sales) — this is where the discharge-cost divisor rule is actually applied today (lines with
`PC.DischargeCosts / NULLIF(PC.orderquantity - ISNULL(cq.CIF_Qty, 0), 0)`). Branch 2 joins
`WH_prices` to `inv` and computes gain against the warehouse's monthly FOT price plus a flat `+16`.

## Swap pricing — resolved

Chosen approach: thread `totals`'s per-PO breakdown columns through `exchange_movements` →
`exchange_p_orders` → `exchange_priced` (no new CTE, no second `P_costs` join). Concretely:

- `exchange_movements` carries `Cif_price`, `DischargeCostTotal`, `[demurrage / Despatch]`,
  `OrderQuantity` from `totals` (replacing the old blended `UnitNetPriceUSD`/`LineTotalNetUSD`).
- `exchange_p_orders` aggregates the same three via `MAX(...)` (unchanged pattern, new columns).
- `exchange_priced` passes them through under their real names once past the aggregation step.
- `purchase_orders`'s Swap branch computes `UnitNetPriceUSD = t.Cif_price + t.[demurrage / Despatch]`
  and `LineTotalNetUSD = UnitNetPriceUSD * Quantity` — i.e. the matched import's FOT-minus-discharge
  price, not a blended average. `PNLKey = 999` / `[PNL Code] = 1010` stay as before, so the shared
  UNION ALL schema with the Invoice/Import branches (and everything `P_costs` does downstream) is
  unchanged — only the *value* going into those two columns changed.
- Discharge cost is deliberately **not** folded in at this point — same divisor rule as everywhere
  else: it needs `CIF_Qty`, which doesn't exist until `sales`/`base_link` are built, further down
  the chain. `P_costs`/the final SELECT apply it exactly as they already did for Import rows.

This means a Swap row entering `P_costs` today already carries the correct FOT-minus-discharge
price as its "commodity" line — `P_costs.Cif_price` for that PO ends up being the matched import's
`Cif_price + demurrage`, and discharge gets layered on afterward with the same `(orderquantity −
CIF_Qty)` divisor Import rows use. No changes needed further downstream for this to work.
