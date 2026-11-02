# Deadlock Incident — October 2026: Summary & Resolution

## What Happened

The Power BI refresh user **BIX** was killed 4 times in ~10 days on the live SQL server. The kills were targeting sessions that were blocking writes on the **expense tables** (`HOTSAOT_COTROT`, `HOTSAOT_SHOROT`, `HOTSAOT_SHROTIM_New`). In some cases a server restart was required.

Confirmed via Refresh history: a refresh failed at the kill time with `"A severe error occurred on the current command"`, source `127.0.0.1;MT` (the gateway running on the live server). The killed session **was** the Power BI refresh, not an SSMS window.

## Root Cause

Four tables in the model read the expense tables:  `factPurchaseExpenses`, `factGain`, `dimPurchaseOrderNumber`, `dimSubPurchaseOrderNumber`. All were changed between 2026-08-30 and 2026-09-09.

**The main problem:** `factGain` and `factPurchaseExpenses` used CTEs that SQL Server re-executed on every reference. Heavy CTEs (`purchase_orders` with a full join on `PurchaseOrderLines`) were re-executed 2–3 times each query, bloating elapsed time to **255–270s** (factGain) and **275–295s** (factPurchaseExpenses). Long-running reads under READ COMMITTED block writers on the expense tables (RCSI is disabled on live).

## Fix: CTE Materialization into Temp Tables

**No logic was changed.** Each multi-referenced CTE was computed once into a `#temp` table, replacing all subsequent references.

| Table | Before (STG) | After (STG) | Temp tables introduced |
|---|---|---|---|
| factGain | 255–270s | ~100s | `#exchange_movements`, `#purchase_orders`, `#P_costs` |
| factPurchaseExpenses | 275–295s | ~54s | `#base`, `#final2`, `#OrderPrices`, `#CIF_Qty`, `#base_expenses` |

**NOLOCK** was added on all `HOTSAOT_*` reads (already safe — these are read-only queries; only `HOTSAOT_*` tables get NOLOCK, not all tables).

QA: row counts are identical; minor float noise and a few rows from pre-existing non-deterministic bugs (`inv`/`final` ROW_NUMBER with no tiebreaker — not introduced by this fix).

## Workaround (interim)

Refreshes limited to morning and evening only, until the fix is published.

## Files

| File | Purpose |
|---|---|
| `fix/factGain__fixed.sql` | Materialized version of factGain query (readable) |
| `fix/factPurchaseExpenses__fixed.sql` | Materialized version of factPurchaseExpenses query (readable) |
| `fix/factOrders__fixed.sql` | factOrders fixed query |
| `fix/build_materialized.py` | Generates the materialized SQL from the CTE version |
| `fix/apply_to_tmdl.py` | Writes fixed SQL back into the TMDL |
| `queries/*__before.sql` / `*__current.sql` | Before/after snapshots for timing comparison |
| `01_env_check.sql` | Environment check (run on live by Shlomo) |
| `02_measure_all.sql` | Measures elapsed/CPU/reads for all 5 tables |
| `03a–03d_*.sql` | Lock simulation scripts (2-window tests) |
| `README.md` | Full investigation log (Hebrew) with timing measurements |

## Known Open Issues (not fixed, need business decision)

1. **`inv` (factGain):** `ROW_NUMBER()` for last-day-of-month price picks arbitrarily when multiple rows exist on the same date (different suppliers/warehouses). FOT_Purchase for Warehouse rows can differ between refreshes.
2. **`final` (factPurchaseExpenses):** `ROW_NUMBER()` for exchange order matching uses `Purchase_Date` at month granularity; 63 exchange orders with equal dates have price disagreement. Tiebreaker needed.

## Pending: Swap Source Fix

A separate fix for swap orders incorrectly using internal (no-ship) purchase orders as their price source is **prepared in the working tree but not committed**. See git diff on `factGain.tmdl` and `fix/factGain__fixed.sql`. Waiting for client confirmation of the hard rule (swap with no ship-backed candidates → blank price rather than wrong price).
