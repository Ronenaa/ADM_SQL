-- ============================================================
-- TEST: Swap source fix — verify ship-backed candidate filter
-- Run on STG (ADM-SQL2020\ADMNEW). Safe — read-only.
-- ============================================================
IF @@SERVERNAME <> 'ADM-SQL2020\ADMNEW'
BEGIN
    RAISERROR('Wrong server — run on STG only', 16, 1);
    RETURN;
END

SET NOCOUNT ON;
IF OBJECT_ID('tempdb..#exchange_movements') IS NOT NULL DROP TABLE #exchange_movements;
IF OBJECT_ID('tempdb..#purchase_orders')    IS NOT NULL DROP TABLE #purchase_orders;
IF OBJECT_ID('tempdb..#P_costs')            IS NOT NULL DROP TABLE #P_costs;
DECLARE @CutoffDate DATE = '2024-01-01';

-- ======================================================================
-- Step 1: materialize exchange_movements
;WITH CurrencyConvertion AS (
SELECT *,
         CASE
         	WHEN sher = 0
         	THEN FIRST_VALUE(sher) OVER (PARTITION BY value_partition ORDER BY Tarikh)
         	ELSE sher
         END AS new_sher
         ,CASE
         	WHEN SHER_EURO = 0
         	THEN FIRST_VALUE(SHER_EURO) OVER (PARTITION BY value_partitionEuro ORDER BY Tarikh)
         	ELSE  SHER_EURO
         END AS new_sherEuro
 FROM (
          SELECT *
 ,SUM(CASE WHEN sher=0 THEN 0 ELSE 1 END) OVER (ORDER BY tarikh ) AS value_partition
 ,SUM(CASE WHEN SHER_EURO=0 THEN 0 ELSE 1 END) OVER (ORDER BY tarikh ) AS value_partitionEuro
          FROM SHERI_MTBE
 ) m
)
,totals_raw AS (
    SELECT
        CONCAT(CAST(CONVERT(BIGINT, POL.POL_OrderID) AS VARCHAR(20)), CAST(POL.POL_LineID AS VARCHAR(10))) AS PurchaseOrderID,
        CONVERT(VARCHAR, HS.QOD_MOTSR) + '-' + CONVERT(VARCHAR, HS.QOD_MOTSR) AS ItemKey,
        CASE WHEN HST.QOD_SHROT IN (5,19,30,4) OR HST.QOD_SHROT IS NULL THEN 999 ELSE HST.QOD_SHROT END AS PNLKey,
        CASE WHEN HS.QOD_SHROT = 0 THEN HS.CMOT ELSE 0 END AS OrderQuantity,
        CASE
            WHEN HS.MTBE = '$'   THEN HS.MCHIR_ICH * HS.CMOT
            WHEN HS.MTBE = 'Eur' THEN HS.MCHIR_ICH * HS.CMOT * (SM.NEW_SHEREURO / NULLIF(SM.NEW_SHER, 0))
            ELSE HS.MCHIR_ICH * HS.CMOT * (1 / CASE WHEN HC.SHER_MTBE = 0 THEN 1 ELSE HC.SHER_MTBE END)
        END AS LineTotalNetUSD
    FROM HOTSAOT_COTROT HC WITH (NOLOCK)
    LEFT JOIN HOTSAOT_SHOROT HS WITH (NOLOCK) ON HC.NOMRTOR = HS.NOMRTOR
    LEFT JOIN PurchaseOrderLines POL
        ON CONCAT(CAST(CONVERT(BIGINT, POL.POL_OrderID) AS VARCHAR(20)), CAST(POL.POL_LineID AS VARCHAR(10)))
           = CAST(HS.MS_MSMKH_QSHOR AS VARCHAR(30))
    LEFT JOIN HOTSAOT_SHROTIM_New HST WITH (NOLOCK) ON HS.QOD_SHROT = HST.QOD_SHROT
    LEFT JOIN CurrencyConvertion SM ON SM.TARIKH = HC.T_ERKH
    WHERE CAST(SUBSTRING(HC.T_MSMKH,1,4) AS INT) >= 2018
      AND CAST(SUBSTRING(HC.T_MSMKH,1,4) AS INT) <= YEAR(GETDATE())
      AND HS.QOD_SHROT NOT IN (14)
)
,totals AS (
    SELECT PurchaseOrderID, ItemKey, 999 AS PNLKey,
        SUM(LineTotalNetUSD) AS LineTotalNetUSD,
        SUM(OrderQuantity)   AS OrderQuantity,
        CAST(ROUND(SUM(LineTotalNetUSD) / NULLIF(SUM(OrderQuantity), 0), 2) AS FLOAT) AS UnitNetPriceUSD
    FROM totals_raw
    GROUP BY PurchaseOrderID, ItemKey
)
,exchange_movements AS (
    SELECT
        TM.QOD_SHOLCH AS DeliveredFrom, W.SHM_GORM AS DeliveredFromName,
        ISNULL(b.MS_HZMNH, bb.MS_HZMNH) AS PurchaseOrderID,
        CASE WHEN ISNULL(b.MS_HZMNH, bb.MS_HZMNH) LIKE '2000%' THEN 'P' ELSE 'E' END AS Order_Type,
        CONVERT(VARCHAR, TM.QOD_MOTSR) + '-' + CONVERT(VARCHAR, TM.QOD_MOTSR) AS ItemKey,
        SUM(TM.MSHQL_NTO) AS qty_loaned,
        G.QOD_GORM AS DeliveredTo, G.SHM_GORM AS DeliveredToName,
        SUBSTRING(TARIKH_MSHLOCH,1,4) + '-' + SUBSTRING(TARIKH_MSHLOCH,5,2) AS [Date],
        t.UnitNetPriceUSD, t.LineTotalNetUSD, t.OrderQuantity
    FROM TEODOT_MSHLOCH TM
    LEFT JOIN GORMIM G                   ON TM.QOD_MQBL       = G.QOD_GORM
    LEFT JOIN GORMIM W                   ON TM.QOD_SHOLCH      = W.QOD_GORM
    LEFT JOIN QISHOR_RCSH_LMCIRH a       ON a.MS_TEODT_MCIRH  = TM.MS_TEODH
    LEFT JOIN QISHOR_T_MSHLOCH_HZMNOT b  ON a.MS_TEODT_RCSH   = b.MS_T_MSHLOCH
    LEFT JOIN QISHOR_T_MSHLOCH_HZMNOT bb ON bb.MS_T_MSHLOCH   = TM.MS_TEODH
    LEFT JOIN (SELECT PurchaseOrderID, ItemKey, UnitNetPriceUSD, LineTotalNetUSD, OrderQuantity
               FROM totals WHERE PNLKey = 999 AND OrderQuantity <> 0) t
        ON ISNULL(b.MS_HZMNH, bb.MS_HZMNH) = t.PurchaseOrderID
    WHERE G.AOPI_PEILOT NOT IN (N'אחסון')
      AND (ISNULL(b.MS_HZMNH, bb.MS_HZMNH) LIKE '2000%'
           OR TRY_CONVERT(DATE, SUBSTRING(TARIKH_MSHLOCH,1,4)+'-'+SUBSTRING(TARIKH_MSHLOCH,5,2)+'-'+SUBSTRING(TARIKH_MSHLOCH,7,2)) >= @CutoffDate)
    GROUP BY
        SUBSTRING(TARIKH_MSHLOCH,1,4)+'-'+SUBSTRING(TARIKH_MSHLOCH,5,2),
        TM.QOD_SHOLCH, W.SHM_GORM, b.MS_HZMNH, bb.MS_HZMNH,
        CONVERT(VARCHAR, TM.QOD_MOTSR)+'-'+CONVERT(VARCHAR, TM.QOD_MOTSR),
        G.QOD_GORM, G.SHM_GORM,
        t.UnitNetPriceUSD, t.LineTotalNetUSD, t.OrderQuantity
)
SELECT * INTO #exchange_movements FROM exchange_movements;

-- ======================================================================
-- Step 2: materialize purchase_orders (Swap branch only — much faster for testing)
;WITH CurrencyConvertion AS (
SELECT *,
    CASE WHEN sher=0 THEN FIRST_VALUE(sher) OVER (PARTITION BY value_partition ORDER BY Tarikh) ELSE sher END AS new_sher,
    CASE WHEN SHER_EURO=0 THEN FIRST_VALUE(SHER_EURO) OVER (PARTITION BY value_partitionEuro ORDER BY Tarikh) ELSE SHER_EURO END AS new_sherEuro
FROM (
    SELECT *, SUM(CASE WHEN sher=0 THEN 0 ELSE 1 END) OVER (ORDER BY tarikh) AS value_partition,
              SUM(CASE WHEN SHER_EURO=0 THEN 0 ELSE 1 END) OVER (ORDER BY tarikh) AS value_partitionEuro
    FROM SHERI_MTBE) m
)
,exchange_p_orders AS (
    SELECT
        DeliveredFrom, DeliveredFromName, PurchaseOrderID, ItemKey,
        SUM(qty_loaned) AS quantity,
        DeliveredTo, DeliveredToName, [Date] AS Purchase_Date,
        MAX(UnitNetPriceUSD) AS max_unit_price,
        MAX(LineTotalNetUSD) AS max_LineTotalNetUSD,
        MAX(OrderQuantity)   AS max_OrderQuantity
    FROM #exchange_movements
    WHERE Order_Type = 'P'
      -- THE FIX: only ship-backed purchase orders are candidates
      AND CAST(PurchaseOrderID AS VARCHAR(30)) IN (
            SELECT CONCAT(CAST(CONVERT(BIGINT, POL.POL_OrderID) AS VARCHAR(20)), CAST(POL.POL_LineID AS VARCHAR(10)))
            FROM PurchaseOrderLines POL
            JOIN ShipsArrivals sa ON POL.POL_ShipArrivalID = sa.SA_ID
            JOIN ShipList      sl ON sa.SA_ShipID          = sl.ShipID)
    GROUP BY DeliveredFrom, DeliveredFromName, PurchaseOrderID, ItemKey, DeliveredTo, DeliveredToName, [Date]
)
,exchange_priced AS (
    SELECT
        b.DeliveredFrom, b.DeliveredFromName,
        b.PurchaseOrderID AS Exchange_order, b.[Date] AS Exchange_Order_Date,
        b.ItemKey, b.qty_loaned AS Qty_Sold,
        p.PurchaseOrderID AS Purchase, p.Purchase_Date,
        p.max_unit_price AS UnitNetPriceUSD, p.max_LineTotalNetUSD AS LineTotalNetUSD,
        p.max_OrderQuantity AS OrderQuantity,
        CASE WHEN p.max_LineTotalNetUSD IS NOT NULL THEN ship_sub.ShipID ELSE NULL END AS ShipID,
        ROW_NUMBER() OVER (
            PARTITION BY b.DeliveredFrom, b.PurchaseOrderID, b.ItemKey, b.[Date]
            ORDER BY
                CASE WHEN CAST(p.Purchase_Date+'-01' AS DATE) <= CAST(b.[Date]+'-01' AS DATE) THEN 0 ELSE 1 END,
                ABS(DATEDIFF(MONTH, CAST(b.[Date]+'-01' AS DATE), CAST(p.Purchase_Date+'-01' AS DATE))) ASC,
                p.PurchaseOrderID DESC
        ) AS rn
    FROM #exchange_movements b
    LEFT JOIN exchange_p_orders p
        ON b.DeliveredFrom = p.DeliveredTo AND b.ItemKey = p.ItemKey
        AND (DATEDIFF(MONTH, CAST(p.Purchase_Date+'-01' AS DATE), CAST(b.[Date]+'-01' AS DATE)) BETWEEN 0 AND 3
             OR CAST(p.Purchase_Date+'-01' AS DATE) > CAST(b.[Date]+'-01' AS DATE))
    LEFT JOIN (
        SELECT CONCAT(CAST(CONVERT(BIGINT, POL.POL_OrderID) AS VARCHAR(20)), CAST(POL.POL_LineID AS VARCHAR(10))) AS PurchaseOrderID,
               MAX(sl.ShipID) AS ShipID
        FROM PurchaseOrderLines POL
        JOIN ShipsArrivals sa ON POL.POL_ShipArrivalID = sa.SA_ID
        JOIN ShipList      sl ON sa.SA_ShipID          = sl.ShipID
        GROUP BY POL.POL_OrderID, POL.POL_LineID
    ) ship_sub ON ship_sub.PurchaseOrderID = CAST(p.PurchaseOrderID AS VARCHAR(30))
)
,Swap_Moves AS (
    SELECT ISNULL(b.MS_HZMNH, bb.MS_HZMNH) AS OrderID, TM.QOD_SHOLCH AS DeliveredFrom, TM.QOD_MQBL AS DeliveredTo
    FROM TEODOT_MSHLOCH TM
    LEFT JOIN GORMIM G                   ON TM.QOD_MQBL      = G.QOD_GORM
    LEFT JOIN QISHOR_RCSH_LMCIRH a       ON a.MS_TEODT_MCIRH = TM.MS_TEODH
    LEFT JOIN QISHOR_T_MSHLOCH_HZMNOT b  ON a.MS_TEODT_RCSH  = b.MS_T_MSHLOCH
    LEFT JOIN QISHOR_T_MSHLOCH_HZMNOT bb ON bb.MS_T_MSHLOCH  = TM.MS_TEODH
    WHERE G.AOPI_PEILOT NOT IN (N'אחסון') AND ISNULL(b.MS_HZMNH, bb.MS_HZMNH) IS NOT NULL
)
,Swap_RootSupplier AS (
    SELECT OrderID, MIN(DeliveredFrom) AS RootSupplierKey
    FROM Swap_Moves m
    WHERE NOT EXISTS (SELECT 1 FROM Swap_Moves m2 WHERE m2.OrderID = m.OrderID AND m2.DeliveredTo = m.DeliveredFrom)
    GROUP BY OrderID
)
,purchase_orders AS (
    SELECT
        CAST(HZ.MSPR_HZMNH AS VARCHAR) AS PurchaseOrderID, 'Swap' AS DocName,
        ISNULL(rs.RootSupplierKey, t.DeliveredFrom) AS SupplierKey,
        CAST(SUBSTRING(HZ.T_ASPQH,1,4)+'-'+SUBSTRING(HZ.T_ASPQH,5,2)+'-'+SUBSTRING(HZ.T_ASPQH,7,2) AS DATE) AS [Value Date],
        CAST(CONVERT(INT,CONVERT(VARCHAR,HZ.MOTSR_MOZMN)) AS VARCHAR)+'-'+CAST(CONVERT(INT,CONVERT(VARCHAR,HZ.MOTSR_MOZMN)) AS VARCHAR) AS ItemKey,
        t.UnitNetPriceUSD, HZ.CMOT_MOZMNT AS Quantity, t.OrderQuantity, 999 AS PNLKey, 1010 AS [PNL Code],
        t.LineTotalNetUSD,
        CONVERT(INT,CONVERT(VARCHAR,SUBSTRING(HZ.T_ASPQH,1,4)+SUBSTRING(HZ.T_ASPQH,5,2))) AS YearMonth,
        t.ShipID, 'Order' AS DocType, SM.NEW_SHER,
        t.Purchase AS MatchedImportPO
    FROM HZMNOT HZ
    LEFT JOIN CurrencyConvertion SM ON SM.TARIKH = HZ.T_HZMNH
    LEFT JOIN exchange_priced t
        ON t.Exchange_order = HZ.MSPR_HZMNH
       AND t.ItemKey = CAST(CONVERT(INT,CONVERT(VARCHAR,HZ.MOTSR_MOZMN)) AS VARCHAR)+'-'+CAST(CONVERT(INT,CONVERT(VARCHAR,HZ.MOTSR_MOZMN)) AS VARCHAR)
       AND t.rn = 1
    LEFT JOIN Swap_RootSupplier rs ON rs.OrderID = HZ.MSPR_HZMNH
    WHERE CAST(SUBSTRING(HZ.T_HZMNH,1,4) AS INT) BETWEEN 2018 AND YEAR(GETDATE())
      AND HZ.OrderStatus <> 3
      AND HZ.ActionType IN (6,7)
      AND CAST(SUBSTRING(HZ.T_ASPQH,1,4)+'-'+SUBSTRING(HZ.T_ASPQH,5,2)+'-'+SUBSTRING(HZ.T_ASPQH,7,2) AS DATE) >= @CutoffDate
)
SELECT * INTO #purchase_orders FROM purchase_orders;

-- ======================================================================
-- Step 3: P_costs (swap rows only)
;WITH po_doctype AS (SELECT DISTINCT PurchaseOrderID, 'Swap' AS DocName FROM #purchase_orders)
,Swap_WinningMatch AS (
    SELECT PurchaseOrderID, MatchedImportPO, ShipID
    FROM (
        SELECT pe.PurchaseOrderID, pe.MatchedImportPO, pe.ShipID,
               ROW_NUMBER() OVER (PARTITION BY pe.PurchaseOrderID ORDER BY pe.MatchedImportPO DESC) AS rn
        FROM #purchase_orders pe
        WHERE pe.MatchedImportPO IS NOT NULL
    ) x WHERE rn = 1
)
,P_costs AS (
    SELECT
        pe.PurchaseOrderID, dt.DocName,
        MIN([Value Date]) AS ValueDate,
        MAX(CASE WHEN [PNLKey]=999 THEN SupplierKey ELSE NULL END) AS SupplierKey,
        MAX(sw.ShipID) AS boat,
        CASE WHEN dt.DocName='Swap' THEN MAX(pe.OrderQuantity) ELSE SUM(pe.OrderQuantity) END AS orderquantity,
        SUM(pe.LineTotalNetUSD) AS LineTotalNetUSD,
        CAST(ROUND(SUM(CASE WHEN PNLKey=999 THEN LineTotalNetUSD ELSE 0 END)
                   / NULLIF(SUM(CASE WHEN PNLKey=999 THEN OrderQuantity ELSE 0 END),0),2) AS FLOAT) AS Cif_price,
        MAX(pe.MatchedImportPO) AS MatchedImportPO
    FROM #purchase_orders pe
    LEFT JOIN po_doctype dt ON dt.PurchaseOrderID = pe.PurchaseOrderID
    LEFT JOIN Swap_WinningMatch sw ON sw.PurchaseOrderID = pe.PurchaseOrderID
    GROUP BY pe.PurchaseOrderID, dt.DocName
)
SELECT * INTO #P_costs FROM P_costs;

-- ======================================================================
-- RESULTS

-- 1. All swaps: matched import PO, ship, and CIF price
PRINT '=== Swaps with their matched import PO (AFTER fix) ===';
SELECT
    pc.PurchaseOrderID          AS SwapOrderID,
    pc.MatchedImportPO          AS MatchedImportPO,
    pc.boat                     AS ShipID,
    sl.ShipDesc                 AS ShipName,
    pc.Cif_price                AS Cif_price,
    pc.orderquantity            AS Quantity,
    pc.ValueDate
FROM #P_costs pc
LEFT JOIN ShipList sl ON CAST(sl.ShipID AS VARCHAR) = CAST(pc.boat AS VARCHAR)
ORDER BY pc.ValueDate DESC;

-- 2. Summary: how many swaps have a price vs. blank
PRINT '=== Price coverage summary ===';
SELECT
    CASE WHEN Cif_price IS NULL OR Cif_price = 0 THEN 'No price (blank)' ELSE 'Has price' END AS PriceStatus,
    COUNT(*) AS SwapCount
FROM #P_costs
GROUP BY CASE WHEN Cif_price IS NULL OR Cif_price = 0 THEN 'No price (blank)' ELSE 'Has price' END;

-- 3. Specifically check if 20007741 still appears as a MatchedImportPO anywhere
PRINT '=== Check: does 20007741 (no-ship order) still appear as matched import? ===';
SELECT SwapOrderID = PurchaseOrderID, MatchedImportPO, Cif_price
FROM #P_costs
WHERE MatchedImportPO LIKE '20007741%';

-- 4. Check if 20007011 (MARO MARO, ship 209) is now chosen instead
PRINT '=== Check: does 20007011 (MARO MARO) now appear as matched import? ===';
SELECT SwapOrderID = PurchaseOrderID, MatchedImportPO, boat, Cif_price
FROM #P_costs
WHERE MatchedImportPO LIKE '20007011%';

-- 5. Before/after comparison: re-run exchange_p_orders WITHOUT the ship filter
--    to see how many swaps had a price before the fix vs. after (result 2 above).
PRINT '=== Before fix: price coverage WITHOUT ship filter ===';
;WITH exchange_p_orders_old AS (
    SELECT DeliveredFrom, DeliveredFromName, PurchaseOrderID, ItemKey,
        SUM(qty_loaned) AS quantity, DeliveredTo, DeliveredToName, [Date] AS Purchase_Date,
        MAX(UnitNetPriceUSD) AS max_unit_price, MAX(LineTotalNetUSD) AS max_LineTotalNetUSD, MAX(OrderQuantity) AS max_OrderQuantity
    FROM #exchange_movements
    WHERE Order_Type = 'P'  -- NO ship filter — old behavior
    GROUP BY DeliveredFrom, DeliveredFromName, PurchaseOrderID, ItemKey, DeliveredTo, DeliveredToName, [Date]
)
,exchange_priced_old AS (
    SELECT b.PurchaseOrderID AS Exchange_order, p.PurchaseOrderID AS Purchase,
        ROW_NUMBER() OVER (
            PARTITION BY b.DeliveredFrom, b.PurchaseOrderID, b.ItemKey, b.[Date]
            ORDER BY
                CASE WHEN CAST(p.Purchase_Date+'-01' AS DATE) <= CAST(b.[Date]+'-01' AS DATE) THEN 0 ELSE 1 END,
                ABS(DATEDIFF(MONTH, CAST(b.[Date]+'-01' AS DATE), CAST(p.Purchase_Date+'-01' AS DATE))) ASC,
                p.PurchaseOrderID DESC
        ) AS rn
    FROM #exchange_movements b
    LEFT JOIN exchange_p_orders_old p
        ON b.DeliveredFrom = p.DeliveredTo AND b.ItemKey = p.ItemKey
        AND (DATEDIFF(MONTH, CAST(p.Purchase_Date+'-01' AS DATE), CAST(b.[Date]+'-01' AS DATE)) BETWEEN 0 AND 3
             OR CAST(p.Purchase_Date+'-01' AS DATE) > CAST(b.[Date]+'-01' AS DATE))
)
SELECT
    CASE WHEN ep.Purchase IS NULL THEN 'No price (blank)' ELSE 'Has price' END AS PriceStatus,
    COUNT(DISTINCT hz.MSPR_HZMNH) AS SwapCount
FROM HZMNOT hz
LEFT JOIN exchange_priced_old ep
    ON ep.Exchange_order = hz.MSPR_HZMNH AND ep.rn = 1
WHERE CAST(SUBSTRING(hz.T_HZMNH,1,4) AS INT) BETWEEN 2018 AND YEAR(GETDATE())
  AND hz.OrderStatus <> 3
  AND hz.ActionType IN (6,7)
  AND CAST(SUBSTRING(hz.T_ASPQH,1,4)+'-'+SUBSTRING(hz.T_ASPQH,5,2)+'-'+SUBSTRING(hz.T_ASPQH,7,2) AS DATE) >= '2024-01-01'
GROUP BY CASE WHEN ep.Purchase IS NULL THEN 'No price (blank)' ELSE 'Has price' END;
