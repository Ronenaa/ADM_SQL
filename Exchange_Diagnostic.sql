/*
    Exchange_Diagnostic.sql
    ────────────────────────────────────────────────────────────────────────
    Shows the full logic of how exchange orders are resolved end-to-end.

    Use this to debug:
      • Why ShipID is NULL for an exchange in the gain query
            → check Ship_From_Purchase: if NULL, the ship_sub join
              didn't match (POL_OrderID may differ from the HZMNOT order ID)
      • Why price is NULL (UnitNetPriceUSD is NULL)
            → check Match_Status: 'NO MATCH' means no purchase order
              was found within the ±3-month window
      • Why quantities don't add up
            → compare Shipped_Qty vs Ordered_Qty
      • Why supplier (DeliveredFrom) is NULL
            → the QISHOR_T_MSHLOCH_HZMNOT link is missing for that movement
      • Why rn > 1 rows appear
            → multiple purchase candidates matched; only rn = 1 is used
              in FactGain_v1 (the most recent purchase ≤ 3 months before)
*/

with CurrencyConvertion as (
    SELECT *,
        CASE
            WHEN sher = 0
            THEN FIRST_VALUE(sher) OVER (PARTITION BY value_partition ORDER BY Tarikh)
            ELSE sher
        END AS new_sher,
        CASE
            WHEN SHER_EURO = 0
            THEN FIRST_VALUE(SHER_EURO) OVER (PARTITION BY value_partitionEuro ORDER BY Tarikh)
            ELSE SHER_EURO
        END AS new_sherEuro
    FROM (
        SELECT *,
            SUM(CASE WHEN sher      = 0 THEN 0 ELSE 1 END) OVER (ORDER BY tarikh) AS value_partition,
            SUM(CASE WHEN SHER_EURO = 0 THEN 0 ELSE 1 END) OVER (ORDER BY tarikh) AS value_partitionEuro
        FROM SHERI_MTBE
    ) m
)

,totals_raw AS (
    SELECT
        CONCAT(
            CAST(CONVERT(BIGINT, POL.POL_OrderID) AS VARCHAR(20)),
            CAST(POL.POL_LineID AS VARCHAR(10))
        ) AS PurchaseOrderID,
        CONVERT(VARCHAR, HS.QOD_MOTSR) + '-' + CONVERT(VARCHAR, HS.QOD_MOTSR) AS ItemKey,
        CASE
            WHEN HST.QOD_SHROT IN (5, 19, 30, 4) OR HST.QOD_SHROT IS NULL THEN 999
            ELSE HST.QOD_SHROT
        END AS PNLKey,
        CASE WHEN HS.QOD_SHROT = 0 THEN HS.CMOT ELSE 0 END AS OrderQuantity,
        CASE
            WHEN HS.MTBE = '$'   THEN HS.MCHIR_ICH * HS.CMOT
            WHEN HS.MTBE = 'Eur' THEN HS.MCHIR_ICH * HS.CMOT * (SM.NEW_SHEREURO / NULLIF(SM.NEW_SHER, 0))
            ELSE                      HS.MCHIR_ICH * HS.CMOT * (1 / CASE WHEN HC.SHER_MTBE = 0 THEN 1 ELSE HC.SHER_MTBE END)
        END AS LineTotalNetUSD
    FROM HOTSAOT_COTROT HC
    LEFT JOIN HOTSAOT_SHOROT HS       ON HC.NOMRTOR = HS.NOMRTOR
    LEFT JOIN PurchaseOrderLines POL  ON CONCAT(
                                             CAST(CONVERT(BIGINT, POL.POL_OrderID) AS VARCHAR(20)),
                                             CAST(POL.POL_LineID AS VARCHAR(10))
                                         ) = CAST(HS.MS_MSMKH_QSHOR AS VARCHAR(30))
    LEFT JOIN HOTSAOT_SHROTIM_New HST ON HS.QOD_SHROT = HST.QOD_SHROT
    LEFT JOIN CurrencyConvertion SM   ON SM.TARIKH = HC.T_ERKH
    WHERE CAST(SUBSTRING(HC.T_MSMKH, 1, 4) AS INT) BETWEEN 2018 AND YEAR(GETDATE())
      AND HS.QOD_SHROT NOT IN (14)
)

,totals AS (
    SELECT
        PurchaseOrderID,
        ItemKey,
        999 AS PNLKey,
        SUM(LineTotalNetUSD)  AS LineTotalNetUSD,
        SUM(OrderQuantity)    AS OrderQuantity,
        CAST(ROUND(SUM(LineTotalNetUSD) / NULLIF(SUM(OrderQuantity), 0), 2) AS FLOAT) AS UnitNetPriceUSD
    FROM totals_raw
    GROUP BY PurchaseOrderID, ItemKey
)

-- ── Step 1: every free shipment (exchange / loan movement) ───────────────────
,exchange_movements AS (
    SELECT
        TM.QOD_SHOLCH  AS DeliveredFrom,
        W.SHM_GORM     AS DeliveredFromName,
        ISNULL(b.MS_HZMNH, bb.MS_HZMNH)                AS PurchaseOrderID,
        CASE
            WHEN ISNULL(b.MS_HZMNH, bb.MS_HZMNH) LIKE '2000%' THEN 'P'
            ELSE 'E'
        END                                             AS Order_Type,
        CONVERT(VARCHAR, TM.QOD_MOTSR) + '-' + CONVERT(VARCHAR, TM.QOD_MOTSR) AS ItemKey,
        SUM(TM.MSHQL_NTO)              AS qty_loaned,
        G.QOD_GORM                     AS DeliveredTo,
        G.SHM_GORM                     AS DeliveredToName,
        SUBSTRING(TARIKH_MSHLOCH, 1, 4) + '-' + SUBSTRING(TARIKH_MSHLOCH, 5, 2) AS [Date],
        t.UnitNetPriceUSD,
        t.LineTotalNetUSD,
        t.OrderQuantity
    FROM TEODOT_MSHLOCH TM
    LEFT JOIN GORMIM G                   ON TM.QOD_MQBL       = G.QOD_GORM
    LEFT JOIN GORMIM W                   ON TM.QOD_SHOLCH      = W.QOD_GORM
    LEFT JOIN QISHOR_RCSH_LMCIRH a       ON a.MS_TEODT_MCIRH  = TM.MS_TEODH
    LEFT JOIN QISHOR_T_MSHLOCH_HZMNOT b  ON a.MS_TEODT_RCSH   = b.MS_T_MSHLOCH
    LEFT JOIN QISHOR_T_MSHLOCH_HZMNOT bb ON bb.MS_T_MSHLOCH   = TM.MS_TEODH
    LEFT JOIN (
        SELECT PurchaseOrderID, ItemKey, UnitNetPriceUSD, LineTotalNetUSD, OrderQuantity
        FROM totals
        WHERE PNLKey = 999 AND OrderQuantity <> 0
    ) t ON ISNULL(b.MS_HZMNH, bb.MS_HZMNH) = t.PurchaseOrderID
    WHERE TM.MCHIR_ICH = 0
      AND G.AOPI_PEILOT NOT IN (N'פחת', N'אחסון')
      AND SUBSTRING(TARIKH_MSHLOCH, 1, 4) + '-' + SUBSTRING(TARIKH_MSHLOCH, 5, 2) >= '2024-01'
    GROUP BY
        SUBSTRING(TARIKH_MSHLOCH, 1, 4) + '-' + SUBSTRING(TARIKH_MSHLOCH, 5, 2),
        TM.QOD_SHOLCH, W.SHM_GORM,
        b.MS_HZMNH, bb.MS_HZMNH,
        CONVERT(VARCHAR, TM.QOD_MOTSR) + '-' + CONVERT(VARCHAR, TM.QOD_MOTSR),
        G.QOD_GORM, G.SHM_GORM,
        t.UnitNetPriceUSD, t.LineTotalNetUSD, t.OrderQuantity
)

-- ── Step 2: purchase-side movements (the stock that was returned / repaid) ───
,purchase_orders AS (
    SELECT
        DeliveredFrom,
        DeliveredFromName,
        PurchaseOrderID,
        ItemKey,
        SUM(qty_loaned)      AS quantity,
        DeliveredTo,
        DeliveredToName,
        [Date]               AS Purchase_Date,
        MAX(UnitNetPriceUSD) AS max_unit_price,
        MAX(LineTotalNetUSD) AS max_LineTotalNetUSD,
        MAX(OrderQuantity)   AS max_OrderQuantity
    FROM exchange_movements
    WHERE Order_Type = 'P'
    GROUP BY
        DeliveredFrom, DeliveredFromName,
        PurchaseOrderID, ItemKey,
        DeliveredTo, DeliveredToName, [Date]
)

-- ── Step 3: rank purchase candidates, attach ship ────────────────────────────
,exchange_priced AS (
    SELECT
        b.DeliveredFrom,
        b.DeliveredFromName,
        b.PurchaseOrderID          AS Exchange_order,
        b.[Date]                   AS Exchange_Order_Date,
        b.ItemKey,
        b.qty_loaned               AS Qty_Sold,
        b.Order_Type,
        p.PurchaseOrderID          AS Purchase,
        p.DeliveredFrom            AS Purchase_Source,
        p.DeliveredFromName        AS Purchase_Source_Name,
        p.Purchase_Date,
        p.quantity                 AS Purchase_Movement_Qty,
        p.max_unit_price           AS UnitNetPriceUSD,
        p.max_LineTotalNetUSD      AS LineTotalNetUSD,
        p.max_OrderQuantity        AS OrderQuantity,
        ship_sub.ShipID,
        ROW_NUMBER() OVER (
            PARTITION BY b.ItemKey, b.PurchaseOrderID
            ORDER BY p.Purchase_Date DESC
        ) AS rn
    FROM exchange_movements b
    LEFT JOIN purchase_orders p
        ON  b.DeliveredFrom = p.DeliveredTo
        AND b.ItemKey       = p.ItemKey
    LEFT JOIN (
        -- Ship lookup: p.PurchaseOrderID = CONCAT(POL_OrderID, POL_LineID) — same format as totals_raw.
        -- Must include POL_LineID in the key, otherwise it never matches.
        SELECT
            CONCAT(
                CAST(CONVERT(BIGINT, POL.POL_OrderID) AS VARCHAR(20)),
                CAST(POL.POL_LineID AS VARCHAR(10))
            )              AS PurchaseOrderID,
            MAX(sl.ShipID) AS ShipID
        FROM PurchaseOrderLines POL
        JOIN ShipsArrivals sa ON POL.POL_ShipArrivalID = sa.SA_ID
        JOIN ShipList      sl ON sa.SA_ShipID          = sl.ShipID
        GROUP BY POL.POL_OrderID, POL.POL_LineID
    ) ship_sub ON ship_sub.PurchaseOrderID = CAST(p.PurchaseOrderID AS VARCHAR(20))
    WHERE DATEDIFF(MONTH,
            CAST(b.[Date]        + '-01' AS DATE),
            CAST(p.Purchase_Date + '-01' AS DATE)
          ) BETWEEN -3 AND 0
)

-- ── Final: one row per exchange order, mirroring what gain actually shows ────
-- Only rn = 1 (best purchase match) is used for pricing in the gain table.
-- Unmatched orders (t.* = NULL) appear here with NO MATCH status.
SELECT
    -- ── Exchange order (HZMNOT header) ──────────────────────────────────────
    HZ.MSPR_HZMNH                                                    AS Exchange_OrderID,
    CAST(
        SUBSTRING(HZ.T_ASPQH,1,4) + '-' + SUBSTRING(HZ.T_ASPQH,5,2) + '-' + SUBSTRING(HZ.T_ASPQH,7,2)
        AS DATE)                                                      AS Exchange_Date,
    HZ.MOTSR_MOZMN                                                    AS ProductID,
    HZ.CMOT_MOZMNT                                                    AS Ordered_Qty,
    HZ.ActionType                                                     AS HZ_ActionType,
    HZ.OrderStatus                                                    AS HZ_OrderStatus,

    -- ── Source warehouse (who shipped the goods) ─────────────────────────────
    t.DeliveredFrom                                                   AS DeliveredFrom_ID,
    t.DeliveredFromName                                               AS DeliveredFrom_Name,

    -- ── Delivery movement quantities ─────────────────────────────────────────
    t.Qty_Sold                                                        AS Shipped_Qty,
    t.Exchange_Order_Date                                             AS Shipped_Month,
    t.Order_Type                                                      AS Linked_PO_Type,  -- P / E / NULL

    -- ── Best-matched purchase order (rn = 1 only) ────────────────────────────
    t.Purchase                                                        AS Matched_Purchase_OrderID,
    t.Purchase_Source_Name                                            AS Purchase_Came_From,
    t.Purchase_Date                                                   AS Matched_Purchase_Month,
    t.Purchase_Movement_Qty                                           AS Purchase_Moved_Qty,
    t.OrderQuantity                                                   AS Purchase_Order_Total_Qty,

    -- ── Cost price from matched purchase ─────────────────────────────────────
    t.UnitNetPriceUSD                                                 AS Purchase_Unit_Price_USD,
    t.LineTotalNetUSD                                                 AS Purchase_Total_USD,

    -- ── Ship from matched purchase (= ShipID in gain for this exchange order) ─
    t.ShipID                                                          AS Ship_From_Purchase,

    -- ── Status — mirrors what gain will show for this exchange order ──────────
    CASE
        WHEN t.Purchase IS NULL
            THEN 'NO MATCH — price & ship will be NULL in gain'
        WHEN t.ShipID IS NOT NULL
            THEN 'OK — price found | ship: ' + CAST(t.ShipID AS VARCHAR)
        ELSE
            'Price found — ship NULL (no ShipsArrivals match for purchase PO)'
    END                                                               AS Gain_Status

FROM HZMNOT HZ
LEFT JOIN exchange_priced t
    ON  t.Exchange_order = HZ.MSPR_HZMNH
    AND t.ItemKey =
        CAST(CONVERT(INT, CONVERT(VARCHAR, HZ.MOTSR_MOZMN)) AS VARCHAR) + '-' +
        CAST(CONVERT(INT, CONVERT(VARCHAR, HZ.MOTSR_MOZMN)) AS VARCHAR)
    AND t.rn = 1   -- only the best purchase match, same as what gain uses
WHERE HZ.ActionType  IN (6, 7)
  AND HZ.OrderStatus <> 3
  AND YEAR(CAST(
        SUBSTRING(HZ.T_ASPQH,1,4) + '-' + SUBSTRING(HZ.T_ASPQH,5,2) + '-' + SUBSTRING(HZ.T_ASPQH,7,2)
        AS DATE)) >= 2024
ORDER BY
    Exchange_Date       DESC,
    Exchange_OrderID;
