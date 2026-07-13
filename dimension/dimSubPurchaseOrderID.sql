WITH
-- =========================
-- 1) Exchange Orders (parse dates once)
-- =========================
ExchangeOrders AS (
    SELECT
        HZ.MSPR_HZMNH AS OrderID,
        TRY_CONVERT(date, HZ.T_ASPQH, 112) AS [Value Date],
        CONVERT(char(6), TRY_CONVERT(date, HZ.T_ASPQH, 112), 112) AS [Year Month]
    FROM HZMNOT HZ
),
ExchangeOrdersOnly AS (
    SELECT
        HZ.MSPR_HZMNH,
        TRY_CONVERT(int, LEFT(HZ.T_HZMNH, 4)) AS OrderYear
    FROM HZMNOT HZ
    WHERE
        TRY_CONVERT(int, LEFT(HZ.T_HZMNH, 4)) >= 2024
        AND TRY_CONVERT(int, LEFT(HZ.T_HZMNH, 4)) BETWEEN 2018 AND YEAR(GETDATE())
        AND HZ.OrderStatus <> 3
        AND HZ.ActionType IN (6, 7)
),

-- =========================
-- 2) Purchase Order value date/yearmonth (avoid DISTINCT + repeated logic)
-- =========================
PO_Base AS (
    SELECT
        HS.MS_MSMKH_QSHOR,
        -- Calculate the grouping key once
        CASE
            WHEN HS.MS_MSMKH_QSHOR = 0 THEN HZ.MSPR_HZMNH
            ELSE HS.MS_MSMKH_QSHOR
        END AS OrderKey,
        HS.QOD_SHROT,
        TRY_CONVERT(date, HC.T_ERKH, 112) AS DocDate
    FROM HOTSAOT_COTROT HC
    LEFT JOIN HOTSAOT_SHOROT HS
        ON HC.NOMRTOR = HS.NOMRTOR
    LEFT JOIN PurchaseOrderLines POL
        ON CONCAT(
            CAST(CONVERT(bigint, POL.POL_OrderID) AS varchar(20)),
            CAST(POL.POL_LineID AS varchar(10))
        ) = CAST(HS.MS_MSMKH_QSHOR AS varchar(30))
    LEFT JOIN HZMNOT HZ
        ON HS.MS_MSMKH_QSHOR = HZ.MSPR_HZMNH
),
Purchase_order AS (
    SELECT
        MS_MSMKH_QSHOR,
        -- Keep original behavior:
        -- If MS_MSMKH_QSHOR=0 take that row's DocDate, else take MIN DocDate where QOD_SHROT=0 per OrderKey.
        CASE
            WHEN MS_MSMKH_QSHOR = 0 THEN MAX(DocDate)
            ELSE MIN(CASE WHEN QOD_SHROT = 0 THEN DocDate END)
        END AS [Value Date],
        CASE
            WHEN MS_MSMKH_QSHOR = 0 THEN CONVERT(char(6), MAX(DocDate), 112)
            ELSE CONVERT(char(6), MIN(CASE WHEN QOD_SHROT = 0 THEN DocDate END), 112)
        END AS [Year Month]
    FROM PO_Base
    GROUP BY
        MS_MSMKH_QSHOR,
        OrderKey
),

-- =========================
-- 3) Shared enrichment for both Purchase + Exchange
-- =========================
BaseLink AS (
    SELECT
        a.MS_TEODT_MCIRH AS DeliveryNote,
        a.MS_TEODT_RCSH,
        b.MS_HZMNH AS PurchaseOrderID,
        ISNULL(sl.ShipDesc, '-') AS ShipDesc
    FROM QISHOR_RCSH_LMCIRH a
    LEFT JOIN QISHOR_T_MSHLOCH_HZMNOT b
        ON a.MS_TEODT_RCSH = b.MS_T_MSHLOCH
    LEFT JOIN PurchaseOrderLines POL
        ON b.MS_HZMNH = POL.POL_SQL_POID
    LEFT JOIN ShipsArrivals sa
        ON POL.POL_ShipArrivalID = sa.SA_ID
    LEFT JOIN ShipList sl
        ON sa.SA_ShipID = sl.ShipID
),

-- =========================
-- 4) Purchase / Exchange (keep INNER JOIN filters)
-- =========================
Purchase AS (
    SELECT
        bl.DeliveryNote,
        bl.MS_TEODT_RCSH,
        CAST(bl.PurchaseOrderID AS VARCHAR) as PurchaseOrderID,
        bl.ShipDesc,
        COALESCE(p.[Value Date], o.[Value Date]) AS [Value Date],
        -- Display month: choose a cheaper format (example: yyyy-MM)
        CONVERT(char(7), COALESCE(p.[Value Date], o.[Value Date]), 120) AS ValueDateMonth,
        DENSE_RANK() OVER (
            ORDER BY COALESCE(p.[Year Month], o.[Year Month]) DESC
        ) AS Sort
    FROM BaseLink bl
    INNER JOIN PurchaseOrderLines polFilter
        ON bl.PurchaseOrderID = polFilter.POL_SQL_POID
    LEFT JOIN Purchase_order p
        ON bl.PurchaseOrderID = p.MS_MSMKH_QSHOR
    LEFT JOIN ExchangeOrders o
        ON bl.PurchaseOrderID = o.OrderID
),
Exchange AS (
    SELECT
        bl.DeliveryNote,
        bl.MS_TEODT_RCSH,
        CAST(bl.PurchaseOrderID AS VARCHAR) as PurchaseOrderID,
        bl.ShipDesc,
        COALESCE(p.[Value Date], o.[Value Date]) AS [Value Date],
        CONVERT(char(7), COALESCE(p.[Value Date], o.[Value Date]), 120) AS ValueDateMonth,
        DENSE_RANK() OVER (
            ORDER BY COALESCE(p.[Year Month], o.[Year Month]) DESC
        ) AS Sort
    FROM BaseLink bl
    INNER JOIN ExchangeOrdersOnly ex
        ON ex.MSPR_HZMNH = bl.PurchaseOrderID
    LEFT JOIN Purchase_order p
        ON bl.PurchaseOrderID = p.MS_MSMKH_QSHOR
    LEFT JOIN ExchangeOrders o
        ON bl.PurchaseOrderID = o.OrderID
),
Final as (
SELECT * FROM Purchase
UNION ALL
SELECT * FROM Exchange)
, RankedDeliveryNote AS (
    SELECT
        DeliveryNote,
        PurchaseOrderID,
        ShipDesc,
        [Value Date],
        ValueDateMonth,
        Sort,
        ROW_NUMBER() OVER (
            PARTITION BY DeliveryNote
            ORDER BY PurchaseOrderID DESC) AS rn
    FROM Final
)

SELECT
    DeliveryNote,
    PurchaseOrderID,
    ShipDesc,
    [Value Date],
    ValueDateMonth,
    Sort
FROM RankedDeliveryNote
WHERE rn = 1