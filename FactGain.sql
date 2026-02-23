WITH CurrencyConvertion as (
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
) ,LastCreation as (
Select OrderId,MAX(CreateDate) AS LastCreation
From tblOrderPriceS
Group By OrderID
)

,LastVersion as (
Select o.OrderId,Max(DayVersion) as LastVersion
From tblOrderPriceS o
Inner Join LastCreation LC
	ON o.OrderID=LC.OrderID AND o.CreateDate=LC.LastCreation
Group By o.OrderID
)

,OrderPrices as (
Select O.*
from tblOrderPriceS o
Inner Join LastCreation LC
	ON o.OrderID=LC.OrderID And o.CreateDate=LC.LastCreation
Inner Join LastVersion LV
	ON o.OrderID=LV.OrderID AND o.DayVersion = LV.LastVersion
Where 1=1
),
totals_raw AS (
    -- Step 1: per-row calculations
    SELECT
        CONCAT(
            CAST(CONVERT(BIGINT, POL.POL_OrderID) AS VARCHAR(20)),
            CAST(POL.POL_LineID AS VARCHAR(10))
        ) AS PurchaseOrderID,
        
        CONVERT(VARCHAR, HS.QOD_MOTSR) + '-' + CONVERT(VARCHAR, HS.QOD_MOTSR) AS ItemKey,

        -- PNL classification
        CASE
            WHEN HST.QOD_SHROT IN (5, 19, 30, 4)
              OR HST.QOD_SHROT IS NULL
                THEN 999
            ELSE HST.QOD_SHROT
        END AS PNLKey,

        -- Order quantity (only main item rows)
        CASE
            WHEN HS.QOD_SHROT = 0 THEN HS.CMOT
            ELSE 0
        END AS OrderQuantity,

        -- Line total in USD (exactly כמו הקטע ששלחת)
        CASE
            WHEN HS.MTBE = '$' THEN HS.MCHIR_ICH * HS.CMOT
            WHEN HS.MTBE = 'Eur' THEN HS.MCHIR_ICH * HS.CMOT * (SM.NEW_SHEREURO / SM.NEW_SHER)
            ELSE HS.MCHIR_ICH * HS.CMOT * (
                    1 / CASE WHEN HC.SHER_MTBE = 0 THEN 1 ELSE HC.SHER_MTBE END
                 )
        END AS LineTotalNetUSD
    FROM HOTSAOT_COTROT HC
    LEFT JOIN HOTSAOT_SHOROT HS
        ON HC.NOMRTOR = HS.NOMRTOR
    LEFT JOIN PurchaseOrderLines POL
        ON CONCAT(
               CAST(CONVERT(BIGINT, POL.POL_OrderID) AS VARCHAR(20)),
               CAST(POL.POL_LineID AS VARCHAR(10))
           ) = CAST(HS.MS_MSMKH_QSHOR AS VARCHAR(30))
    LEFT JOIN HOTSAOT_SHROTIM_New HST
        ON HS.QOD_SHROT = HST.QOD_SHROT
    LEFT JOIN CurrencyConvertion SM
        ON SM.TARIKH = HC.T_ERKH
    WHERE 1 = 1
      AND CAST(SUBSTRING(HC.T_MSMKH, 1, 4) AS INT) >= 2018
      AND CAST(SUBSTRING(HC.T_MSMKH, 1, 4) AS INT) <= YEAR(GETDATE())
      AND HS.QOD_SHROT NOT IN (14)
),

totals AS (
    -- Step 2: aggregate like the DAX measure
    SELECT
        PurchaseOrderID,
        ItemKey,
        999 as PNLKey,
        SUM(LineTotalNetUSD) AS LineTotalNetUSD,       -- same as [Total Expenses USD]
        SUM(OrderQuantity)   AS OrderQuantity,     -- same as [Total Order Quantity (Hzmnot)]
        SUM(LineTotalNetUSD) /
            NULLIF(SUM(OrderQuantity), 0) AS UnitNetPriceUSD  
    FROM totals_raw
    GROUP BY
        PurchaseOrderID,
        ItemKey
),

main AS (

    SELECT
        TM.QOD_SHOLCH AS DeliveredFrom,
        W.SHM_GORM AS DeliveredFromName,
        ISNULL(b.MS_HZMNH, bb.MS_HZMNH) AS PurchaseOrderID,
        CASE 
            WHEN ISNULL(b.MS_HZMNH, bb.MS_HZMNH) LIKE '2000%' THEN 'P' 
            ELSE 'E'                                                   
        END AS Order_Type,
        CONVERT(VARCHAR, TM.QOD_MOTSR) + '-' + CONVERT(VARCHAR, TM.QOD_MOTSR) AS ItemKey,
        SUM(TM.MSHQL_NTO) AS qty_loaned,
        G.QOD_GORM AS DeliveredTo,
        G.SHM_GORM AS DeliveredToName,
        SUBSTRING(TARIKH_MSHLOCH, 1, 4) + '-' + SUBSTRING(TARIKH_MSHLOCH, 5, 2) AS [Date]
    FROM TEODOT_MSHLOCH TM
    LEFT JOIN GORMIM G
        ON TM.QOD_MQBL = G.QOD_GORM    -- Customer
    LEFT JOIN GORMIM W
        ON TM.QOD_SHOLCH = W.QOD_GORM  -- Supplier / source
    LEFT JOIN QISHOR_RCSH_LMCIRH a
        ON a.MS_TEODT_MCIRH = TM.MS_TEODH
    LEFT JOIN QISHOR_T_MSHLOCH_HZMNOT b
        ON a.MS_TEODT_RCSH = b.MS_T_MSHLOCH
    LEFT JOIN QISHOR_T_MSHLOCH_HZMNOT bb
        ON bb.MS_T_MSHLOCH = TM.MS_TEODH
    WHERE 1 = 1
      AND TM.MCHIR_ICH = 0                          -- Only free/loaned shipments
      AND G.AOPI_PEILOT NOT IN (N'פחת', N'אחסון')  -- Exclude waste/storage
    GROUP BY
        SUBSTRING(TARIKH_MSHLOCH, 1, 4) + '-' + SUBSTRING(TARIKH_MSHLOCH, 5, 2),
        TM.QOD_SHOLCH,
        W.SHM_GORM,
        b.MS_HZMNH,
        CONVERT(VARCHAR, TM.QOD_MOTSR) + '-' + CONVERT(VARCHAR, TM.QOD_MOTSR),
        G.QOD_GORM,
        G.SHM_GORM,
        bb.MS_HZMNH
),

base AS (
    -- Link exchange movements to USD unit price from totals (only PNLKey=999 and non-zero order qty)
    SELECT DISTINCT
        a.*,
        b.UnitNetPriceUSD,
		b.LineTotalNetUSD,
		b.OrderQuantity 
    FROM main a
    LEFT JOIN (
        SELECT *
        FROM totals
        WHERE PNLKey = 999
          AND OrderQuantity <> 0
    ) b
        ON a.PurchaseOrderID = b.PurchaseOrderID
    WHERE a.[Date] >= '2024-01'
),

purchase_orders AS (
    -- Aggregate original purchase orders (P-type) by item and counterparty
    SELECT 
        DeliveredFrom,
        DeliveredFromName,
        PurchaseOrderID,
        ItemKey,
        SUM(qty_loaned) AS quantity,
        DeliveredTo,
        DeliveredToName,
        [Date] AS Purchase_Date,
        MAX(UnitNetPriceUSD) AS max_unit_price,
		MAX(LineTotalNetUSD) as max_LineTotalNetUSD,
		MAX(OrderQuantity)	 as max_OrderQuantity
    FROM base
    WHERE Order_Type = 'P'
    GROUP BY 
        DeliveredFrom,
        DeliveredFromName,
        PurchaseOrderID,
        ItemKey,
        DeliveredTo,
        DeliveredToName,
        [Date]
),

final AS (
    -- Match exchange (E/P) movements to the most recent purchase within 0–3 months
    SELECT 
        b.DeliveredFrom,
        b.DeliveredFromName,
        b.PurchaseOrderID AS Exchange_order,
        b.[Date] AS Exchange_Order_Date,
        b.ItemKey,
        b.qty_loaned AS Qty_Sold,
        p.PurchaseOrderID AS Purchase,
        p.Purchase_Date,
        ROW_NUMBER() OVER (
            PARTITION BY b.ItemKey, b.PurchaseOrderID
            ORDER BY p.Purchase_Date DESC
        ) AS rn,
        p.quantity,
        p.max_unit_price AS UnitNetPriceUSD,
		p.max_LineTotalNetUSD as LineTotalNetUSD,
		p.max_OrderQuantity as OrderQuantity
    FROM base b 
    LEFT JOIN purchase_orders p
        ON b.DeliveredFrom = p.DeliveredTo
       AND b.ItemKey = p.ItemKey
    WHERE 1 = 1
      -- Month-diff between exchange date and purchase date between 0 and 3
      AND CONVERT(INT, SUBSTRING(b.[Date], 1, 4) + SUBSTRING(b.[Date], 6, 2))
        - CONVERT(INT, SUBSTRING(p.Purchase_Date, 1, 4) + SUBSTRING(p.Purchase_Date, 6, 2))
        BETWEEN 0 AND 3
),
final2 as (
SELECT 
    DeliveredFrom,
    DeliveredFromName,
    Exchange_order,
    Exchange_Order_Date,
    ItemKey,
    Qty_Sold,
    Purchase,
    Purchase_Date,
    UnitNetPriceUSD,
	LineTotalNetUSD,
	OrderQuantity
FROM final
WHERE 1 = 1 
  AND rn = 1
  AND Exchange_order IS NOT NULL 
  AND UnitNetPriceUSD IS NOT NULL
  )
,PurchaseOrders AS (

	/*Exchange orders*/
    SELECT DISTINCT
        'Orders' AS DocName,
        HZ.MSPR_HZMNH AS PurchaseOrderID,
		CAST(SUBSTRING(HZ.T_ASPQH,1,4) + '-' + SUBSTRING(HZ.T_ASPQH,5,2) + '-' + SUBSTRING(HZ.T_ASPQH,7,2)
        AS DATE)                                                       AS [Value Date],
		NULL                                                                AS ShipID,
		CAST(CONVERT(INT, CONVERT(VARCHAR,HZ.MOTSR_MOZMN)) AS VARCHAR) + '-' +
		CAST(CONVERT(INT, CONVERT(VARCHAR,HZ.MOTSR_MOZMN)) AS VARCHAR)      AS ItemKey,
		HZ.QOD_SHOLCH                                                       AS SupplierKey,
		t.OrderQuantity as OrderQuantity,
		t.LineTotalNetUSD                                                   AS LineTotalNetUSD,
		999                                                                 AS PNLKey,
		'Order' as DocType
    FROM HZMNOT HZ
	LEFT JOIN final2 t
    ON t.Exchange_order = HZ.MSPR_HZMNH
	WHERE CAST(SUBSTRING(HZ.T_HZMNH,1,4) AS INT) BETWEEN 2018 AND YEAR(GETDATE())
	AND HZ.OrderStatus <> 3
	AND HZ.ActionType IN (6,7)

    UNION

    /* Purchase (Start with 2000XXXX*/
    SELECT 
    'Order Expenses' AS DocName,
    CONCAT(
	CAST(CONVERT(BIGINT, POL.POL_OrderID) AS VARCHAR(20)),
	CAST(POL.POL_LineID AS VARCHAR(10))
	)	as PurchaseOrderID,
	CASE 
	WHEN HS.MS_MSMKH_QSHOR = 0 THEN CONVERT(date, HC.T_ERKH, 112)
	ELSE  MAX(CASE WHEN HS.QOD_SHROT = 0 THEN CONVERT(date, HC.T_ERKH, 112) END) OVER (PARTITION BY CASE 
			WHEN HS.MS_MSMKH_QSHOR = 0 THEN HZ.MSPR_HZMNH 
			ELSE HS.MS_MSMKH_QSHOR 
			END) END AS [Value Date],
	sl.ShipID,
	CASE
			WHEN  HST.QOD_SHROT IS NULL OR HS.QOD_SHROT IN (5, 19, 30, 4) THEN cast(CONVERT(INT, CONVERT(VARCHAR,HS.QOD_MOTSR)) as varchar)+ '-' + cast(CONVERT(INT, CONVERT(VARCHAR,HS.QOD_MOTSR)) as varchar) -- העמסת עליות הובלה והפרש מחיר על המוצר
			ELSE cast(CONVERT(INT, CONVERT(VARCHAR,HS.QOD_MOTSR)) as varchar)+ '-' + cast(CONVERT(INT, CONVERT(VARCHAR,HST.QOD_SHROT)) as varchar)+'S' 
			END	AS ItemKey,
	CAST(CONVERT(INT, CONVERT(VARCHAR,HC.QOD_SPQ))as varchar) AS SupplierKey,
	CASE WHEN HS.QOD_SHROT = 0 then HS.CMOT
			ELSE 0
			END	AS OrderQuantity,
	CASE
	        WHEN HS.MTBE = '$' THEN HS.MCHIR_ICH*HS.CMOT 
			WHEN HS.MTBE = 'Eur' THEN HS.MCHIR_ICH*HS.CMOT * (SM.NEW_SHEREURO/SM.NEW_SHER)
	        ELSE HS.MCHIR_ICH*HS.CMOT*(1/CASE WHEN HC.SHER_MTBE=0 THEN 1 ELSE HC.SHER_MTBE END)
        END AS LineTotalNetUSD,
	CASE 
		WHEN HST.QOD_SHROT IN (5, 19, 30, 4,18) OR HST.QOD_SHROT IS NULL 
		THEN 999
		ELSE HST.QOD_SHROT
	END	AS PNLKey,
	CASE 
		    WHEN HS.QOD_SHROT IN (5, 19, 30, 20, 4) THEN N'סחורה'
		    ELSE HC.SOG_MSMKH
		END												as DocType
    FROM HOTSAOT_COTROT HC
	LEFT JOIN HOTSAOT_SHOROT HS ON HC.NOMRTOR = HS.NOMRTOR
	LEFT JOIN PurchaseOrderLines POL ON CONCAT(
										CAST(CONVERT(BIGINT, POL.POL_OrderID) AS VARCHAR(20)),
										CAST(POL.POL_LineID AS VARCHAR(10))
									)	
										= CAST(HS.MS_MSMKH_QSHOR AS VARCHAR(30))
	LEFT JOIN HZMNOT HZ ON (HS.MS_MSMKH_QSHOR = HZ.MSPR_HZMNH)
	LEFT JOIN HOTSAOT_SHROTIM_New HST ON HS.QOD_SHROT = HST.QOD_SHROT
	LEFT JOIN ShipsArrivals sa ON POL.POL_ShipArrivalID = sa.SA_ID
	LEFT JOIN ShipList sl ON sa.SA_ShipID = sl.ShipID
	LEFT JOIN PurchaseOrder PO ON POL.POL_OrderID = PO.PO_OrderID
	LEFT JOIN CurrencyConvertion SM  ON SM.TARIKH=HC.T_ERKH
	LEFT JOIN OrderPrices OP
		ON op.OrderID = POL_OrderID AND op.OrderIDLine = POL.POL_LineID
	WHERE 1=1
	and Cast(SUBSTRING(HC.T_MSMKH,1,4) as int) >=2018 and Cast(SUBSTRING(HC.T_MSMKH,1,4) as int) <= YEAR(GETDATE())
	and HS.QOD_SHROT NOT IN (14)
)

SELECT
*
FROM PurchaseOrders