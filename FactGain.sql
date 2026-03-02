-- Create a temporary table with the values
DECLARE @SubItemsMapping TABLE (
    ItemID varchar(10),
    ItemDesc varchar(20)
);

-- Insert values into the table variable
INSERT INTO @SubItemsMapping (ItemID, ItemDesc)
VALUES (9990, 'ריבית'),
       (9991, 'הובלה'),
       (9992, 'הפרשי שער');

with CurrencyConvertion as (
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
,LastCreation as (
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
--------------------------------------------------------------- the below part should be a view is the database ----------------------------------------------------
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
            NULLIF(SUM(OrderQuantity), 0) AS UnitNetPriceUSD  -- Avg Price USD (Expenses)
    FROM totals_raw
    GROUP BY
        PurchaseOrderID,
        ItemKey--,
        --PNLKey
),
--select * from totals
--where PurchaseOrderID = 20005431
main AS (
    -- Monthly quantity of items loaned between parties (exchanges), by PO and item
    SELECT
        TM.QOD_SHOLCH AS DeliveredFrom,
        W.SHM_GORM AS DeliveredFromName,
        ISNULL(b.MS_HZMNH, bb.MS_HZMNH) AS PurchaseOrderID,
        CASE 
            WHEN ISNULL(b.MS_HZMNH, bb.MS_HZMNH) LIKE '2000%' THEN 'P' -- Purchase order
            ELSE 'E'                                                   -- Expense / other
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

-----------------------------------------------------------------------------------------------------------------------------------------------
,Purchase_Exchange as (

----Invoice--
SELECT
	 CAST(CCS.HZMNT_RCSH AS VARCHAR)  AS PurchaseOrderID,
     'Invoice'                                            AS DocName,
     NULL                          AS SupplierKey,
     CAST(SUBSTRING(CCS.T_CHSHBONIT,1,4) + '-' + SUBSTRING(CCS.T_CHSHBONIT,5,2) + '-' + SUBSTRING(CCS.T_CHSHBONIT,7,2) AS DATE) AS [Value Date],
     CAST(CONVERT(VARCHAR, POL_ProductID) AS VARCHAR) + '-' + CAST(CONVERT(VARCHAR, M.ServiceCode) AS VARCHAR) + 'S' AS ItemKey,
     CAST(CCS.MCHIR_ICH_LLA_ME_M / (CASE WHEN SHER_LCHISHOB <> 0 THEN SHER_LCHISHOB ELSE CC2.new_sher END) AS DECIMAL(12,4)) * -1  AS UnitNetPriceUSD,
     CCS.MSHQL_NTO                   AS Quantity,
     CASE WHEN CCS.QOD_MOTSR = 0 THEN CCS.MSHQL_NTO ELSE 0 END AS OrderQuantity,
     HST.QOD_SHROT                           AS PNLKey,
	 HST.PNL as [PNL Code],
     CAST(CCS.CMOT * (CCS.MCHIR_ICH_LLA_ME_M / (CASE WHEN SHER_LCHISHOB <> 0 THEN SHER_LCHISHOB ELSE CC2.new_sher END)) AS DECIMAL(12,4)) * -1 AS LineTotalNetUSD,
     CONVERT(INT, CONVERT(VARCHAR, SUBSTRING(CCS.T_CHSHBONIT,1,4) + SUBSTRING(CCS.T_CHSHBONIT,5,2))) AS YearMonth,
	 null as ShipID,
    'Invoice'                                AS DocType,
	CASE WHEN SHER_LCHISHOB <> 0 THEN SHER_LCHISHOB ELSE CC2.new_sher END as [NEW_SHER]
FROM [dbo].[CHIOBI_CHOTS_COTROT] CC
LEFT JOIN GORMIM G2
       ON CC.QOD_LQOCH = G2.QOD_GORM
LEFT JOIN TBLT_ANSHI_MCIROT AM2
       ON AM2.SHM_AISH_MCIROT = G2.AISH_MCIROT_MTPL
LEFT JOIN [dbo].[CHIOBI_CHOTS_SHOROT] CCS
       ON CC.MS_CHSHBONIT = CCS.MS_CHSHBONIT
LEFT JOIN CurrencyConvertion CC2
       ON CCS.[TARIKH_MSHLOCH] = CC2.Tarikh
LEFT JOIN ( ---- Connect to the base purchase in order to get the relative productID
			select DISTINCT
			POL_SQL_POID, POL_ProductID
			from PurchaseOrderLines POL
			) POL
		ON CCS.HZMNT_RCSH = POL.POL_SQL_POID
LEFT JOIN MOTSRIM M
	ON CCS.QOD_MOTSR = M.QOD_MOTSR
LEFT JOIN HOTSAOT_SHROTIM_New HST 
	ON M.ServiceCode = HST.QOD_SHROT
	where CAST(SUBSTRING(CCS.T_CHSHBONIT,1,4) + '-' + SUBSTRING(CCS.T_CHSHBONIT,5,2) + '-' + SUBSTRING(CCS.T_CHSHBONIT,7,2) AS DATE) >= '2024-01-01'

UNION ALL
	------Import--------
SELECT

		CONCAT(
			CAST(CONVERT(BIGINT, POL.POL_OrderID) AS VARCHAR(20)),
			CAST(POL.POL_LineID AS VARCHAR(10))
		 )	as 'PurchaseOrderID'
		,'Order Expenses' AS 'DocName'
		,CAST(CONVERT(INT, CONVERT(VARCHAR,HC.QOD_SPQ))as varchar) AS 'SupplierKey'
		,CASE 
			WHEN HS.MS_MSMKH_QSHOR = 0 
			  THEN CONVERT(date, HC.T_ERKH, 112)
			ELSE
			  -- pull the header-row date per order (using corrected key)
			  MAX(
				CASE WHEN HS.QOD_SHROT = 0 
					 THEN CONVERT(date, HC.T_ERKH, 112) 
				END
			  ) OVER (
				PARTITION BY 
				  CASE 
					WHEN HS.MS_MSMKH_QSHOR = 0 
					  THEN HZ.MSPR_HZMNH 
					ELSE HS.MS_MSMKH_QSHOR 
				  END
			  )
		  END AS 'Value Date'
		,CASE
			--WHEN  HST.QOD_SHROT IS NULL THEN cast(CONVERT(INT, CONVERT(VARCHAR,HS.QOD_MOTSR)) as varchar)+ '-' + cast(CONVERT(INT, CONVERT(VARCHAR,HS.QOD_MOTSR)) as varchar) - לפני שינוי של העמסת עלויות
			WHEN  HST.QOD_SHROT IS NULL OR HS.QOD_SHROT IN (5, 19, 30, 4) THEN cast(CONVERT(INT, CONVERT(VARCHAR,HS.QOD_MOTSR)) as varchar)+ '-' + cast(CONVERT(INT, CONVERT(VARCHAR,HS.QOD_MOTSR)) as varchar) -- העמסת עליות הובלה והפרש מחיר על המוצר
			ELSE cast(CONVERT(INT, CONVERT(VARCHAR,HS.QOD_MOTSR)) as varchar)+ '-' + cast(CONVERT(INT, CONVERT(VARCHAR,HST.QOD_SHROT)) as varchar)+'S' 
			END		AS 'ItemKey'

		 ,CASE
			WHEN HST.QOD_SHROT IS NOT NULL THEN
				CASE 
					WHEN HS.MTBE = '$' THEN 
						CAST(HS.MCHIR_ICH / NULLIF(POL.POL_FinalWeightReceived, 0) AS decimal(12,4))
					WHEN HS.MTBE = 'Eur' THEN
						CAST(
							HS.MCHIR_ICH * (SM.NEW_SHEREURO/SM.NEW_SHER) / NULLIF(POL.POL_FinalWeightReceived, 0)
							AS decimal(12,4)
						)
					ELSE 
						CAST(
							HS.MCHIR_ICH * (1 / NULLIF(HC.SHER_MTBE, 0)) / NULLIF(POL.POL_FinalWeightReceived, 0)
							AS decimal(12,4)
						)
				END
			ELSE 
				CASE 
					WHEN HS.MTBE = '$' THEN HS.MCHIR_ICH
					WHEN HS.MTBE = 'Eur' THEN
						CAST(
							HS.MCHIR_ICH * (SM.NEW_SHEREURO/SM.NEW_SHER)
							AS decimal(12,4)
						)
					ELSE 
						CAST(
							HS.MCHIR_ICH * (1 / NULLIF(HC.SHER_MTBE, 0)) 
							AS decimal(12,4)
						)
				END
		END AS 'UnitNetPriceUSD'
		,HS.CMOT AS 'Quantity'
		,CASE WHEN HS.QOD_SHROT = 0 then HS.CMOT
				ELSE 0
				END									AS 'OrderQuantity'
		,CASE 
			WHEN HST.QOD_SHROT IN (5, 19, 30, 4,18) OR HST.QOD_SHROT IS NULL 
			THEN 999
			ELSE HST.QOD_SHROT
		END											AS 'PNLKey' ----------  העמסת עליות הובלה על המוצר והפרש מחיר
			,case when HST.QOD_SHROT IN (5, 19, 30, 4,18) OR HST.QOD_SHROT IS NULL  then 1010 
			else HST.PNL end as [PNL Code]
			,CASE
	         WHEN HS.MTBE = '$' THEN HS.MCHIR_ICH*HS.CMOT 
			 WHEN HS.MTBE = 'Eur' THEN HS.MCHIR_ICH*HS.CMOT * (SM.NEW_SHEREURO/SM.NEW_SHER)
	         ELSE HS.MCHIR_ICH*HS.CMOT*(1/CASE WHEN HC.SHER_MTBE=0 THEN 1 ELSE HC.SHER_MTBE END)
         END AS 'LineTotalNetUSD'
		,CONVERT(INT, CONVERT(VARCHAR,SUBSTRING(HS.T_ERKH,1,4) + SUBSTRING(HS.T_ERKH,5,2))) AS YearMonth
		,sl.ShipID
		,CASE 
		    WHEN HS.QOD_SHROT IN (5, 19, 30, 20, 4) THEN N'סחורה'
		    ELSE HC.SOG_MSMKH
		END												as DocType
		,SM.NEW_SHER
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
and CONCAT(CAST(CONVERT(BIGINT, POL.POL_OrderID) AS VARCHAR(20)),CAST(POL.POL_LineID AS VARCHAR(10))) <> ' '
and  cast( HC.T_ERKH as date) >= '2024-01-01'

UNION ALL

-----Exchange----
SELECT
    HZ.MSPR_HZMNH                                                       AS PurchaseOrderID,
    'Orders'                                                            AS DocName,
    HZ.QOD_SHOLCH                                                       AS SupplierKey,
    CAST(SUBSTRING(HZ.T_ASPQH,1,4) + '-' + SUBSTRING(HZ.T_ASPQH,5,2) + '-' + SUBSTRING(HZ.T_ASPQH,7,2)
         AS DATE)                                                       AS [Value Date],
    CAST(CONVERT(INT, CONVERT(VARCHAR,HZ.MOTSR_MOZMN)) AS VARCHAR) + '-' +
    CAST(CONVERT(INT, CONVERT(VARCHAR,HZ.MOTSR_MOZMN)) AS VARCHAR)      AS ItemKey,

    -- >>> Take unit price from totals (Avg Price USD (Expenses))
    t.UnitNetPriceUSD                                                   AS UnitNetPriceUSD,
    HZ.CMOT_MOZMNT                                                      AS Quantity,
    t.OrderQuantity                                                     AS OrderQuantity,
    999                                                                 AS PNLKey,
	1010  as [PNL Code],
    t.LineTotalNetUSD                                                  AS LineTotalNetUSD,
    CONVERT(INT, CONVERT(VARCHAR, SUBSTRING(HZ.T_ASPQH,1,4) + SUBSTRING(HZ.T_ASPQH,5,2))) AS YearMonth,
    NULL                                                                AS ShipID,
    'Order'                                                             AS DocType,
    SM.NEW_SHER
FROM HZMNOT HZ
LEFT JOIN CurrencyConvertion SM
    ON SM.TARIKH = HZ.T_HZMNH
LEFT JOIN final2 t
    ON t.Exchange_order = HZ.MSPR_HZMNH
   AND t.ItemKey =
        CAST(CONVERT(INT, CONVERT(VARCHAR,HZ.MOTSR_MOZMN)) AS VARCHAR) + '-' +
        CAST(CONVERT(INT, CONVERT(VARCHAR,HZ.MOTSR_MOZMN)) AS VARCHAR)
LEFT JOIN OrderPrices OP
    ON OP.OrderID = HZ.MSPR_HZMNH
WHERE CAST(SUBSTRING(HZ.T_HZMNH,1,4) AS INT) BETWEEN 2018 AND YEAR(GETDATE())
  AND HZ.OrderStatus <> 3
  AND HZ.ActionType IN (6,7)
  AND YEAR(CAST(SUBSTRING(HZ.T_ASPQH,1,4) + '-' + SUBSTRING(HZ.T_ASPQH,5,2) + '-' + SUBSTRING(HZ.T_ASPQH,7,2) AS DATE))  >= 2024
  )
, P_costs as (
    select 
	PurchaseOrderID,
	min([Value Date]) as ValueDate,
	max(case when [PNLKey] = 999 then SupplierKey else null end) as SupplierKey,
	max(ShipID) as boat,
	SUM(CASE WHEN [PNL Code] = 2270 THEN LineTotalNetUSD ELSE 0 END) as DischargeCosts,
	SUM(CASE WHEN [PNL Code] = 1492 THEN LineTotalNetUSD ELSE 0 END)/nullif(sum(orderquantity),0) as [Analysis fees],
	SUM(CASE WHEN [PNL Code] = 8210 THEN LineTotalNetUSD ELSE 0 END)/nullif(sum(orderquantity),0) as [Back to back haulage],
	SUM(CASE WHEN [PNL Code] = 8100 THEN LineTotalNetUSD ELSE 0 END)/nullif(sum(orderquantity),0) as [Balance quantities],
	SUM(CASE WHEN [PNL Code] = 1624 THEN LineTotalNetUSD ELSE 0 END)/nullif(sum(orderquantity),0) as [Delays (Dagon)],
	SUM(CASE WHEN [PNL Code] = 1201 THEN LineTotalNetUSD ELSE 0 END)/nullif(sum(orderquantity),0) as [demurrage / Despatch],
	SUM(CASE WHEN [PNL Code] = 7210 THEN LineTotalNetUSD ELSE 0 END)/nullif(sum(orderquantity),0) as [Haulage],
	SUM(CASE WHEN [PNL Code] = 4103 THEN LineTotalNetUSD ELSE 0 END)/nullif(sum(orderquantity),0) as [Ocean freight],
	SUM(CASE WHEN [PNL Code] = 4126 THEN LineTotalNetUSD ELSE 0 END)/nullif(sum(orderquantity),0) as [Overage Premium Owner],
	SUM(CASE WHEN [PNL Code] = 1496 THEN LineTotalNetUSD ELSE 0 END)/nullif(sum(orderquantity),0) as [Processing],
	SUM(CASE WHEN [PNL Code] = 1211 THEN LineTotalNetUSD ELSE 0 END)/nullif(sum(orderquantity),0) as [Quality settlement],
	SUM(CASE WHEN [PNL Code] = 1111 THEN LineTotalNetUSD ELSE 0 END)/nullif(sum(orderquantity),0) as [Shortage],
	SUM(CASE WHEN [PNL Code] = 1497 THEN LineTotalNetUSD ELSE 0 END)/nullif(sum(orderquantity),0) as [Warehouse operation],
	SUM(CASE WHEN [PNL Code] = 3620 THEN LineTotalNetUSD ELSE 0 END)/nullif(sum(orderquantity),0) as [Warehouse service],
	SUM(CASE WHEN [PNL Code] = 1571 THEN LineTotalNetUSD ELSE 0 END)/nullif(sum(orderquantity),0) as [Washout],
	SUM(CASE WHEN [PNL Code] not in (1010,2270) THEN LineTotalNetUSD ELSE 0 END)/nullif(sum(orderquantity),0) as [all_except_purchase],
	sum (orderquantity) as orderquantity,
	sum(LineTotalNetUSD) as LineTotalNetUSD,
	SUM(CASE WHEN PNLKey = 999 THEN LineTotalNetUSD ELSE 0 END)
	/
	NULLIF(
	SUM(CASE WHEN PNLKey = 999 THEN OrderQuantity ELSE 0 END),0)
	AS Cif_price,
	sum(CASE WHEN [PNL Code] in (1010,1201,2270) THEN LineTotalNetUSD ELSE 0 END) as totalFOT
  from Purchase_Exchange
  group by PurchaseOrderID
  )
  , sales as (
  SELECT 
	'Invoice' AS 'DocName'
		,Case
			WHEN CS.QOD_MOTSR = TM.QOD_MOTSR
				THEN 'Item'
			ELSE 'Additional Expense'
		END AS 'LineType'
		,CONVERT(VARCHAR,CS.QOD_LQOCH) 'AccountKey' -- customer from invoices in case you need just invoices 
		,SUBSTRING(CS.T_CHSHBONIT,1,4) + '-' + SUBSTRING(CS.T_CHSHBONIT,5,2) + '-' + SUBSTRING(CS.T_CHSHBONIT,7,2) AS 'Date' -- Invoice date  
		,CAST(CONVERT(INT, CONVERT(VARCHAR,AM.QOD))as varchar) 'AgentKey' -- Agent from Customer method
		,CAST( CONVERT(VARCHAR,CS.QOD_MOTSR) as varchar) +'-' + CAST( CONVERT(VARCHAR,CS.QOD_MOTSR) as varchar) AS 'ItemKey'
		,CAST(CS.MCHIR_ICH_LLA_ME_M/(case when SHER_LCHISHOB<>0 then SHER_LCHISHOB else 1 end) AS decimal (12,2)) AS 'UnitNetPriceUSD'
		,CS.MSHQL_NTO AS 'Quantity'
		,case
		when TM.ActionType = 11 and CS.QOD_MOTSR = TM.QOD_MOTSR then TM.MSHQL_NTO
		else 0 end
	as qty_cif
	,case
		when TM.ActionType = 1 and CS.QOD_MOTSR = TM.QOD_MOTSR then TM.MSHQL_NTO
		else 0 end
	as qty_fot
	,case
		when TM.ActionType = 1 then 'FOT'
		WHEN TM.ActionType = 11 then 'CIF'
		WHEN TM.ActionType = 12 then 'FOT Premium'
	else null 
	end as SalesType
		,CASE
			WHEN CS.QOD_MOTSR=TM.QOD_MOTSR
				THEN 0
			ELSE Cs.CMOT
		END as AdditionalQuantity
		,CAST(CASE 
			WHEN CS.QOD_MOTSR<>0 
				THEN CAST((CS.MCHIR_ICH_LLA_ME_M * CS.MSHQL_NTO)/(case when SHER_LCHISHOB<>0 then SHER_LCHISHOB else CC.new_sher end) AS decimal (18,2))
		     ELSE CAST((CS.MCHIR_ICH_LLA_ME_M)/case when SHER_LCHISHOB<>0 then SHER_LCHISHOB else CC.new_sher end AS decimal (18,2)) 
		END
		+
		CASE
			WHEN CS.QOD_MOTSR=TM.QOD_MOTSR
				THEN 0
			WHEN TM.QOD_MOTSR IS NULL
				THEN 0
			ELSE Cs.CMOT * CAST(CS.MCHIR_ICH_LLA_ME_M/(case when SHER_LCHISHOB<>0 then SHER_LCHISHOB else CC.new_sher  end) AS decimal (12,2))
		END AS decimal (18,2))		AS'LineTotalNet_USD' --Invoices are always in NIS
		,CONVERT(INT, CONVERT(VARCHAR,SUBSTRING(CS.T_CHSHBONIT,1,4) + SUBSTRING(CS.T_CHSHBONIT,5,2))) AS YearMonth
		, TM.MS_TEODH as 'DeliveryNote'
		,CASE
			WHEN SUBSTRING(TM.TARIKH_MSHLOCH,1,4) = '0000'
				THEN CAST(SUBSTRING(CS.T_CHSHBONIT,1,4) + '-' + SUBSTRING(CS.T_CHSHBONIT,5,2) + '-' + SUBSTRING(CS.T_CHSHBONIT,7,2) as date)
			ELSE CAST(ISNULL(SUBSTRING(TM.TARIKH_MSHLOCH,1,4) + '-' + SUBSTRING(TM.TARIKH_MSHLOCH,5,2) + '-' + SUBSTRING(TM.TARIKH_MSHLOCH,7,2),
		ISNULL(SUBSTRING(FTM.FirstDeliveryDateTM,1,4) + '-' + SUBSTRING(FTM.FirstDeliveryDateTM,5,2) + '-' + SUBSTRING(FTM.FirstDeliveryDateTM,7,2),
		SUBSTRING(CS.T_CHSHBONIT,1,4) + '-' + SUBSTRING(CS.T_CHSHBONIT,5,2) + '-' + SUBSTRING(CS.T_CHSHBONIT,7,2)))
		AS date) 
		END as 'DeliveryDate'
		,HZ.MSPR_HZMNH as 'OrderID'
		,TM.QOD_SHOLCH as 'SupplierWarehouse'
		,TM.ActionType							as 'ActionType'
		,CASE 
			WHEN TM.ActionType = 6 THEN G.AOPI_PEILOT
			ELSE act.ActionType
		END										as 'ActionTypeDesc'										
		,'0'									as 'AdjustmentFlag'
		,'Sales'								as  'QuantityCategory'
FROM CHSHBONIOT_COTROT CH
Left Join GORMIM G
	on CH.QOD_LQOCH = G.QOD_GORM
Left Join TBLT_ANSHI_MCIROT AM 
	on AM.SHM_AISH_MCIROT = G.AISH_MCIROT_MTPL
LEFT JOIN CHSHBONIOT_SHOROT CS 
    ON CH.MS_CHSHBONIT = CS.MS_CHSHBONIT
LEFT JOIN @SubItemsMapping SIM 
	ON (
		CASE WHEN (TAOR_MOTSR LIKE '%ריבית%' OR TAOR_MOTSR LIKE '%הובלה%' OR TAOR_MOTSR LIKE '%הפרשי שער%') AND  CS.QOD_MOTSR=0 AND TAOR_MOTSR LIKE '%'+SIM.ItemDesc+'%'
		then 1 else 0 end = 1)
LEFT JOIN TEODOT_MSHLOCH TM 
	ON CS.MS_T_MSHLOCH = TM.MS_TEODH
LEFT JOIN (SELECT MS_CHSHBONIT , Min (TM.TARIKH_MSHLOCH) as 'FirstDeliveryDateTM',Min (CHS.TARIKH_MSHLOCH) as 'FirstDeliveryDateCHS'
			FROM CHSHBONIOT_SHOROT CHS
			Left Join TEODOT_MSHLOCH TM
				ON TM.MS_TEODH = CHS.MS_T_MSHLOCH
			Where 1=1
			AND TM.TARIKH_MSHLOCH <>00000000
			AND CHS.TARIKH_MSHLOCH <>00000000
			Group By CHS.MS_CHSHBONIT) FTM
	ON FTM.MS_CHSHBONIT = CH.MS_CHSHBONIT
Left Join (SELECT MS_HZMNH,MS_T_MSHLOCH
			FROM QISHOR_T_MSHLOCH_HZMNOT
			) HZT
	on TM.MS_TEODH = HZT.MS_T_MSHLOCH
Left Join HZMNOT HZ
	on HZ.MSPR_HZMNH = HZT.MS_HZMNH
Left Join (
			Select *
			From GORMIM
			Where EntityType Like N'%מקום אספקה%') W
	On W.QOD_GORM = TM.QOD_SHOLCH
left join TBLT_PEOLOT_HZMNH_T_MSHLOCH act
	on TM.ActionType = act.MS_AOPTSIH
left join CurrencyConvertion CC 
	ON cs.[TARIKH_MSHLOCH] = cc.Tarikh
WHERE 1=1  
AND Cast(SUBSTRING(cs.T_CHSHBONIT,1,4) as int) >=2018 and Cast(SUBSTRING(cs.T_CHSHBONIT,1,4) as int) <= Year(Getdate())

-------OPEN Orders----------------------------------------------------------------------------------
UNION ALL

 SELECT 
	'Delivery Note' as  'DocName'
	,'Item' AS 'LineType'
	,CONVERT(INT, CONVERT(VARCHAR, TM.QOD_MQBL)) AS 'AccountKey'
	,SUBSTRING(TARIKH_MSHLOCH,1,4) + '-' + SUBSTRING(TARIKH_MSHLOCH,5,2) + '-' + SUBSTRING(TARIKH_MSHLOCH,7,2) as 'Date'
	,CONVERT(INT, CONVERT(VARCHAR, TM.AISH_MCIROT)) AS 'AgentKey'
	,CONVERT(VARCHAR, TM.QOD_MOTSR)+'-'+CONVERT(VARCHAR, TM.QOD_MOTSR) AS 'ItemKey'
	,CASE
	         WHEN TM.MTBE_SH = '$' THEN CAST(TM.MCHIR_ICH as decimal(12,2))
		   --THEN CAST(HZ.MCHIR_ICH AS decimal (12,4))
	         ELSE CAST(TM.MCHIR_ICH *(1/SM.NEW_SHER)  AS decimal (12,2))
         END AS 'UnitNetPriceUSD'
	,TM.MSHQL_NTO as 'Quantity'
	,case
		when TM.ActionType = 11 then TM.MSHQL_NTO
		else 0 end
	as qty_cif
	,case
		when TM.ActionType = 1 then TM.MSHQL_NTO
		else 0 end
	as qty_fot
	,case
		when TM.ActionType = 1 then 'FOT'
		WHEN TM.ActionType = 11 then 'CIF'
		WHEN TM.ActionType = 12 then 'FOT Premium'
	else null 
	end as SalesType
	,0 AS 'AdditionalQuantity'
	,CASE
	         WHEN TM.MTBE_SH = '$'
		     THEN CAST((TM.MCHIR_ICH)*TM.MSHQL_NTO/*TM.CMOT_SHSOPQH*/ AS decimal (18,2))
	         ELSE CAST(TM.MCHIR_ICH * TM.MSHQL_NTO/* TM.CMOT_SHSOPQH*/ * (1/SM.NEW_SHER) AS decimal (18,2))
         END AS 'LineTotalNet_USD'
	,CONVERT(INT, CONVERT(VARCHAR,SUBSTRING(T_ASPQH,1,4) + SUBSTRING(T_ASPQH,5,2))) AS YearMonth
	,TM.MS_TEODH as 'DeliveryNote'
	,CAST(SUBSTRING(TARIKH_MSHLOCH,1,4) + '-' + SUBSTRING(TARIKH_MSHLOCH,5,2) + '-' + SUBSTRING(TARIKH_MSHLOCH,7,2) as date) as 'DeliveryDate'
	,HZ.MSPR_HZMNH as 'OrderID'
	,TM.QOD_SHOLCH as 'SupplierWarehouse'
	,TM.ActionType							as 'ActionType'
	,CASE 
		WHEN TM.ActionType = 6 THEN G.AOPI_PEILOT
		ELSE act.ActionType
	END as 'ActionTypeDesc'
	,CASE
	WHEN TM.MCHIR_ICH = 0
		THEN 1
	ELSE 0
	END AS 'AdjustmentFlag'
	,CASE
		WHEN TM.MCHIR_ICH <> 0 THEN 'Sales'
		WHEN TM.MCHIR_ICH = 0 and G.AOPI_PEILOT = 'פחת' THEN 'Shortage'
		WHEN TM.MCHIR_ICH = 0 and G.AOPI_PEILOT = 'אחסון' THEN 'Storage'
		WHEN TM.MCHIR_ICH = 0 and G.AOPI_PEILOT NOT IN ('פחת','אחסון') then 'Exchange'
	END AS 'QuantityCategory'
FROM TEODOT_MSHLOCH TM
Left Join (SELECT distinct MS_T_MSHLOCH
			FROM CHSHBONIOT_SHOROT
		) CH
	on TM.MS_TEODH = CH.MS_T_MSHLOCH
Left Join (SELECT MS_HZMNH,MS_T_MSHLOCH
			FROM QISHOR_T_MSHLOCH_HZMNOT
		) HZT
	on TM.MS_TEODH = HZT.MS_T_MSHLOCH
Left Join HZMNOT HZ
	on HZ.MSPR_HZMNH = HZT.MS_HZMNH
Left Join (select *, 
Case
	when sher = 0
		then first_value(sher) over (partition by value_partition order by Tarikh) 
	else sher
end as new_sher
from (
  select *, sum(case when sher=0 then 0 else 1 end) over (order by tarikh ) as value_partition
  from SHERI_MTBE) m

) SM  
	on SM.TARIKH=HZ.T_HZMNH
Left Join GORMIM G
	on TM.QOD_MQBL = G.QOD_GORM
Left Join (
			Select *
			From GORMIM
			Where EntityType Like N'%מקום אספקה%') W
	On W.QOD_GORM = TM.QOD_SHOLCH
left join TBLT_PEOLOT_HZMNH_T_MSHLOCH act
	on TM.ActionType = act.MS_AOPTSIH
WHERE CH.MS_T_MSHLOCH is null
AND Cast(SUBSTRING(TARIKH_MSHLOCH,1,4) as int) >=2018 and Cast(SUBSTRING(TARIKH_MSHLOCH,1,4) as int) <= Year(Getdate())
AND STTOS in (0,1)
AND TM.PurchaseOrderType = 0
  )

  , base_link as (
  SELECT distinct
        a.MS_TEODT_MCIRH AS DeliveryNote,
        b.MS_HZMNH AS PurchaseOrderID
    FROM QISHOR_RCSH_LMCIRH a
    LEFT JOIN QISHOR_T_MSHLOCH_HZMNOT b
        ON a.MS_TEODT_RCSH = b.MS_T_MSHLOCH

		)

		--select * from Purchase_Exchange where PurchaseOrderID = 143773
		--select 
		--SupplierKey,
		--LineTotalNetUSD/total_qty as FOT
		--,Cif_price
		--from P_costs where PurchaseOrderID = 20005901

select 
	cast(s.DeliveryNote as varchar) as DeliveryNote,
	cast(bl.PurchaseOrderID as varchar) as PurchaseOrderID,
	cast(PC.SupplierKey as varchar) as SupplierKey,
	cast(PC.boat as varchar) as ShipID,
	PC.orderquantity as [Purchase Quantity],
	PC.ValueDate,
	s.LineType,
	s.DeliveryDate,
	cast(s.AccountKey as varchar) as AccountKey,
	cast(s.AgentKey as varchar) as AgentKey,
	cast(s.ItemKey as varchar) as ItemKey,
	s.SalesType,
	s.QuantityCategory,
	s.ActionTypeDesc,
	s.Quantity,
	s.LineTotalNet_USD,
	s.UnitNetPriceUSD,
	PC.Cif_price as CIF_Purchase,
	PC.[demurrage / Despatch],
	PC.DischargeCosts/
	nullif((PC.orderquantity - sum(case when s.SalesType = 'CIF' then s.Quantity else 0 end) over (partition by bl.PurchaseOrderID)),0) as DischargeCost,
	PC.Cif_price + PC.[demurrage / Despatch] + (PC.DischargeCosts/
	nullif((PC.orderquantity - sum(case when s.SalesType = 'CIF' then s.Quantity else 0 end) over (partition by bl.PurchaseOrderID)),0)) as FOT_Purchase,
	case
		when s.SalesType = 'CIF' and s.LineType = 'Item' then s.UnitNetPriceUSD - PC.Cif_price 
		when s.SalesType in ('FOT','FOT Premium')  and s.LineType = 'Item' then s.UnitNetPriceUSD - (PC.Cif_price + PC.[demurrage / Despatch] + (PC.DischargeCosts/nullif((PC.orderquantity - sum(case when s.SalesType = 'CIF' then s.Quantity else 0 end) over (partition by bl.PurchaseOrderID)),0)))
	else 0 
	end as 'Gain'
	,
	case
		when s.SalesType = 'CIF' and s.LineType = 'Item' then (s.UnitNetPriceUSD - PC.Cif_price)* s.Quantity
		when s.SalesType in ('FOT','FOT Premium') and s.LineType = 'Item' then (s.UnitNetPriceUSD - (PC.Cif_price + PC.[demurrage / Despatch] + (PC.DischargeCosts/nullif((PC.orderquantity - sum(case when s.SalesType = 'CIF' then s.Quantity else 0 end) over (partition by bl.PurchaseOrderID)),0))))*s.Quantity
	else 0 
	end as 'TotalGain'
from sales s
inner join base_link bl
on bl.DeliveryNote = s.DeliveryNote
left join P_costs PC 
on PC.PurchaseOrderID = bl.PurchaseOrderID
	--LEFT JOIN (TOt
 --   SELECT * 
 --   FROM tblPnlList 
 --   WHERE PNL_Type = 'OUT') p ON PC.[PNL Code] = p.PNL_ID

where PC.ValueDate is not null
--and bl.PurchaseOrderID = 143996
--and s.SalesType = 'FOT'
--and s.AccountKey = 146
--and s.LineType = 'Additional Expense'