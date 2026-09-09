-- נדרש עבור ה-CTE sales שהועתק מ-FactGain.sql (מסנן את שורות המכירה לפי תאריך).
-- אותו ערך כמו ב-FactGain.sql — שינוי כאן חייב להשתקף גם שם.
DECLARE @CutoffDate DATE = '2024-01-01';

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
 -- and Purchase = 20005431
--order by Exchange_Order_Date desc, Exchange_order
)
-- =============================================================================
-- מכאן ומטה: חישוב כמות ה-CIF לכל הזמנה, לצורך המחלק של "עלות לטון".
--
-- הכלל: עלות פריקה (PNL 2270) ודמורג' (PNL 1201) מתחלקות
-- ב-(OrderQuantity - CIF_Qty) ולא ב-OrderQuantity, כי טונות שנמכרו CIF לא נפרקו.
--
-- ה-CTE הבאים (sales / base_link / CIF_Qty) הועתקו מילה-במילה מ-FactGain.sql,
-- כדי ש-CIF_Qty כאן יהיה זהה למחלק שמייצר שם את DischargeCost_Base.
-- כל שינוי בהיגיון שם חייב להשתקף כאן (ראה sql/facts/FactGain_scope.md).
-- =============================================================================
,sales as (
  SELECT 
	'Invoice' AS 'DocName'
		,Case
			WHEN CS.QOD_MOTSR = TM.QOD_MOTSR
				THEN 'Item'
			ELSE 'Additional Expense'
		END AS 'LineType'
		,CONVERT(VARCHAR,CS.QOD_LQOCH) 'AccountKey' -- customer from invoices in case you need just invoices 
		,SUBSTRING(CS.T_CHSHBONIT,1,4) + '-' + SUBSTRING(CS.T_CHSHBONIT,5,2) + '-' + SUBSTRING(CS.T_CHSHBONIT,7,2) AS 'Date' -- Invoice date
		,CAST( CONVERT(VARCHAR,CS.QOD_MOTSR) as varchar) +'-' + CAST( CONVERT(VARCHAR,CS.QOD_MOTSR) as varchar) AS 'ItemKey'
		,CAST(CONVERT(VARCHAR,M.MasterProductInPurchase) as varchar) +'-' + CAST(CONVERT(VARCHAR,M.MasterProductInPurchase) as varchar) AS [מוצר על]
		,CAST(ROUND(CS.MCHIR_ICH_LLA_ME_M / NULLIF(CASE WHEN SHER_LCHISHOB <> 0 THEN SHER_LCHISHOB ELSE 1 END, 0), 2) AS FLOAT) AS 'UnitNetPriceUSD'
		,cast(ROUND(CS.MSHQL_NTO,2)AS FLOAT)  AS 'Quantity'
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
		,CAST(ROUND(
			CASE
				WHEN CS.QOD_MOTSR <> 0
					THEN (CS.MCHIR_ICH_LLA_ME_M * CS.MSHQL_NTO) / NULLIF(CASE WHEN SHER_LCHISHOB <> 0 THEN SHER_LCHISHOB ELSE CC.new_sher END, 0)
				ELSE CS.MCHIR_ICH_LLA_ME_M / NULLIF(CASE WHEN SHER_LCHISHOB <> 0 THEN SHER_LCHISHOB ELSE CC.new_sher END, 0)
			END
			+
			CASE
				WHEN CS.QOD_MOTSR = TM.QOD_MOTSR THEN 0
				WHEN TM.QOD_MOTSR IS NULL         THEN 0
				ELSE Cs.CMOT * (CS.MCHIR_ICH_LLA_ME_M / NULLIF(CASE WHEN SHER_LCHISHOB <> 0 THEN SHER_LCHISHOB ELSE CC.new_sher END, 0))
			END
		, 2) AS FLOAT) AS 'LineTotalNet_USD' --Invoices are always in NIS
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
--New line to add product family
LEFT JOIN MOTSRIM M
	ON CS.QOD_MOTSR = M.QOD_MOTSR
	--end here
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
left join TBLT_PEOLOT_HZMNH_T_MSHLOCH act
	on TM.ActionType = act.MS_AOPTSIH
left join CurrencyConvertion CC 
	ON cs.[TARIKH_MSHLOCH] = cc.Tarikh
WHERE 1=1
AND CAST(SUBSTRING(cs.T_CHSHBONIT,1,4) + '-' + SUBSTRING(cs.T_CHSHBONIT,5,2) + '-' + SUBSTRING(cs.T_CHSHBONIT,7,2) AS DATE) >= @CutoffDate
AND TM.QOD_SHOLCH <> TM.QOD_MQBL
AND CS.QOD_MOTSR <> 96

-------OPEN Orders----------------------------------------------------------------------------------
UNION ALL

 SELECT 
	'Delivery Note' as  'DocName'
	,'Item' AS 'LineType'
	,CONVERT(INT, CONVERT(VARCHAR, TM.QOD_MQBL)) AS 'AccountKey'
	,SUBSTRING(TARIKH_MSHLOCH,1,4) + '-' + SUBSTRING(TARIKH_MSHLOCH,5,2) + '-' + SUBSTRING(TARIKH_MSHLOCH,7,2) as 'Date'
	,CONVERT(VARCHAR, TM.QOD_MOTSR)+'-'+CONVERT(VARCHAR, TM.QOD_MOTSR) AS 'ItemKey'
	,CAST(CONVERT(VARCHAR,M.MasterProductInPurchase) as varchar) +'-' + CAST(CONVERT(VARCHAR,M.MasterProductInPurchase) as varchar) AS [מוצר על]
	,CAST(ROUND(CASE
	         WHEN TM.MTBE_SH = '$' THEN TM.MCHIR_ICH
	         ELSE TM.MCHIR_ICH * (1 / NULLIF(SM.NEW_SHER, 0))
         END, 2) AS FLOAT) AS 'UnitNetPriceUSD'
	,cast(ROUND(TM.MSHQL_NTO,2)AS FLOAT) as 'Quantity'
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
	,CAST(ROUND(CASE
	         WHEN TM.MTBE_SH = '$'
		     THEN TM.MCHIR_ICH * TM.MSHQL_NTO /*TM.CMOT_SHSOPQH*/
	         ELSE TM.MCHIR_ICH * TM.MSHQL_NTO /*TM.CMOT_SHSOPQH*/ * (1 / NULLIF(SM.NEW_SHER, 0))
         END, 2) AS FLOAT) AS 'LineTotalNet_USD'
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
		THEN '1'
	ELSE '0'
	END AS 'AdjustmentFlag'
	,CASE
		WHEN TM.MCHIR_ICH <> 0 THEN 'Sales'
		WHEN TM.MCHIR_ICH = 0 and G.AOPI_PEILOT = 'פחת' THEN 'Shortage'
		WHEN TM.MCHIR_ICH = 0 and G.AOPI_PEILOT = 'אחסון' THEN 'Storage'
		WHEN TM.MCHIR_ICH = 0 and G.AOPI_PEILOT NOT IN ('פחת','אחסון') then 'Swap'
	END AS 'QuantityCategory'
FROM TEODOT_MSHLOCH TM
Left Join (SELECT distinct MS_T_MSHLOCH
			FROM CHSHBONIOT_SHOROT
		) CH
	on TM.MS_TEODH = CH.MS_T_MSHLOCH
LEFT JOIN MOTSRIM M
	ON TM.QOD_MOTSR = M.QOD_MOTSR
Left Join (SELECT MS_HZMNH,MS_T_MSHLOCH
			FROM QISHOR_T_MSHLOCH_HZMNOT
		) HZT
	on TM.MS_TEODH = HZT.MS_T_MSHLOCH
Left Join HZMNOT HZ
	on HZ.MSPR_HZMNH = HZT.MS_HZMNH
LEFT JOIN CurrencyConvertion SM  -- reuse top-level CTE instead of repeating SHERI_MTBE inline
	ON SM.TARIKH = HZ.T_HZMNH
Left Join GORMIM G
	on TM.QOD_MQBL = G.QOD_GORM
Left Join (
			Select *
			From GORMIM
			Where EntityType Like N'%מקום אספקה%') W
	On W.QOD_GORM = TM.QOD_SHOLCH
left join TBLT_PEOLOT_HZMNH_T_MSHLOCH act
	on TM.ActionType = act.MS_AOPTSIH
WHERE
CH.MS_T_MSHLOCH is null
AND CAST(SUBSTRING(TARIKH_MSHLOCH,1,4) + '-' + SUBSTRING(TARIKH_MSHLOCH,5,2) + '-' + SUBSTRING(TARIKH_MSHLOCH,7,2) AS DATE) >= @CutoffDate
AND STTOS in (0,1)
AND TM.PurchaseOrderType = 0
AND TM.QOD_MOTSR <> 96 
AND TM.QOD_SHOLCH <> TM.QOD_MQBL   -- exclude internal docs (same source and destination)
  )
,base_link as (
  SELECT distinct
        a.MS_TEODT_MCIRH AS DeliveryNote,
        b.MS_HZMNH AS PurchaseOrderID
    FROM QISHOR_RCSH_LMCIRH a
    LEFT JOIN QISHOR_T_MSHLOCH_HZMNOT b
        ON a.MS_TEODT_RCSH = b.MS_T_MSHLOCH

		)

,CIF_Qty AS (
    -- זהה ל-CTE CIF_Qty ב-FactGain.sql: סכימת qty_cif (ActionType = 11) לכל הזמנה.
    SELECT
        bl.PurchaseOrderID,
        SUM(s.qty_cif) AS CIF_Qty
    FROM sales s
    INNER JOIN base_link bl
        ON bl.DeliveryNote = s.DeliveryNote
    GROUP BY bl.PurchaseOrderID
)

,base_expenses AS (
--select * from final2
--where Exchange_order = 133902
-----------------------------------------------------------------------------------------------------------------------------------------------

SELECT
'1' as 'EntityID'
        ,CAST(HS.NOMRTOR AS VARCHAR) AS 'ExpenseOrderID'
		--,CAST(HZ.MSPR_HZMNH AS VARCHAR) AS 'OrderID'
		,CAST(HS.MS_MSMKH_QSHOR  AS VARCHAR) AS 'OrderID'--'PurchaseOrderID'
		--,CONCAT(CAST(POL.POL_OrderID AS VARCHAR(20)),CAST(POL.POL_LineID AS VARCHAR(10)))
		,CONCAT(
			CAST(CONVERT(BIGINT, POL.POL_OrderID) AS VARCHAR(20)),
			CAST(POL.POL_LineID AS VARCHAR(10))
		 )	as 'PurchaseOrderID'
		,HS.SHORH AS 'OrderLineNumber'
		,'Order Expenses' AS 'DocName'
		--,CAST(CONVERT(INT, CONVERT(VARCHAR,HS.QOD_GORM_MQBL))as varchar) AS 'AccountKey' -- customer from invoices in case you need just invoices 
		,CAST(CONVERT(INT, CONVERT(VARCHAR,HC.QOD_SPQ))as varchar) AS 'SupplierKey'
		,CAST(
		  MAX(
			CASE WHEN HS.QOD_SHROT = 0
				 THEN CONVERT(INT, CONVERT(VARCHAR, HC.QOD_SPQ))
			END
		  ) OVER (PARTITION BY HS.MS_MSMKH_QSHOR)
			AS VARCHAR)												AS 'OrderSupplierKey'
		,CAST(CONVERT(INT, CONVERT(VARCHAR,hz.QOD_MQBL))as varchar) AS 'SourceKey'
		--,NULL AS 'Transport Type'--M.SHM_MOBIL AS 'Transport Type'
		--,SUBSTRING(HZ.T_ASPQH,1,4) + '-' + SUBSTRING(HZ.T_ASPQH,5,2) + '-' + SUBSTRING(HZ.T_ASPQH,7,2) as 'Supply Date'
		--,SUBSTRING(HZ.T_HZMNH,1,4) + '-' + SUBSTRING(HZ.T_HZMNH,5,2) + '-' + SUBSTRING(HZ.T_HZMNH,7,2) as 'Order Date'
        --,SUBSTRING(HS.T_ERKH,1,4) + '-' + SUBSTRING(HS.T_ERKH,5,2) + '-' + SUBSTRING(HS.T_ERKH,7,2) as 'Value Date'
		--,SUBSTRING(HS.T_BITSOE,1,4) + '-' + SUBSTRING(HS.T_BITSOE,5,2) + '-' + SUBSTRING(HS.T_BITSOE,7,2) as 'Value Date'
		 -- grab the header-string once per order, convert to date, then expose on every row
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
		,SUBSTRING(HS.T_HQLDH,1,4) + '-' + SUBSTRING(HS.T_HQLDH,5,2) + '-' + SUBSTRING(HS.T_HQLDH,7,2) as 'EntryDate'
		,cast(CONVERT(INT, CONVERT(VARCHAR,HS.QOD_MOTSR)) as varchar)+ '-' + cast(CONVERT(INT, CONVERT(VARCHAR,HS.QOD_MOTSR)) as varchar) AS 'ItemKey_backup'
		,CASE
			--WHEN  HST.QOD_SHROT IS NULL THEN cast(CONVERT(INT, CONVERT(VARCHAR,HS.QOD_MOTSR)) as varchar)+ '-' + cast(CONVERT(INT, CONVERT(VARCHAR,HS.QOD_MOTSR)) as varchar) - לפני שינוי של העמסת עלויות
			WHEN  HST.QOD_SHROT IS NULL OR HS.QOD_SHROT IN (5, 19, 30, 4) THEN cast(CONVERT(INT, CONVERT(VARCHAR,HS.QOD_MOTSR)) as varchar)+ '-' + cast(CONVERT(INT, CONVERT(VARCHAR,HS.QOD_MOTSR)) as varchar) -- העמסת עליות הובלה והפרש מחיר על המוצר
			ELSE cast(CONVERT(INT, CONVERT(VARCHAR,HS.QOD_MOTSR)) as varchar)+ '-' + cast(CONVERT(INT, CONVERT(VARCHAR,HST.QOD_SHROT)) as varchar)+'S' 
			END		AS 'ItemKey'
		/*,CASE
	         WHEN HS.MTBE = '$' THEN HS.MCHIR_ICH
		   --THEN CAST(HZ.MCHIR_ICH AS decimal (12,4))
	         ELSE CAST(HS.MCHIR_ICH *(1/CASE WHEN HC.SHER_MTBE=0 THEN 1 ELSE HC.SHER_MTBE END)  AS decimal (12,4))
         END AS 'UnitNetPriceUSD'*/ 
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
		,HS.MTBE
		,HS.CMOT AS 'Quantity'
		--,POL.POL_FinalWeightReceived as tst
		--,HZ.CMOT_MOZMNT AS 'OrderQuantity1' -- מוריד לבדיקה
		,CASE WHEN HS.QOD_SHROT = 0 then HS.CMOT
				ELSE 0
				END									AS 'OrderQuantity'
		--,ISNULL(HST.QOD_SHROT,999) AS 'PNLKey'
		,CASE 
			WHEN HST.QOD_SHROT IN (5, 19, 30, 4,18) OR HST.QOD_SHROT IS NULL 
			THEN 999
			ELSE HST.QOD_SHROT
		END											AS 'PNLKey'  ----------  העמסת עליות הובלה על המוצר והפרש מחיר
		--,HS.QOD_SHROT AS 'PNLKey'
		,HS.CMOT AS 'Balance'
		,NULL AS 'LineTotalCost'
			,CASE
	         WHEN HS.MTBE = '$' THEN HS.MCHIR_ICH*HS.CMOT 
			 WHEN HS.MTBE = 'Eur' THEN HS.MCHIR_ICH*HS.CMOT * (SM.NEW_SHEREURO/SM.NEW_SHER)
	         ELSE HS.MCHIR_ICH*HS.CMOT*(1/CASE WHEN HC.SHER_MTBE=0 THEN 1 ELSE HC.SHER_MTBE END)
         END AS 'LineTotalNetUSD'
				/*CASE
	         WHEN HZ.MTBE = '$'
		     THEN CAST((HS.MCHIR_ICH)*(HS.CMOT - CMOT_SHSOPQH) AS decimal (12,4))
	         ELSE CAST(HS.MCHIR_ICH*(HS.CMOT_MOZMNT - CMOT_SHSOPQH)*(1/SM.NEW_SHER) AS decimal (12,4))
         END */
		,CASE
	         WHEN HS.MTBE = '$' THEN HS.MCHIR_ICH*HS.CMOT 
			 WHEN HS.MTBE = 'Eur' THEN HS.MCHIR_ICH*HS.CMOT * ( SM.NEW_SHEREURO/SM.NEW_SHER)
	         ELSE HS.MCHIR_ICH*HS.CMOT*(1/CASE WHEN HC.SHER_MTBE=0 THEN 1 ELSE HC.SHER_MTBE END) END AS 'LineTotalBalanceUSD'	
		--, (round(ii.IVCOST, 2)/ii.IEXCHANGE) *fnc.EXCHANGE2   *(CASE WHEN (i.DEBIT = 'C') THEN - 1 ELSE 1 END) AS 'LineTotalNet_USD'
		,CASE
	         WHEN HS.MTBE = '$' THEN HS.MCHIR_ICH*HC.SHER_MTBE 
			 WHEN HS.MTBE = 'Eur' THEN HS.MCHIR_ICH * SM.NEW_SHEREURO
	         ELSE HS.MCHIR_ICH 
         END AS 'UnitNetPriceNIS'
	    ,CASE
	         WHEN HS.MTBE = '$' THEN HS.MCHIR_ICH*HC.SHER_MTBE*HS.CMOT
			 WHEN HS.MTBE = 'Eur' THEN HS.MCHIR_ICH*HS.CMOT * SM.NEW_SHEREURO
	         ELSE HS.MCHIR_ICH*HS.CMOT 
         END AS 'LineTotalNetNIS'
		 ,/*CASE
	         WHEN HS.MTBE = '$'
		     THEN CAST((HS.MCHIR_ICH*SM.NEW_SHER)*(HZ.CMOT_MOZMNT - CMOT_SHSOPQH) AS decimal (12,4))
	         ELSE CAST(HS.MCHIR_ICH*(HZ.CMOT_MOZMNT - CMOT_SHSOPQH) AS decimal (12,4))
         END*/
		 CASE
	         WHEN HS.MTBE = '$' THEN HS.MCHIR_ICH*HC.SHER_MTBE*HS.CMOT 
			 WHEN HS.MTBE = 'Eur' THEN HS.MCHIR_ICH*HS.CMOT * SM.NEW_SHEREURO
	         ELSE HS.MCHIR_ICH*HS.CMOT 
         END  AS 'LineTotalBalanceNIS'
		--,null AS 'LineTotalNetVAT'
		--,null AS 'LineTotalNetVAT_USD'
		--,CONVERT(INT, LEFT(CONVERT(VARCHAR, CONVERT(DATETIME, (HZ.T_ASPQH + 46283040) / 1440.0), 112), 6)) AS YearMonth
		,CONVERT(INT, CONVERT(VARCHAR,SUBSTRING(HS.T_ERKH,1,4) + SUBSTRING(HS.T_ERKH,5,2))) AS YearMonth
		--,case when (i.DEBIT='C')  then round(ii.IVCOST  ,2)*-1 else round(ii.IVCOST  ,2) end  as 'LineTotalNet'  -- Without Exchange
		--,case when (i.DEBIT='C')  then round(ii.IVCOST  ,2)*-1*ii.IEXCHANGE else round(ii.IVCOST  ,2)*ii.IEXCHANGE end  as 'LineTotalNet' --allways exchange
		--,case when (i.DEBIT='C')  then round(ii.QPRICE  ,2)*-1*ii.IEXCHANGE else round(ii.QPRICE  ,2)*ii.IEXCHANGE end  as 'LineTotalNet' -- the invoice items price without the total invoice discount
		----
		,HC.STTOS AS 'סטטוס'
		,HC.HEROT AS 'Details'
		,HST.SHM_SHROT AS 'ServiceDetail' 
		,sl.ShipID
		,CAST(PO.PO_OrderCreateDate as date)			as OrderDate
		,CAST(PO.PO_UpdatedDeliveryDateFrom as date)	as SupplyDate
		,GETDATE() AS RowInsertDatetime
		,CASE
			WHEN POL.POL_OrderID IS NULL THEN 'Other'
			ELSE 'Import'
			END											as ExpenseSource
		,OP.ActionType AS 'TransactionType'
		,CASE 
		    WHEN HS.QOD_SHROT IN (5, 19, 30, 20, 4) THEN N'סחורה'
		    ELSE HC.SOG_MSMKH
		END												as DocType
		,SM.NEW_SHER
		--,HC.SHER_MTBE
		--,HS.MCHIR_ICH
		--,(SM.NEW_SHEREURO/SM.NEW_SHER) as tst
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
--AND HZ.ActionType IN(2,10)
--and HC.NOMRTOR = 10921
--and HS.MS_MSMKH_QSHOR = 20004792--20004161 
--and HS.NOMRTOR = 10677

UNION ALL


SELECT
     '1'                                                   AS EntityID,
	 NULL                             AS ExpenseOrderID,
	 CAST(CCS.HZMNT_RCSH AS VARCHAR)  AS OrderID,
	 CAST(CCS.HZMNT_RCSH AS VARCHAR)  AS PurchaseOrderID,
	 NULL                            AS OrderLineNumber,
     'Invoice'                                            AS DocName,
     NULL                          AS SupplierKey,
     NULL                          AS OrderSupplierKey,
     NULL                          AS SourceKey,
     CAST(SUBSTRING(CCS.T_CHSHBONIT,1,4) + '-' + SUBSTRING(CCS.T_CHSHBONIT,5,2) + '-' + SUBSTRING(CCS.T_CHSHBONIT,7,2) AS DATE) AS [Value Date],
     CAST(SUBSTRING(CCS.T_CHSHBONIT,1,4) + '-' + SUBSTRING(CCS.T_CHSHBONIT,5,2) + '-' + SUBSTRING(CCS.T_CHSHBONIT,7,2) AS DATE) AS EntryDate,
     CAST(CONVERT(VARCHAR, CCS.QOD_MOTSR) AS VARCHAR) + '-' + CAST(CONVERT(VARCHAR, CCS.QOD_MOTSR) AS VARCHAR) AS ItemKey_backup,
     CAST(CONVERT(VARCHAR, POL_ProductID) AS VARCHAR) + '-' + CAST(CONVERT(VARCHAR, M.ServiceCode) AS VARCHAR) + 'S' AS ItemKey,
     CAST(CCS.MCHIR_ICH_LLA_ME_M / (CASE WHEN SHER_LCHISHOB <> 0 THEN SHER_LCHISHOB ELSE CC2.new_sher END) AS DECIMAL(12,4)) * -1  AS UnitNetPriceUSD,
     NULL                            AS MTBE,
     CCS.MSHQL_NTO                   AS Quantity,
     CASE WHEN CCS.QOD_MOTSR = 0 THEN CCS.MSHQL_NTO ELSE 0 END AS OrderQuantity,
     HST.QOD_SHROT                           AS PNLKey,
     CCS.MSHQL_NTO                   AS Balance,
     NULL                            AS LineTotalCost,
     CAST(CCS.CMOT * (CCS.MCHIR_ICH_LLA_ME_M / (CASE WHEN SHER_LCHISHOB <> 0 THEN SHER_LCHISHOB ELSE CC2.new_sher END)) AS DECIMAL(12,4)) * -1 AS LineTotalNetUSD,
     CAST(CCS.CMOT * (CCS.MCHIR_ICH_LLA_ME_M / (CASE WHEN SHER_LCHISHOB <> 0 THEN SHER_LCHISHOB ELSE CC2.new_sher END)) AS DECIMAL(12,4)) * -1 AS LineTotalBalanceUSD,
     CAST(CCS.MCHIR_ICH_LLA_ME_M AS DECIMAL(12,4)) * -1  AS UnitNetPriceNIS,
     CAST(CCS.CMOT * CCS.MCHIR_ICH_LLA_ME_M AS DECIMAL(12,4)) * -1  AS LineTotalNetNIS,
     CAST(CCS.CMOT * CCS.MCHIR_ICH_LLA_ME_M AS DECIMAL(12,4)) * -1  AS LineTotalBalanceNIS,
     CONVERT(INT, CONVERT(VARCHAR, SUBSTRING(CCS.T_CHSHBONIT,1,4) + SUBSTRING(CCS.T_CHSHBONIT,5,2))) AS YearMonth,
     NULL                                    AS [סטטוס],
     NULL                                    AS Details,
     NULL                               AS ServiceDetail,
     NULL                                    AS ShipID,
     NULL                                   AS OrderDate,
     NULL                                  AS SupplyDate,
     GETDATE()                      AS RowInsertDatetime,
     'Invoice'                          AS ExpenseSource,
     NULL                             AS TransactionType,
    'Invoice'                                AS DocType,
	CASE WHEN SHER_LCHISHOB <> 0 THEN SHER_LCHISHOB ELSE CC2.new_sher END

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

UNION ALL

SELECT
    '1'                                                                 AS EntityID,
    NULL                                                                AS ExpenseOrderID,
    CAST(HZ.MSPR_HZMNH AS VARCHAR)                                      AS OrderID,
    HZ.MSPR_HZMNH                                                       AS PurchaseOrderID,
    NULL                                                                AS OrderLineNumber,
    'Orders'                                                            AS DocName,
    HZ.QOD_SHOLCH                                                                AS SupplierKey,
    NULL                                                                AS OrderSupplierKey,
	NULL																AS SourceKey,

    -- Supply date as value date
    CAST(SUBSTRING(HZ.T_ASPQH,1,4) + '-' + SUBSTRING(HZ.T_ASPQH,5,2) + '-' + SUBSTRING(HZ.T_ASPQH,7,2)
         AS DATE)                                                       AS [Value Date],

    -- Order date as entry date
    CAST(SUBSTRING(HZ.T_HZMNH,1,4) + '-' + SUBSTRING(HZ.T_HZMNH,5,2) + '-' + SUBSTRING(HZ.T_HZMNH,7,2)
         AS DATE)                                                       AS EntryDate,

    -- Item key (must match totals.ItemKey)
    CAST(CONVERT(INT, CONVERT(VARCHAR,HZ.MOTSR_MOZMN)) AS VARCHAR) + '-' +
    CAST(CONVERT(INT, CONVERT(VARCHAR,HZ.MOTSR_MOZMN)) AS VARCHAR)      AS ItemKey_backup,
    CAST(CONVERT(INT, CONVERT(VARCHAR,HZ.MOTSR_MOZMN)) AS VARCHAR) + '-' +
    CAST(CONVERT(INT, CONVERT(VARCHAR,HZ.MOTSR_MOZMN)) AS VARCHAR)      AS ItemKey,

    -- >>> Take unit price from totals (Avg Price USD (Expenses))
    t.UnitNetPriceUSD                                                   AS UnitNetPriceUSD,

    HZ.MTBE                                                             AS MTBE,

    -- Physical ordered quantity (for reference)
    HZ.CMOT_MOZMNT                                                      AS Quantity,

	--HZ.CMOT_MOZMNT														AS 'OrderQuantity',

     -->>> Order quantity used by DAX = from totals
    t.OrderQuantity                                                     AS OrderQuantity,

    999                                                                 AS PNLKey,
    (HZ.CMOT_MOZMNT - HZ.CMOT_SHSOPQH)                                  AS Balance,
    NULL                                                                AS LineTotalCost,

    -- >>> Total expenses in USD from totals (feeds Total Expenses USD in DAX)
    t.LineTotalNetUSD                                                  AS LineTotalNetUSD,

    -- Balance in USD can stay based on HZ (business choice)
    CASE
        WHEN HZ.MTBE = '$' THEN CAST(HZ.MCHIR_ICH * (HZ.CMOT_MOZMNT - HZ.CMOT_SHSOPQH) AS DECIMAL(12,4))
        ELSE CAST(HZ.MCHIR_ICH * (HZ.CMOT_MOZMNT - HZ.CMOT_SHSOPQH) * (1 / NULLIF(SM.NEW_SHER, 0))
                  AS DECIMAL(12,4))
    END                                                                 AS LineTotalBalanceUSD,

    -- NIS fields (unchanged)
    CASE
        WHEN HZ.MTBE = '$' THEN CAST(HZ.MCHIR_ICH * SM.NEW_SHER AS DECIMAL(12,4))
        ELSE CAST(HZ.MCHIR_ICH AS DECIMAL(12,4))
    END                                                                 AS UnitNetPriceNIS,
    CASE
        WHEN HZ.MTBE = '$' THEN CAST(HZ.MCHIR_ICH * SM.NEW_SHER * HZ.CMOT_MOZMNT AS DECIMAL(12,4))
        ELSE CAST(HZ.MCHIR_ICH * HZ.CMOT_MOZMNT AS DECIMAL(12,4))
    END                                                                 AS LineTotalNetNIS,
    CASE
        WHEN HZ.MTBE = '$' THEN CAST(HZ.MCHIR_ICH * SM.NEW_SHER * (HZ.CMOT_MOZMNT - HZ.CMOT_SHSOPQH)
                                     AS DECIMAL(12,4))
        ELSE CAST(HZ.MCHIR_ICH * (HZ.CMOT_MOZMNT - HZ.CMOT_SHSOPQH) AS DECIMAL(12,4))
    END                                                                 AS LineTotalBalanceNIS,

    CONVERT(INT, CONVERT(VARCHAR, SUBSTRING(HZ.T_ASPQH,1,4) + SUBSTRING(HZ.T_ASPQH,5,2))) AS YearMonth,
    HZ.OrderStatus                                                      AS [סטטוס],
    NULL                                                                AS Details,
    NULL                                                                AS ServiceDetail,
    NULL                                                                AS ShipID,

    CAST(SUBSTRING(HZ.T_HZMNH,1,4) + '-' + SUBSTRING(HZ.T_HZMNH,5,2) + '-' + SUBSTRING(HZ.T_HZMNH,7,2)
         AS DATE)                                                       AS OrderDate,
    CAST(SUBSTRING(HZ.T_ASPQH,1,4) + '-' + SUBSTRING(HZ.T_ASPQH,5,2) + '-' + SUBSTRING(HZ.T_ASPQH,7,2)
         AS DATE)                                                       AS SupplyDate,

    GETDATE()                                                           AS RowInsertDatetime,
    'Orders'                                                            AS ExpenseSource,
    OP.ActionType                                                       AS TransactionType,
    'Order'                                                             AS DocType,
    SM.NEW_SHER
FROM HZMNOT HZ
LEFT JOIN CurrencyConvertion SM
    ON SM.TARIKH = HZ.T_HZMNH

-- >>> Join to totals (same source as first query)
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
--and hz.MSPR_HZMNH = 20005361
)

,PO_OrderQty AS (
    -- כמות ההזמנה לכל PurchaseOrderID, מתוך שורות ההוצאה עצמן (OrderQuantity שונה
    -- מ-0 רק בשורת הכותרת). משמש לבניית NetTonnage בלבד.
    SELECT
        CAST(PurchaseOrderID AS VARCHAR(30)) AS PurchaseOrderID,
        SUM(OrderQuantity)                   AS OrderQuantity
    FROM base_expenses
    GROUP BY CAST(PurchaseOrderID AS VARCHAR(30))
)

-- ה-SELECT החיצוני: מוסיף את המחלק כעמודות. CIF_Qty ו-NetTonnage הם קבועים ברמת
-- ההזמנה וחוזרים זההים בכל שורות ההוצאה של אותה הזמנה — לכן ב-DAX יש לקרוא
-- אותם עם MAX ולא SUM (SUM היה מכפיל במספר השורות).
SELECT
    be.*,
    CAST(ISNULL(cq.CIF_Qty, 0) AS FLOAT)                                      AS CIF_Qty,
    -- טונאז' נטו: אם הכל נמכר CIF (או אין כמות) מוחזר NULL, כך שחלוקה
    -- ב-DAX תיתן BLANK ולא מספר מטעה.
    CAST(NULLIF(ISNULL(oq.OrderQuantity, 0) - ISNULL(cq.CIF_Qty, 0), 0) AS FLOAT) AS NetTonnage
FROM base_expenses be
LEFT JOIN CIF_Qty cq
    ON CAST(cq.PurchaseOrderID AS VARCHAR(30)) = CAST(be.PurchaseOrderID AS VARCHAR(30))
LEFT JOIN PO_OrderQty oq
    ON oq.PurchaseOrderID = CAST(be.PurchaseOrderID AS VARCHAR(30))
