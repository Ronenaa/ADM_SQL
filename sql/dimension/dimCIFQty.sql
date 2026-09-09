-- =============================================================================
-- dimCIFQty - CIF quantity per purchase order (grain: one row per PurchaseOrderID)
--
-- מקור האמת היחיד למחלק של "עלות לטון" עבור פריקה (PNL 2270)
-- ודמורג' (PNL 1201). הכלל: מתחלקים ב-(OrderQuantity - CIF_Qty) ולא
-- ב-OrderQuantity, כי טונות שנמכרו CIF לא נפרקו ולכן אינן נושאות עלות פריקה.
--
-- למה זה קיים: FactGain.sql מחשב את המחלק בתוכו (CTE CIF_Qty) ומייצר את
-- DischargeCost_Base. ל-factPurchaseExpenses אין את ה-CTE sales/base_link ולכן לא
-- יכל לחשב אותו. במקום זה ה-DAX חישב מחדש דרך [CIF Qty (Sales)], שסינן על
-- [Price Term] = "CIF" (עמודת COALESCE) במקום על ActionType = 11, וגם הושפע ממסנני
-- תאריך - ומכאן הפער בין 14.68 ל-10.20.
--
-- ה-CTE כאן (CurrencyConvertion / sales / base_link / CIF_Qty) הועתקו מילה-במילה
-- מ-FactGain.sql. כל שינוי בהיגיון שם חייב להשתקף כאן.
--
-- שימוש במודל:
--   1. לייבא כטבלה נפרדת (Import).
--   2. לקשר dimCIFQty[PurchaseOrderID] -> factPurchaseExpenses[PurchaseOrderID] (1:N).
--   3. ב-DAX: NetTonnage = [Order Qty] - CIF_Qty. ה-CIF_Qty הוא קבוע לרמת ההזמנה,
--      לכן יש לצרוף אותו עם SUMX על VALUES(PurchaseOrderID) — לא SUM ישר על
--      שורות הוצאה, שיכפיל אותו במספר השורות.
--
-- הערות:
--   * CIF_Qty הוא כמות לכל חיי ההזמנה, בלי חיתוך תאריכים — בדיוק כמו ב-FactGain.
--     זה הכרחי: המחלק חייב להיות זהה לזה שיוצר את DischargeCost_Base.
--   * מוחזרות רק הזמנות שבאמת נמכרו CIF (CIF_Qty > 0); לשאר ה-DAX יקבל BLANK
--     ויתייחס אליו כ-0, כך שהטבלה נשארת קטנה.
-- =============================================================================

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

SELECT
    CAST(PurchaseOrderID AS VARCHAR(30)) AS PurchaseOrderID,
    CAST(CIF_Qty AS FLOAT)               AS CIF_Qty
FROM CIF_Qty
WHERE PurchaseOrderID IS NOT NULL
  AND CIF_Qty > 0
