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
)

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
			WHEN HST.QOD_SHROT IN (5, 19, 30, 4) OR HST.QOD_SHROT IS NULL 
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
		    WHEN HS.QOD_SHROT IN (5, 19, 30) THEN N'סחורה'
		    ELSE HC.SOG_MSMKH
		END												as DocType
		,HC.SHER_MTBE
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
--and HS.MS_MSMKH_QSHOR = 20004511--20004161 
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
    '1'                                                                 AS EntityID
  , NULL                                                                AS ExpenseOrderID
  , CAST(HZ.MSPR_HZMNH AS VARCHAR)                                       AS OrderID
  , HZ.MSPR_HZMNH                                                        AS PurchaseOrderID
  , NULL							                                     AS OrderLineNumber
  , 'Orders'                                                            AS DocName
  , NULL                                                                AS SupplierKey
  , NULL                                                                AS OrderSupplierKey
  , NULL                                                                AS SourceKey
  , CAST(SUBSTRING(HZ.T_ASPQH,1,4) + '-' + SUBSTRING(HZ.T_ASPQH,5,2) + '-' + SUBSTRING(HZ.T_ASPQH,7,2) AS DATE) AS [Value Date]  -- Supply date
  , CAST(SUBSTRING(HZ.T_HZMNH,1,4) + '-' + SUBSTRING(HZ.T_HZMNH,5,2) + '-' + SUBSTRING(HZ.T_HZMNH,7,2) AS DATE) AS EntryDate      -- same as order date
  , CAST(CONVERT(INT, CONVERT(VARCHAR,HZ.MOTSR_MOZMN)) AS VARCHAR) + '-' + CAST(CONVERT(INT, CONVERT(VARCHAR,HZ.MOTSR_MOZMN)) AS VARCHAR) AS ItemKey_backup
  , CAST(CONVERT(INT, CONVERT(VARCHAR,HZ.MOTSR_MOZMN)) AS VARCHAR) + '-' + CAST(CONVERT(INT, CONVERT(VARCHAR,HZ.MOTSR_MOZMN)) AS VARCHAR) AS ItemKey
  , CASE
        WHEN HZ.MTBE = '$' THEN HZ.MCHIR_ICH
        ELSE CAST(HZ.MCHIR_ICH * (1 / NULLIF(SM.NEW_SHER, 0)) AS DECIMAL(12,4))
    END                                                                 AS UnitNetPriceUSD
  , HZ.MTBE                                                             AS MTBE
  , HZ.CMOT_MOZMNT                                                      AS Quantity
  , HZ.CMOT_MOZMNT                                                      AS OrderQuantity
  , NULL                                                                AS PNLKey
  , (HZ.CMOT_MOZMNT - HZ.CMOT_SHSOPQH)                                  AS Balance
  , NULL                                                                AS LineTotalCost
  , CASE
        WHEN HZ.MTBE = '$' THEN CAST(HZ.MCHIR_ICH * HZ.CMOT_MOZMNT AS DECIMAL(12,4))
        ELSE CAST(HZ.MCHIR_ICH * HZ.CMOT_MOZMNT * (1 / NULLIF(SM.NEW_SHER, 0)) AS DECIMAL(12,4))
    END                                                                 AS LineTotalNetUSD
  , CASE
        WHEN HZ.MTBE = '$' THEN CAST(HZ.MCHIR_ICH * (HZ.CMOT_MOZMNT - HZ.CMOT_SHSOPQH) AS DECIMAL(12,4))
        ELSE CAST(HZ.MCHIR_ICH * (HZ.CMOT_MOZMNT - HZ.CMOT_SHSOPQH) * (1 / NULLIF(SM.NEW_SHER, 0)) AS DECIMAL(12,4))
    END                                                                 AS LineTotalBalanceUSD
  , CASE
        WHEN HZ.MTBE = '$' THEN CAST(HZ.MCHIR_ICH * SM.NEW_SHER AS DECIMAL(12,4))
        ELSE CAST(HZ.MCHIR_ICH AS DECIMAL(12,4))
    END                                                                 AS UnitNetPriceNIS
  , CASE
        WHEN HZ.MTBE = '$' THEN CAST(HZ.MCHIR_ICH * SM.NEW_SHER * HZ.CMOT_MOZMNT AS DECIMAL(12,4))
        ELSE CAST(HZ.MCHIR_ICH * HZ.CMOT_MOZMNT AS DECIMAL(12,4))
    END                                                                 AS LineTotalNetNIS
  , CASE
        WHEN HZ.MTBE = '$' THEN CAST(HZ.MCHIR_ICH * SM.NEW_SHER * (HZ.CMOT_MOZMNT - HZ.CMOT_SHSOPQH) AS DECIMAL(12,4))
        ELSE CAST(HZ.MCHIR_ICH * (HZ.CMOT_MOZMNT - HZ.CMOT_SHSOPQH) AS DECIMAL(12,4))
    END                                                                 AS LineTotalBalanceNIS
  , CONVERT(INT, CONVERT(VARCHAR, SUBSTRING(HZ.T_ASPQH,1,4) + SUBSTRING(HZ.T_ASPQH,5,2))) AS YearMonth
  , HZ.OrderStatus                                                      AS [סטטוס]
  , NULL                                                                AS Details
  , NULL                                                                AS ServiceDetail
  , NULL                                                                AS ShipID
  , CAST(SUBSTRING(HZ.T_HZMNH,1,4) + '-' + SUBSTRING(HZ.T_HZMNH,5,2) + '-' + SUBSTRING(HZ.T_HZMNH,7,2) AS DATE) AS OrderDate
  , CAST(SUBSTRING(HZ.T_ASPQH,1,4) + '-' + SUBSTRING(HZ.T_ASPQH,5,2) + '-' + SUBSTRING(HZ.T_ASPQH,7,2) AS DATE) AS SupplyDate
  , GETDATE()                                                           AS RowInsertDatetime
  , 'Orders'                                                            AS ExpenseSource
  , OP.ActionType                                                       AS TransactionType
  , 'Order'                                                             AS DocType
  ,SM.NEW_SHER
FROM HZMNOT HZ
LEFT JOIN (
    SELECT *
         , CASE WHEN sher = 0 THEN FIRST_VALUE(sher) OVER (PARTITION BY value_partition ORDER BY Tarikh) ELSE sher END AS new_sher
    FROM (
        SELECT *
             , SUM(CASE WHEN sher = 0 THEN 0 ELSE 1 END) OVER (ORDER BY tarikh) AS value_partition
        FROM SHERI_MTBE
    ) m
) SM
    ON SM.TARIKH = HZ.T_HZMNH
LEFT JOIN OrderPrices OP
    ON OP.OrderID = HZ.MSPR_HZMNH
WHERE CAST(SUBSTRING(HZ.T_HZMNH,1,4) AS INT) BETWEEN 2018 AND YEAR(GETDATE())
  AND HZ.OrderStatus <> 3
  AND HZ.ActionType IN (6,7)
--and hz.MSPR_HZMNH = 141662