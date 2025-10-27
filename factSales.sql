---factSales
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

with CurrencyConvertion as ( --Curenncy COnvertion Table--
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
 FROM ( SELECT *
 ,SUM(CASE WHEN sher=0 THEN 0 ELSE 1 END) OVER (ORDER BY tarikh ) AS value_partition
 ,SUM(CASE WHEN SHER_EURO=0 THEN 0 ELSE 1 END) OVER (ORDER BY tarikh ) AS value_partitionEuro
          FROM SHERI_MTBE) m
		  )
----------------חשבוניות--------------------
SELECT 
        '1' as 'EntityID'
        ,CH.MS_CHSHBONIT  AS 'DocID'
		,ROW_NUMBER () over (partition by CH.MS_CHSHBONIT order by CS.T_CHSHBONIT,TM.TARIKH_MSHLOCH) AS 'InvoiceLineNumber'
		,'Invoice' AS 'DocName'
		,Case
			WHEN CS.QOD_MOTSR = TM.QOD_MOTSR
				THEN 'Item'
			ELSE 'Additional Expense'
		END AS 'LineType'
		,Case
			WHEN CS.QOD_MOTSR = TM.QOD_MOTSR
				THEN 0
			ELSE 1
		END AS 'LineTypeCode'
		--,CONCAT('1' , CONVERT(INT, CONVERT(VARCHAR, c.CUST)))   AS 'AccountKey' -- customer from invoices in case you need just invoices 
		,CONVERT(VARCHAR,CS.QOD_LQOCH) 'AccountKey' -- customer from invoices in case you need just invoices 
		,SUBSTRING(CS.T_CHSHBONIT,1,4) + '-' + SUBSTRING(CS.T_CHSHBONIT,5,2) + '-' + SUBSTRING(CS.T_CHSHBONIT,7,2) AS 'Date' -- Invoice date  
		--,null AS 'Time'
		,CAST(CONVERT(INT, CONVERT(VARCHAR,AM.QOD))as varchar) 'AgentKey' -- Agent from Customer method
		--,'1' + CONVERT(INT, CONVERT(VARCHAR, ii.PART)) AS 'ItemKey'
		--,CONCAT('1' , CONVERT(INT, CONVERT(VARCHAR, ii.PART))  AS 'ItemKey'
		--,CASE WHEN CS.QOD_MOTSR<>0 THEN CAST('1' as varchar) + CAST(CONVERT(INT, CONVERT(VARCHAR,CS.QOD_MOTSR)) as varchar) ELSE CAST('1' as varchar) + CAST(CONVERT(INT, CONVERT(VARCHAR,CS.QOD_MOTSR)) as varchar) +'-'+SIM.ItemID END AS 'ItemKey'
		,CAST( CONVERT(VARCHAR,CS.QOD_MOTSR) as varchar) +'-' + CAST( CONVERT(VARCHAR,CS.QOD_MOTSR) as varchar) AS 'ItemKey'
		,CASE WHEN TM.QOD_MOTSR IS NOT NULL THEN CAST(CONVERT(VARCHAR,TM.QOD_MOTSR) as varchar) ELSE CAST( CONVERT(VARCHAR,CS.QOD_MOTSR) as varchar) END +'-' + CASE WHEN CS.QOD_MOTSR<>0 THEN CAST( CONVERT(VARCHAR,CS.QOD_MOTSR) as varchar) ELSE SIM.ItemID END AS 'ItemKey_backup'
		,CASE WHEN TM.QOD_MOTSR IS NOT NULL THEN CAST(CONVERT(VARCHAR,TM.QOD_MOTSR) as varchar) ELSE CAST( CONVERT(VARCHAR,CS.QOD_MOTSR) as varchar) END AS Item
		,CASE WHEN CS.QOD_MOTSR<>0 THEN CAST( CONVERT(VARCHAR,CS.QOD_MOTSR) as varchar) ELSE SIM.ItemID END AS AdditionalKey
		--,CAST('1' as varchar) + CAST(CONVERT(INT, CONVERT(VARCHAR, i.BRANCH)) as varchar) AS 'BranchKey'
		--,null as 'EmployeeKey'
		--,NULL AS 'BranchKey'
		--,null as 'חבר מועדון'
		--,(1 - (1 - ii.[T$PERCENT] / 100) * (1 - i.[T$PERCENT] / 100)) AS 'DiscountPercent' -- the DiscountPercent for the invoice and invoice line
		,NULL AS 'DiscountPercent'
		--,(1 - (1 - ii.[T$PERCENT] / 100) * (1 - i.[T$PERCENT] / 100)) * ii.TQUANT / 1000 * ROUND(ii.PRICE, 2) * ii.IEXCHANGE AS 'LineTotalDiscount' -- the discount from gross price to the net
		,NULL AS 'LineTotalDiscount'
		--,ROUND(ii.PRICE, 2) * ii.IEXCHANGE AS 'UnitGrossPrice'
		,NULL AS 'UnitGrossPrice'
		--,ROUND(ii.PRICE * (1 - ii.[T$PERCENT] / 100) * (1 - i.[T$PERCENT] / 100), 2) * ii.IEXCHANGE AS 'UnitNetPrice'
		,CAST(CS.MCHIR_ICH_LLA_ME_M/(case when SHER_LCHISHOB<>0 then SHER_LCHISHOB else 1 end) AS decimal (12,2)) AS 'UnitNetPriceUSD'
		,CAST(CS.MCHIR_ICH_LLA_ME_M as decimal(12,2)) 'UnitNetPriceNIS'
		--,ii.TQUANT / 1000 * (CASE WHEN i.DEBIT = 'C' THEN - 1 ELSE 1 END) AS 'Quantity'
		,CS.MSHQL_NTO AS 'Quantity'
		,CASE
			WHEN CS.QOD_MOTSR=TM.QOD_MOTSR
				THEN 0
			ELSE Cs.CMOT
		END as AdditionalQuantity
		--The cost can be tor.cost * Quant or just tor.cost
		--Option 1
		--,CASE WHEN tor.COSTFLAG NOT IN ('C', '\0') 
		--THEN tor.COST * (CASE WHEN (tor.QUANT) < 0.0 THEN - 1 ELSE 1 END)
		--ELSE p.COST * ((ii.QUANT / 1000) / p.COSTQUANT) * (CASE WHEN i.DEBIT = 'C' THEN - 1 ELSE 1 END) * (CASE WHEN ii.CREDITFLAG = 'Y' THEN 0 ELSE 1 END)
		--END AS 'LineTotalCost' --------Cost without Quant
		,NULL AS 'LineTotalCost'
		--Option 2
		--,Case when tor.COSTFLAG NOT IN ('C','\0') then tor.COST * (ii.TQUANT/1000) * (Case when (tor.QUANT) < 0.0 then -1 else 1 end)
		-- else 
		-- p.COST * ((ii.QUANT/1000) / p.COSTQUANT) * (CASE when i.DEBIT = 'C' then -1 else 1 end) *(case when ii.CREDITFLAG = 'Y' then 0 else 1 end) 
		-- end as 'Line Total Cost' ----Cost with Quant
		--choose one of the above , The defauls is uncomment :
		,CAST(CASE 
			WHEN CS.QOD_MOTSR<>0 
				THEN CAST((CS.MCHIR_ICH_LLA_ME_M * CS.MSHQL_NTO) AS decimal (18,2)) 
			ELSE CS.MCHIR_ICH_LLA_ME_M 
		END 
		+
		CASE
			WHEN CS.QOD_MOTSR=TM.QOD_MOTSR
				THEN 0
			--WHEN TM.QOD_MOTSR IS NULL
			--	THEN 0
			ELSE Cs.CMOT*CAST(CS.MCHIR_ICH_LLA_ME_M as decimal(12,2))
		END 
		AS decimal (18,2))	AS 'LineTotalNetNIS'

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

		,null AS 'LineTotalNetVAT'
		,null AS 'LineTotalNetVAT_USD'
		,CONVERT(INT, CONVERT(VARCHAR,SUBSTRING(CS.T_CHSHBONIT,1,4) + SUBSTRING(CS.T_CHSHBONIT,5,2))) AS YearMonth
		--,case when (i.DEBIT='C')  then round(ii.IVCOST  ,2)*-1 else round(ii.IVCOST  ,2) end  as 'LineTotalNet'  -- Without Exchange
		--,case when (i.DEBIT='C')  then round(ii.IVCOST  ,2)*-1*ii.IEXCHANGE else round(ii.IVCOST  ,2)*ii.IEXCHANGE end  as 'LineTotalNet' --allways exchange
		--,case when (i.DEBIT='C')  then round(ii.QPRICE  ,2)*-1*ii.IEXCHANGE else round(ii.QPRICE  ,2)*ii.IEXCHANGE end  as 'LineTotalNet' -- the invoice items price without the total invoice discount
		----
		,NULL as 'Status'
		--,NULL as 'ChargeFlag'  
		--,null as 'סטטוס ליקוט כותרת'
	   -- ,null  as 'סטטוס ליקוט שורה'
	--	,NULL as 'דגל סטורנו'
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
		,CASE
			WHEN W.Qod_Gorm is Null
				THEN 0
			ELSE 1
		END AS 'WarehouseFlag'
		,GETDATE() AS RowInsertDatetime
		,TM.ActionType							as 'ActionType'
		--,act.ActionType							as 'ActionTypeDesc'
		,CASE 
			WHEN TM.ActionType = 6 THEN G.AOPI_PEILOT
			ELSE act.ActionType
		END										as 'ActionTypeDesc'										
		,'0'									as 'AdjustmentFlag'
		,null									as  'TransactionType'
		,'Sales'								as  'QuantityCategory'
		
		--,SIM.ItemID AS 'SubItemID'
FROM CHSHBONIOT_COTROT CH
/*left join (Select CHSHBONIT, SUM(SCOM_MOTAM) as SCOM
			from QISHOR_QBLOT_CHSHBONIOT
			GROUP BY CHSHBONIT
) QCH
on CH.MS_CHSHBONIT = QCH.CHSHBONIT*/
--left join QISHOR_QBLOT_CHSHBONIOT QB 
--   on CH.MS_CHSHBONIT = QB.CHSHBONIT
--left join QBLOT_COTROT Q
--on QB.QBLH = Q.MS_QBLH
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

/*Inner Join (Select *,
Case
	WHEN SCOM_MOTAM = Amount and Amount = Lag(Amount) over (partition by CHSHBONIT,QBLH order by T_Preon)
		THEN 0
	WHEN Amount is null
		THEN SCOM_MOTAM
	ELSE Amount
END as 'Scom'
From Qblot_Chshbonit) QQC
	on CH.MS_CHSHBONIT = QQC.CHSHBONIT and QQC.QBLH = Q.MS_QBLH*/


WHERE 1=1  
--and QCH.CHSHBONIT is not null 
--and Round(CH.SH_C_LLA_ME_M + ME_M-isnull(QCH.SCOM,0),0) = 0
AND Cast(SUBSTRING(cs.T_CHSHBONIT,1,4) as int) >=2018 and Cast(SUBSTRING(cs.T_CHSHBONIT,1,4) as int) <= Year(Getdate())

--AND CH.MS_CHSHBONIT IN (64577,70531,66916)



			-------OPEN Orders----------------------------------------------------------------------------------
	
UNION ALL


 SELECT 
 '1' as 'EntityID'
,MS_TEODH as 'DocID'
,NULL as 'InvoiceLineNumber'
,'Delivery Note' as  'DocName'
,'Item' AS 'LineType'
,0 AS 'LineTypeCode'
,CONVERT(INT, CONVERT(VARCHAR, TM.QOD_MQBL)) AS 'AccountKey'
,SUBSTRING(TARIKH_MSHLOCH,1,4) + '-' + SUBSTRING(TARIKH_MSHLOCH,5,2) + '-' + SUBSTRING(TARIKH_MSHLOCH,7,2) as 'Date'
--,null AS 'Time'
,CONVERT(INT, CONVERT(VARCHAR, TM.AISH_MCIROT)) AS 'AgentKey'
,CONVERT(VARCHAR, TM.QOD_MOTSR)+'-'+CONVERT(VARCHAR, TM.QOD_MOTSR) AS 'ItemKey'
,CONVERT(VARCHAR, TM.QOD_MOTSR)+'-'+CONVERT(VARCHAR, TM.QOD_MOTSR) AS 'ItemKey_backup'
,TM.QOD_MOTSR AS Item
,TM.QOD_MOTSR AS AdditionalKey
--,concat('1',CONVERT(INT, CONVERT(VARCHAR, TM.QOD_MOTSR)))+'-'+concat('1',CONVERT(INT, CONVERT(VARCHAR, TM.QOD_MOTSR))) AS 'SubItemKey'
--,concat('1',CONVERT(INT, CONVERT(VARCHAR, o.BRANCH))) AS 'BranchKey'
--,null as 'EmployeeKey'
--,null as 'חבר מועדון'
--,(1-(1 - oi.[T$PERCENT] / 100)*(1 - o.[T$PERCENT] / 100))*100  as 'DiscountPercent'
,null AS 'DiscountPercent'
--,(round((oi.TQUANT/1000)* oi.PRICE,2)*oi.IEXCHANGE)-((oi.TQUANT/1000)*round(oi.PRICE*(1 - oi.[T$PERCENT] / 100)*(1 - o.[T$PERCENT] / 100) ,2)*oi.IEXCHANGE) as 'LineTotalDiscount' 
,NULL AS 'LineTotalDiscount' 
--,round(oi.PRICE ,2)*oi.IEXCHANGE as 'Unit Gross Price' 
,NULL AS 'UnitGrossPrice'
--,round(oi.PRICE*(1 - oi.[T$PERCENT] / 100)*(1 - o.[T$PERCENT] / 100) ,2)*oi.IEXCHANGE as 'Unit Net Price'
,CASE
	         WHEN TM.MTBE_SH = '$' THEN CAST(TM.MCHIR_ICH as decimal(12,2))
		   --THEN CAST(HZ.MCHIR_ICH AS decimal (12,4))
	         ELSE CAST(TM.MCHIR_ICH *(1/SM.NEW_SHER)  AS decimal (12,2))
         END AS 'UnitNetPriceUSD'
,CASE
	  WHEN TM.MTBE_SH = '$'
	  THEN CAST(TM.MCHIR_ICH*SM.NEW_SHER AS decimal (12,2))
	  ELSE CAST(TM.MCHIR_ICH AS decimal (12,2))
      END AS 'UnitNetPriceNIS'
,TM.MSHQL_NTO as 'Quantity'
,0 AS 'AdditionalQuantity'
/*,tor.COST * (
			CASE 
				WHEN doct.DEBIT = 'C'
					THEN - 1
				ELSE 1
				END
			) AS 'LineTotalCost' */
,NULL AS 'LineTotalCost' 
--,(oi.TQUANT/1000)*round(oi.PRICE*(1 - oi.[T$PERCENT] / 100)*(1 - o.[T$PERCENT] / 100) ,2)*oi.IEXCHANGE  as 'LineTotalNet'
,CASE
	         WHEN TM.MTBE_SH = '$'
		     THEN CAST((TM.MCHIR_ICH * SM.NEW_SHER) * TM.MSHQL_NTO AS decimal (18,2))
	         ELSE CAST( TM.MCHIR_ICH * TM.MSHQL_NTO AS decimal (18,2))
         END AS 'LineTotalNetNIS'
,CASE
	         WHEN TM.MTBE_SH = '$'
		     THEN CAST((TM.MCHIR_ICH)*TM.MSHQL_NTO/*TM.CMOT_SHSOPQH*/ AS decimal (18,2))
	         ELSE CAST(TM.MCHIR_ICH * TM.MSHQL_NTO/* TM.CMOT_SHSOPQH*/ * (1/SM.NEW_SHER) AS decimal (18,2))
         END AS 'LineTotalNet_USD'
,null AS 'LineTotalNetVAT'
,null AS 'LineTotalNetVAT_USD'
,CONVERT(INT, CONVERT(VARCHAR,SUBSTRING(T_ASPQH,1,4) + SUBSTRING(T_ASPQH,5,2))) AS YearMonth
,STTOS as 'Status'
--,null as 'ChargeFlag'  
--,null as 'סטטוס ליקוט כותרת'
--,null as 'סטטוס ליקוט שורה'
--,null  as 'דגל סטורנו'
,TM.MS_TEODH as 'DeliveryNote'
,CAST(SUBSTRING(TARIKH_MSHLOCH,1,4) + '-' + SUBSTRING(TARIKH_MSHLOCH,5,2) + '-' + SUBSTRING(TARIKH_MSHLOCH,7,2) as date) as 'DeliveryDate'
,HZ.MSPR_HZMNH as 'OrderID'
,TM.QOD_SHOLCH as 'SupplierWarehouse'
,CASE
	WHEN W.Qod_Gorm is Null
		THEN 0
	ELSE 1
END AS 'WarehouseFlag'
,GETDATE() AS RowInsertDatetime
,TM.ActionType							as 'ActionType'
--,act.ActionType							as 'ActionTypeDesc'
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
	WHEN TM.MCHIR_ICH = 0 AND W.QOD_GORM IS NOT NULL THEN G.AOPI_PEILOT
	WHEN TM.MCHIR_ICH = 0 AND W.QOD_GORM IS NULL	 THEN 'החלפה'
	ELSE NULL
END AS  'TransactionType'
,CASE
	WHEN TM.MCHIR_ICH <> 0 THEN 'Sales'
	WHEN TM.MCHIR_ICH = 0 and G.AOPI_PEILOT = 'פחת' THEN 'Shortage'
	WHEN TM.MCHIR_ICH = 0 and G.AOPI_PEILOT = 'אחסון' THEN 'Storage'
	WHEN TM.MCHIR_ICH = 0 and G.AOPI_PEILOT NOT IN ('פחת','אחסון') then 'Exchange'
END AS 'QuantityCategory'
--,NULL AS SubItemID


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
--AND TM.ActionType in (1,11,12)
--AND TM.MCHIR_ICH <> 0
AND TM.PurchaseOrderType = 0