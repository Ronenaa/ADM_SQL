 ----------------תעודות משלוח מלאי יוצא-----------------------------
 
 SELECT 
 '1' as 'EntityID'
,'A'+ CAST(MS_TEODH as nvarchar(100)) as 'DocID'
,ROW_NUMBER () Over (Partition By MS_TEODH Order By TARIKH_MSHLOCH)  as 'InvoiceLineNumber'
,'Out' as  'DocName'
,2 AS DocType
,CONVERT(INT, CONVERT(VARCHAR, TM.QOD_MQBL)) AS 'AccountKey'
,SUBSTRING(TARIKH_MSHLOCH,1,4) + '-' + SUBSTRING(TARIKH_MSHLOCH,5,2) + '-' + SUBSTRING(TARIKH_MSHLOCH,7,2) as 'Date'
--,null AS 'Time'
,CONVERT(INT, CONVERT(VARCHAR, TM.AISH_MCIROT)) AS 'AgentKey'
,CONVERT(VARCHAR, TM.QOD_MOTSR)+'-'+CONVERT(VARCHAR, TM.QOD_MOTSR) AS 'ItemKey'
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
,CONVERT(INT, CONVERT(VARCHAR,SUBSTRING(TARIKH_MSHLOCH,1,4) + SUBSTRING(TARIKH_MSHLOCH,5,2))) AS YearMonth
,STTOS as 'Status'
--,null as 'ChargeFlag'  
--,null as 'סטטוס ליקוט כותרת'
--,null as 'סטטוס ליקוט שורה'
--,null  as 'דגל סטורנו'
,TM.QOD_SHOLCH as 'InventoryWarehouse'
,GETDATE() AS RowInsertDatetime
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

WHERE 1=1
AND Cast(SUBSTRING(TARIKH_MSHLOCH,1,4) as int) >=2018 and Cast(SUBSTRING(TARIKH_MSHLOCH,1,4) as int) <= Year(Getdate())
AND STTOS in (0,1)
AND TM.ActionType in (1,11,12)

 ----------------תעודות משלוח מלאי נכנס-----------------------------
 Union ALL


 SELECT 
 '1' as 'EntityID'
,'A'+ CAST(MS_TEODH as nvarchar(100)) as 'DocID'
,ROW_NUMBER () Over (Partition By MS_TEODH Order By TARIKH_MSHLOCH)  as 'InvoiceLineNumber'
,'In' as  'DocName'
,1 AS DocType
,CONVERT(INT, CONVERT(VARCHAR, TM.QOD_SHOLCH)) AS 'AccountKey'
,SUBSTRING(TARIKH_MSHLOCH,1,4) + '-' + SUBSTRING(TARIKH_MSHLOCH,5,2) + '-' + SUBSTRING(TARIKH_MSHLOCH,7,2) as 'Date'
--,null AS 'Time'
,CONVERT(INT, CONVERT(VARCHAR, TM.AISH_MCIROT)) AS 'AgentKey'
,CONVERT(VARCHAR, TM.QOD_MOTSR)+'-'+CONVERT(VARCHAR, TM.QOD_MOTSR) AS 'ItemKey'
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
,CONVERT(INT, CONVERT(VARCHAR,SUBSTRING(TARIKH_MSHLOCH,1,4) + SUBSTRING(TARIKH_MSHLOCH,5,2))) AS YearMonth
,STTOS as 'Status'
--,null as 'ChargeFlag'  
--,null as 'סטטוס ליקוט כותרת'
--,null as 'סטטוס ליקוט שורה'
--,null  as 'דגל סטורנו'
,TM.QOD_MQBL as 'InventoryWarehouse'
,GETDATE() AS RowInsertDatetime
--,NULL AS SubItemID
FROM TEODOT_MSHLOCH TM
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
WHERE 1=1
AND Cast(SUBSTRING(TARIKH_MSHLOCH,1,4) as int) >=2018 and Cast(SUBSTRING(TARIKH_MSHLOCH,1,4) as int) <= Year(Getdate())
AND STTOS in (0,1)
AND TM.ActionType = 2

 ----------------תעודות משלוח החלפה מלאי נכנס-----------------------------
 
 Union ALL


 SELECT
 '1' as 'EntityID'
,'A'+ CAST(MS_TEODH as nvarchar(100)) as 'DocID'
,ROW_NUMBER () Over (Partition By MS_TEODH Order By TARIKH_MSHLOCH)  as 'InvoiceLineNumber'
,'Exchange - In' as  'DocName'
,3 AS DocType
,CONVERT(INT, CONVERT(VARCHAR, TM.QOD_SHOLCH)) AS 'AccountKey'
,SUBSTRING(TARIKH_MSHLOCH,1,4) + '-' + SUBSTRING(TARIKH_MSHLOCH,5,2) + '-' + SUBSTRING(TARIKH_MSHLOCH,7,2) as 'Date'
--,null AS 'Time'
,CONVERT(INT, CONVERT(VARCHAR, TM.AISH_MCIROT)) AS 'AgentKey'
,CONVERT(VARCHAR, TM.QOD_MOTSR)+'-'+CONVERT(VARCHAR, TM.QOD_MOTSR) AS 'ItemKey'
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
,CONVERT(INT, CONVERT(VARCHAR,SUBSTRING(TARIKH_MSHLOCH,1,4) + SUBSTRING(TARIKH_MSHLOCH,5,2))) AS YearMonth
,STTOS as 'Status'
--,null as 'ChargeFlag'  
--,null as 'סטטוס ליקוט כותרת'
--,null as 'סטטוס ליקוט שורה'
--,null  as 'דגל סטורנו'
,TM.QOD_MQBL as 'InventoryWarehouse'
,GETDATE() AS RowInsertDatetime
--,NULL AS SubItemID
FROM TEODOT_MSHLOCH TM
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
WHERE 1=1
AND Cast(SUBSTRING(TARIKH_MSHLOCH,1,4) as int) >=2018 and Cast(SUBSTRING(TARIKH_MSHLOCH,1,4) as int) <= Year(Getdate())
AND STTOS in (0,1)
AND TM.ActionType in (6,7)

 ----------------תעודות משלוח החלפה מלאי יוצא-----------------------------
 
 Union ALL


 SELECT
 '1' as 'EntityID'
,'A'+ CAST(MS_TEODH as nvarchar(100)) as 'DocID'
,ROW_NUMBER () Over (Partition By MS_TEODH Order By TARIKH_MSHLOCH)  as 'InvoiceLineNumber'
,'Exchange - Out' as  'DocName'
,4 AS DocType
,CONVERT(INT, CONVERT(VARCHAR, TM.QOD_MQBL)) AS 'AccountKey'
,SUBSTRING(TARIKH_MSHLOCH,1,4) + '-' + SUBSTRING(TARIKH_MSHLOCH,5,2) + '-' + SUBSTRING(TARIKH_MSHLOCH,7,2) as 'Date'
--,null AS 'Time'
,CONVERT(INT, CONVERT(VARCHAR, TM.AISH_MCIROT)) AS 'AgentKey'
,CONVERT(VARCHAR, TM.QOD_MOTSR)+'-'+CONVERT(VARCHAR, TM.QOD_MOTSR) AS 'ItemKey'
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
,CONVERT(INT, CONVERT(VARCHAR,SUBSTRING(TARIKH_MSHLOCH,1,4) + SUBSTRING(TARIKH_MSHLOCH,5,2))) AS YearMonth
,STTOS as 'Status'
--,null as 'ChargeFlag'  
--,null as 'סטטוס ליקוט כותרת'
--,null as 'סטטוס ליקוט שורה'
--,null  as 'דגל סטורנו'
,TM.QOD_SHOLCH as 'InventoryWarehouse'
,GETDATE() AS RowInsertDatetime
--,NULL AS SubItemID
FROM TEODOT_MSHLOCH TM
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
WHERE 1=1
AND Cast(SUBSTRING(TARIKH_MSHLOCH,1,4) as int) >=2018 and Cast(SUBSTRING(TARIKH_MSHLOCH,1,4) as int) <= Year(Getdate())
AND STTOS in (0,1)
AND TM.ActionType in (6,7)

------------------------------תנועות לוגוס עבור אחרים-------------------------------
UNION ALL 
 SELECT
 '1' as 'EntityID'
,'L' + CAST(NOMRTOR_TNOEH as nvarchar(100)) as 'DocID'
,ROW_NUMBER () Over (Partition By NOMRTOR_TNOEH Order By T_TNOEH)  as 'InvoiceLineNumber'
,CASE
	WHEN SOG_TNOEH_CN_ITS_SP_HE LIKE N'%י%'
		THEN 'Logos - Out' 
	ELSE 'Logos - In' 
END as  'DocName'
,CASE
	WHEN SOG_TNOEH_CN_ITS_SP_HE LIKE N'%י%'
		THEN 6 
	ELSE 5 
END AS DocType
,CONVERT(INT, CONVERT(VARCHAR, TM.QOD_GORM)) AS 'AccountKey'
,SUBSTRING(T_TNOEH,1,4) + '-' + SUBSTRING(T_TNOEH,5,2) + '-' + SUBSTRING(T_TNOEH,7,2) as 'Date'
--,null AS 'Time'
,NULL AS 'AgentKey'
,CONVERT(VARCHAR, TM.QOD_PRIT)+'-'+CONVERT(VARCHAR, TM.QOD_PRIT) AS 'ItemKey'
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
,NULL AS 'UnitNetPriceUSD'
,NULL AS 'UnitNetPriceNIS'
,TM.CMOT_LOGOS as 'Quantity'
/*,tor.COST * (
			CASE 
				WHEN doct.DEBIT = 'C'
					THEN - 1
				ELSE 1
				END
			) AS 'LineTotalCost' */
,NULL AS 'LineTotalCost' 
--,(oi.TQUANT/1000)*round(oi.PRICE*(1 - oi.[T$PERCENT] / 100)*(1 - o.[T$PERCENT] / 100) ,2)*oi.IEXCHANGE  as 'LineTotalNet'
,NULL AS 'LineTotalNetNIS'
,NULL AS 'LineTotalNet_USD'
,null AS 'LineTotalNetVAT'
,null AS 'LineTotalNetVAT_USD'
,CONVERT(INT, CONVERT(VARCHAR,SUBSTRING(T_TNOEH,1,4) + SUBSTRING(T_TNOEH,5,2))) AS YearMonth
,1 as 'Status'
--,null as 'ChargeFlag'  
--,null as 'סטטוס ליקוט כותרת'
--,null as 'סטטוס ליקוט שורה'
--,null  as 'דגל סטורנו'
,1144 as 'InventoryWarehouse'
,GETDATE() AS RowInsertDatetime
--,NULL AS SubItemID
FROM [BT].[dbo].[TNOEOT_MLAI_CLLI] TM

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
	on SM.TARIKH=TM.T_TNOEH
WHERE 1=1
AND QOD_GORM <> 1
AND Cast(SUBSTRING(T_TNOEH,1,4) as int) >=2018 and Cast(SUBSTRING(T_TNOEH,1,4) as int) <= Year(Getdate())
AND SOG_TNOEH_CN_ITS_SP_HE NOT LIKE N'%ה%'