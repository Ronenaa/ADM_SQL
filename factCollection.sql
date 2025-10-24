with Qblot_Chshbonit as ( 
Select *,
CASE 
	WHEN QQC.SCOM_MOTAM <= QS.Total
		THEN QQC.SCOM_MOTAM
	ELSE QS.Total
END AS 'Amount'
from [dbo].[QISHOR_QBLOT_CHSHBONIOT] QQC
inner Join (Select MS_QBLH,T_Preon,Aopn_TSHLOM,SUM(Scom) as 'Total'
			from QBLOT_SHOROT
			where 1=1
			--AND AOPN_TSHLOM <> N'ש'
			AND Cast(SUBSTRING(T_PREON,1,4) as int) >=2018 and Cast(SUBSTRING(T_PREON,1,4) as int) <= Year(Getdate())
			AND CAST(SUBSTRING(T_PREON,1,4) + '-' + SUBSTRING(T_PREON,5,2) + '-' + SUBSTRING(T_PREON,7,2) as date) < Cast(GETDATE() as date)
			GROUP BY MS_QBLH,T_Preon,Aopn_TSHLOM
			) QS
	on QQC.QBLH=QS.MS_QBLH
) 

,FirstDeliveryforInvoice as (
Select distinct MS_CHSHBONIT,MS_T_MSHLOCH
			,SUBSTRING(TARIKH_MSHLOCH,1,4) + '-' + SUBSTRING(TARIKH_MSHLOCH,5,2) + '-' + SUBSTRING(TARIKH_MSHLOCH,7,2) as 'DeliveryDate'
			from CHSHBONIOT_SHOROT
			WHERE 1=1
			AND Cast(SUBSTRING(TARIKH_MSHLOCH,1,4) as int) >=2018 and Cast(SUBSTRING(TARIKH_MSHLOCH,1,4) as int) <= Year(Getdate())
)


,STG_Step1 as (

-----הזמנות פתוחות עם הקצאה ----- 

Select MSPR_HZMNH as 'TransactionID'
,1 as 'Line'
,1 as 'DocTypeCode' -- PK
,'Allocation' as 'DocType'-- Orders Before Shippment but after the item amount was allocated.
,QOD_MQBL as 'AccountKey'
,AISH_MCIROT as 'AgentKey'
,null as 'Invoice'
,SUBSTRING(T_HZMNH,1,4) + '-' + SUBSTRING(T_HZMNH,5,2) + '-' + SUBSTRING(T_HZMNH,7,2) as 'InvoiceDate'
,SUBSTRING(T_ASPQH,1,4) + '-' + SUBSTRING(T_ASPQH,5,2) + '-' + SUBSTRING(T_ASPQH,7,2) as 'ExpectedPayDate'
,NULL AS 'ActualPayedDate'
,NULL AS 'DeliveryDate'
,G.TNAI_TSHLOM_IMIM as 'TNAI_TSHLOM'
,CASE
	WHEN MTBE = '$'
		THEN (MCHIR_ICH*SM.NEW_SHER)*Hqr.Cmot
	ELSE MCHIR_ICH*HqR.Cmot
END AS 'InvoiceAmount'
,0.00 as 'PayedAmount'
,null as 'Receipt'
,HEROT_1 as 'Details'
,CASE
	WHEN MTBE = '$'
		THEN (MCHIR_ICH*SM.NEW_SHER)*Hqr.Cmot *1.18
	ELSE MCHIR_ICH*HqR.Cmot *1.18
END AS 'Debt'
,CASE
	WHEN MTBE = '$'
		THEN (MCHIR_ICH*SM.NEW_SHER)*Hqr.Cmot
	ELSE MCHIR_ICH*HqR.Cmot
END AS 'Obligo'

--,MCHIR_ICH
--,HqR.Cmot
--,MTBE
--,SM.NEW_SHER
from HZMNOT HZ
inner join (
			select HZMNT_MCIRH
			,sum(CMOT_MOQTST) as Cmot
			from HQTSAOT_RCSH_HCHLPH
			where 1=1
			and SOG_TM_SH_RCSH_HZMNT_HCHLPH <> ''
			--and HZMNT_MCIRH = 138471--139627
			group by HZMNT_MCIRH
			Having sum(CMOT_MOQTST) <> 0
			) HqR
	on  HZ.mspr_Hzmnh = HqR.HZMNT_MCIRH
Left join (
			Select distinct MS_HZMNH
			from QISHOR_T_MSHLOCH_HZMNOT
			) HzN
	on hz.MSPR_HZMNH = HzN.MS_HZMNH
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
	on SM.TARIKH= CONVERT(VARCHAR(8), GETDATE(), 112)--HZ.T_HZMNH --- שער המרה לפי תאריך של היום
Left Join GORMIM G
	on HZ.QOD_MQBL = G.QOD_GORM
where OrderStatus in (1,2)
--AND HzN.MS_HZMNH is null
AND Cast(SUBSTRING(T_HZMNH,1,4) as int) >=2018 and Cast(SUBSTRING(T_HZMNH,1,4) as int) <= Year(Getdate())
AND (Hqr.Cmot <> 0 or MCHIR_ICH <> 0)

--AND MSPR_HZMNH = 139627
--AND QOD_MQBL = 1041

------ תעודות משלוח פתוחות ללא חשבונית -------------

UNION ALL

Select MS_TEODH as 'TransactionID'
,1 as 'Line'
,2 as 'DocTypeCode' -- PK
,'Delivery Notes' as 'DocType'-- Orders Before Shippment but after the item amount was allocated.
,TM.QOD_MQBL as 'AccountKey'
,TM.AISH_MCIROT as 'AgentKey'
,null as 'Invoice'
,SUBSTRING(TARIKH_MSHLOCH,1,4) + '-' + SUBSTRING(TARIKH_MSHLOCH,5,2) + '-' + SUBSTRING(TARIKH_MSHLOCH,7,2) as 'InvoiceDate'
,SUBSTRING(TARIKH_MSHLOCH,1,4) + '-' + SUBSTRING(TARIKH_MSHLOCH,5,2) + '-' + SUBSTRING(TARIKH_MSHLOCH,7,2) as 'ExpectedPayDate'
,NULL AS 'ActualPayedDate'
,SUBSTRING(TARIKH_MSHLOCH,1,4) + '-' + SUBSTRING(TARIKH_MSHLOCH,5,2) + '-' + SUBSTRING(TARIKH_MSHLOCH,7,2) AS 'DeliveryDate'
,G.TNAI_TSHLOM_IMIM as 'TNAI_TSHLOM'
,CASE
	WHEN MTBE_SH = '$'
		THEN (TM.MCHIR_ICH*SM.new_sher)*TM.MSHQL_NTO
	ELSE TM.MCHIR_ICH*TM.MSHQL_NTO
END AS 'InvoiceAmount'
,0.00 as 'PayedAmount'
,null as 'Receipt'
,HEROT_1 as 'Details'
,CASE
	WHEN MTBE_SH = '$'
		THEN (TM.MCHIR_ICH*SM.new_sher)*TM.MSHQL_NTO * 1.18 -----תוספת מעמ
	ELSE TM.MCHIR_ICH*TM.MSHQL_NTO * 1.18
END AS 'Debt'
,CASE
	WHEN MTBE_SH = '$'
		THEN (TM.MCHIR_ICH*SM.new_sher)*TM.MSHQL_NTO
	ELSE TM.MCHIR_ICH*TM.MSHQL_NTO
END AS 'Obligo'


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
	on SM.TARIKH=CONVERT(VARCHAR(8), GETDATE(), 112)--HZ.T_HZMNH --- שער המרה לפי תאריך של היום
Left Join GORMIM G
	on TM.QOD_MQBL = G.QOD_GORM
WHERE CH.MS_T_MSHLOCH is null
AND Cast(SUBSTRING(TARIKH_MSHLOCH,1,4) as int) >=2018 and Cast(SUBSTRING(TARIKH_MSHLOCH,1,4) as int) <= Year(Getdate())
AND STTOS in (0,1)
AND TM.ActionType not in (2,6,7,9,10)
AND TM.MCHIR_ICH <> 0
AND TM.PurchaseOrderType = 0


--AND TM.QOD_MQBL = 1041


-------חשבוניות פתוחות ------
UNION ALL


Select CH.MS_CHSHBONIT as 'TransactionID'
,1 as 'Line'
,3 as 'DocTypeCode' -- PK
,'Open Invoices' as 'DocType'-- Orders Before Shippment but after the item amount was allocated.
,CH.QOD_LQOCH as 'AccountKey'
,AM.QOD as 'AgentKey'
,null as 'Invoice'
,SUBSTRING(TARIKH_HPQH,1,4) + '-' + SUBSTRING(TARIKH_HPQH,5,2) + '-' + SUBSTRING(TARIKH_HPQH,7,2) as 'InvoiceDate'
,SUBSTRING(PREON_TSHLOM,1,4) + '-' + SUBSTRING(PREON_TSHLOM,5,2) + '-' + SUBSTRING(PREON_TSHLOM,7,2) as 'ExpectedPayDate'
,NULL AS 'ActualPayedDate'
,FirstDeliveryDate AS 'DeliveryDate'
,G.TNAI_TSHLOM_IMIM as 'TNAI_TSHLOM'
,CH.SH_C_LLA_ME_M + ME_M AS 'InvoiceAmount'
,CASE
	WHEN Round(QCH.SCOM,0) = 0
		Then 0
	Else isnull(QCH.SCOM,0)
END AS 'PayedAmount'
,null as 'Receipt'
,null as 'Details'
,CH.SH_C_LLA_ME_M + ME_M-isnull(QCH.SCOM,0) AS 'Debt'
,CH.SH_C_LLA_ME_M + ME_M-isnull(QCH.SCOM,0) AS 'Obligo'

FROM CHSHBONIOT_COTROT CH
left join (Select CHSHBONIT, SUM(SCOM_MOTAM) as SCOM
			from QISHOR_QBLOT_CHSHBONIOT
			GROUP BY CHSHBONIT
			) QCH
	on CH.MS_CHSHBONIT = QCH.CHSHBONIT
left join (Select MS_CHSHBONIT, MIN(DeliveryDate) as FirstDeliveryDate
			from FirstDeliveryforInvoice
			Group By MS_CHSHBONIT
			) CHS
	on CHS.MS_CHSHBONIT = QCH.CHSHBONIT
Left Join GORMIM G
	on CH.QOD_LQOCH = G.QOD_GORM
Left Join TBLT_ANSHI_MCIROT AM 
	on AM.SHM_AISH_MCIROT = G.AISH_MCIROT_MTPL
WHERE 1=1  
--and QCH.CHSHBONIT is not null 
and Round(CH.SH_C_LLA_ME_M + ME_M-isnull(QCH.SCOM,0),0) <> 0
AND Cast(SUBSTRING(TARIKH_HPQH,1,4) as int) >=2018 and Cast(SUBSTRING(TARIKH_HPQH,1,4) as int) <= Year(Getdate())
AND CH.IsTemp <> 1


-------חשבוניות סגורות ------
UNION ALL

Select CH.MS_CHSHBONIT as 'TransactionID'
,ROW_NUMBER () over (partition by CH.MS_CHSHBONIT order by T_PREON)  as 'Line'
,4 as 'DocTypeCode' -- PK
,'Reciept' as 'DocType'-- Orders Before Shippment but after the item amount was allocated.
,CH.QOD_LQOCH as 'AccountKey'
,AM.QOD as 'AgentKey'
,CH.MS_CHSHBONIT as 'Invoice'
,SUBSTRING(ch.TARIKH_HPQH,1,4) + '-' + SUBSTRING(ch.TARIKH_HPQH,5,2) + '-' + SUBSTRING(ch.TARIKH_HPQH,7,2) as 'InvoiceDate'
,SUBSTRING(PREON_TSHLOM,1,4) + '-' + SUBSTRING(PREON_TSHLOM,5,2) + '-' + SUBSTRING(PREON_TSHLOM,7,2) as 'ExpectedPayDate'
,ISNULL(SUBSTRING(QQC.T_PREON,1,4) + '-' + SUBSTRING(QQC.T_PREON,5,2) + '-' + SUBSTRING(QQC.T_PREON,7,2),
		SUBSTRING(PREON_TSHLOM,1,4) + '-' + SUBSTRING(PREON_TSHLOM,5,2) + '-' + SUBSTRING(PREON_TSHLOM,7,2) )AS 'ActualPayedDate'
,FirstDeliveryDate AS 'DeliveryDate'
,G.TNAI_TSHLOM_IMIM as 'TNAI_TSHLOM'
,QQC.SCOM AS 'InvoiceAmount'
,QQC.SCOM AS 'PayedAmount'
,QB.QBLH as 'Receipt'

,CH.HEROT as 'Details'
,Round(CH.SH_C_LLA_ME_M + ME_M-isnull(QCH.SCOM,0),0) AS 'Debt'
,Round(CH.SH_C_LLA_ME_M + ME_M-isnull(QCH.SCOM,0),0) AS 'Obligo'

FROM CHSHBONIOT_COTROT CH
left join (Select CHSHBONIT, SUM(SCOM_MOTAM) as SCOM
			from QISHOR_QBLOT_CHSHBONIOT
			GROUP BY CHSHBONIT
) QCH
on CH.MS_CHSHBONIT = QCH.CHSHBONIT
left join QISHOR_QBLOT_CHSHBONIOT QB
on CH.MS_CHSHBONIT = QB.CHSHBONIT
left join QBLOT_COTROT Q
on QB.QBLH = Q.MS_QBLH
Left Join GORMIM G
	on CH.QOD_LQOCH = G.QOD_GORM
Left Join TBLT_ANSHI_MCIROT AM 
	on AM.SHM_AISH_MCIROT = G.AISH_MCIROT_MTPL
Inner Join (Select *,
Case
	WHEN SCOM_MOTAM = Amount and Amount = Lag(Amount) over (partition by CHSHBONIT,QBLH order by T_Preon)
		THEN 0
	WHEN Amount is null
		THEN SCOM_MOTAM
	ELSE Amount
END as 'Scom'
From Qblot_Chshbonit) QQC
	on CH.MS_CHSHBONIT = QQC.CHSHBONIT and QQC.QBLH = Q.MS_QBLH
left join (Select MS_CHSHBONIT, MIN(DeliveryDate) as FirstDeliveryDate
			from FirstDeliveryforInvoice
			Group By MS_CHSHBONIT
			) CHS
	on CHS.MS_CHSHBONIT = QCH.CHSHBONIT

WHERE 1=1  
--and QCH.CHSHBONIT is not null 
and Round(CH.SH_C_LLA_ME_M + ME_M-isnull(QCH.SCOM,0),0) = 0
AND Cast(SUBSTRING(ch.TARIKH_HPQH,1,4) as int) >=2018 and Cast(SUBSTRING(ch.TARIKH_HPQH,1,4) as int) <= Year(Getdate())
--and MS_CHSHBONIT in (52409)
--order by QB.QBLH
AND CH.IsTemp <> 1


--------צ'קים עם פרעון עתידי-----------
UNION ALL

Select QS.MS_QBLH as 'TransactionID'
,ROW_NUMBER () over (partition by QC.MS_QBLH order by T_PREON)  as 'Line'
,5 as 'DocTypeCode' -- PK
,'Cheques' as 'DocType'-- Orders Before Shippment but after the item amount was allocated.
,QS.QOD_LQOCH as 'AccountKey'
,AM.QOD as 'AgentKey'
,QS.MS_QBLH as 'Invoice'
,SUBSTRING(QC.TARIKH_HPQH,1,4) + '-' + SUBSTRING(QC.TARIKH_HPQH,5,2) + '-' + SUBSTRING(QC.TARIKH_HPQH,7,2) as 'InvoiceDate'
,SUBSTRING(QS.T_PREON,1,4) + '-' + SUBSTRING(QS.T_PREON,5,2) + '-' + SUBSTRING(QS.T_PREON,7,2) as 'ExpectedPayDate'
,SUBSTRING(QS.T_PREON,1,4) + '-' + SUBSTRING(QS.T_PREON,5,2) + '-' + SUBSTRING(QS.T_PREON,7,2) AS 'ActualPayedDate'
,NULL as 'DeliveryDate'
,G.TNAI_TSHLOM_IMIM as 'TNAI_TSHLOM'
,QS.SCOM AS 'InvoiceAmount'
,0.00 AS 'PayedAmount'
,QS.MS_QBLH as 'Receipt'
,QC.HEROT as 'Details'
,QS.SCOM AS 'Debt'
,QS.SCOM AS 'Obligo'

FROM QBLOT_SHOROT QS
Left Join QBLOT_COTROT QC
	ON QS.MS_QBLH =QC.MS_QBLH
Left Join GORMIM G
	on QS.QOD_LQOCH = G.QOD_GORM
Left Join TBLT_ANSHI_MCIROT AM 
	on AM.SHM_AISH_MCIROT = G.AISH_MCIROT_MTPL

WHERE 1=1 
AND AOPN_TSHLOM = N'ש'
AND Cast(SUBSTRING(T_PREON,1,4) as int) >=2018
AND CAST(SUBSTRING(T_PREON,1,4) + '-' + SUBSTRING(T_PREON,5,2) + '-' + SUBSTRING(T_PREON,7,2) as date) >= Cast(GETDATE() as date)

)


,STG_Step2 as (
select 
		   
		    [TransactionID]
           ,[Line]
           ,[DocTypeCode]
           ,[Invoice]
           ,CAST([InvoiceDate] as date) as 'InvoiceDate'
           ,CASE
				WHEN DocTypeCode = 1 or DocTypeCode = 2
					THEN DATEADD(DD,TNAI_TSHLOM,Cast(EOMONTH([ExpectedPayDate]) as date)) 
				ELSE CAST([ExpectedPayDate] as date)
			END as 'ExpectedPayDate'
           ,CAST([ActualPayedDate] as date) as 'ActualPayedDate'
		   ,CASE
				WHEN CAST([ActualPayedDate] as date) is null
					THEN ISNULL(CAST(DeliveryDate as Date),CAST([InvoiceDate] as date))
				WHEN CAST(DeliveryDate as Date)>=CAST([ActualPayedDate] as date)
					THEN CAST([ActualPayedDate] as date)
				ELSE ISNULL(CAST(DeliveryDate as Date),CAST([InvoiceDate] as date)) 
			END as 'DeliveryDate'
		   ,[AccountKey]
		   ,[AgentKey]
           ,[InvoiceAmount]
           ,[Receipt]
           ,[PayedAmount]
           ,[Details]
           ,[Debt]
           ,[Obligo]
           ,[DocType]


from STG_Step1

)

,STG_Step3 as (
Select

		    [TransactionID]
           ,[Line]
           ,[DocTypeCode]
           ,[Invoice]
           ,[InvoiceDate]
           ,[ExpectedPayDate]
           ,[ActualPayedDate]
		   ,[DeliveryDate]
		   ,[AccountKey]
		   ,[AgentKey]
           ,[InvoiceAmount]
           ,[Receipt]
           ,[PayedAmount]
           ,[Details]
           ,[Debt]
           ,[Obligo]
           ,[DocType]
           ,case 
				when [ActualPayedDate] is null 
					then DATEDIFF(DAY,[DeliveryDate],GETDATE()) 
				else  DATEDIFF(DAY,[DeliveryDate],[ActualPayedDate]) 
			end as [PayedDuration]
           ,case 
				when (DATEADD(DAY,0,[ExpectedPayDate])< cast(GETDATE() as date) and [ActualPayedDate] is null) or ( DATEADD(DAY,0,[ExpectedPayDate])<[ActualPayedDate] ) 
					then 1 
				else 0 
			end as [LateFlag]
           ,case 
				when (DATEADD(DAY,0,[ExpectedPayDate])< cast(GETDATE() as date) and [ActualPayedDate] is null) or ( DATEADD(DAY,0,[ExpectedPayDate])<[ActualPayedDate] ) 
					then case 
							when [ActualPayedDate] is null 
								then DATEDIFF(DAY,[ExpectedPayDate],cast(GETDATE() as date)) 
							else DATEDIFF(DAY,[ExpectedPayDate],[ActualPayedDate]) 
						end
				else 0 
		   end as [Delay]
		   from STG_Step2


)
,STG_Step4 as (
Select

             
		   [TransactionID]
		   ,CAST(CONVERT(INT, CONVERT(VARCHAR,[AccountKey]))as varchar) AS 'AccountKey'
		   ,cast(CONVERT(INT, CONVERT(VARCHAR,[AgentKey])) as varchar) AS 'AgentKey'
		   ,[InvoiceDate]
		   ,cast([InvoiceAmount] as decimal (18,2)) as 'InvoiceAmount'
		   ,[PayedAmount]
		   ,[Debt]
		   ,[Obligo]
		   ,[DocType]
		   ,[PayedDuration]
		   ,[LateFlag]
		   ,[Delay]
		   ,[Invoice] AS 'חשבונית'
		   ,[ExpectedPayDate] AS 'תאריך תשלום צפוי'
		   ,[ActualPayedDate] AS 'תאריך קבלה'
		   ,[DeliveryDate]
           ,[Receipt] AS 'קבלה'
		   ,[Details] AS 'פרטים נוספים'
		   ,[Line]
           ,[DocTypeCode]
           
           ,1 AS 'EntityID'
           
           
		  
           
           
		   ,case
		    when [Delay] >0 and [Delay] <=7 then '1-7'
		--	when [Delay] >7 and [Delay] <=14 then '7-14'
			when [Delay] >7 and [Delay] <=30 then '7-30'
			--when [Delay] >14 and [Delay] <=30 then '14-31'
			when [Delay] >30 and [Delay] <= 60 then '31-60'
			--when [Delay] >60 and [Delay] <=90 then '61-90'
			when [Delay] >60 and [Delay] <=120 then '61-120' 
			--when [Delay] >90 and [Delay] <=120 then '91-120' 
			when [Delay] <=0 then 'Current' 
			else
			'120+'
			end as 'ימי איחור'

			,case
		    when [Delay] >0 and [Delay] <=7 then 1
			when [Delay] >7 and [Delay] <=30 then 2
			--when [Delay] >14 and [Delay] <=30 then 3
			when [Delay] >30and [Delay] <=60then 3
			--when [Delay] >60 and [Delay] <=90 then 5 
			when [Delay] >60 and [Delay] <=120 then 4
			when [Delay] <=0 then 0
			else
			8
			end as 'ימי איחור סדר'

           ,case when [LateFlag]=1 then 'באיחור' else 'Not Overdue' end as 'Overdue'

           ,case 
				when  [PayedAmount]<>0 or [PayedAmount] is not null 
					then case
							when [InvoiceAmount]<>[PayedAmount] 
								then  [InvoiceAmount]-[PayedAmount]
							else [InvoiceAmount] 
						end
					else [InvoiceAmount]
		   end as  [PayedToDate]

           ,case 
				when  [PayedAmount]<>0 or [PayedAmount] is not null 
					then case
							when [InvoiceAmount]<>[PayedAmount] 
								then  ([InvoiceAmount]-[PayedAmount])*[PayedDuration]
							else [InvoiceAmount]*[PayedDuration] 
						end
					else [InvoiceAmount]*[PayedDuration]
		   end as  [PayedDurationXPayedToDate]

           ,GETDATE() as [RowInsertDatetime]
		   from STG_Step3
		   

)


select  *
from STG_Step4
where [TransactionID] <> 411032