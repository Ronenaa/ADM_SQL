Select
'1' as 'EntityID',
NOMRTOR_MSR AS 'DocID',
'Message' AS 'DocName',
1 AS 'DocType',
SUBSTRING(T_QLITH,1,4) + '-' + SUBSTRING(T_QLITH,5,2) + '-' + SUBSTRING(T_QLITH,7,2) AS 'Date',
SUBSTRING(TARIKH_OZMN_QLITH,5,4) + '-' + SUBSTRING(TARIKH_OZMN_QLITH,1,2) + '-' + SUBSTRING(TARIKH_OZMN_QLITH,3,2) as DueDate,
cast(CONVERT(INT, CONVERT(VARCHAR,QOD_CHOMR)) as varchar)+ '-' + cast(CONVERT(INT, CONVERT(VARCHAR,QOD_CHOMR)) as varchar) AS 'ItemKey'
,CASE
	WHEN SHOLCH_MSR like N'%ל%'
		THEN 1144
	WHEN SHOLCH_MSR like N'%מ%'
		THEN 1220
	ELSE NULL
END AS SupplierID,
MSHQL_NTO_SOS+MSHQL_NTO_EGLH AS 'Quantity'
From MSRIM M
  LEFT JOIN (SELECT *
			FROM [dbo].[TRGOM_ANGLIT]
			WHERE SOG_PRIT_SOCN_BNQ_TM_SH_CHBRH = N'פ') T
			ON M.QOD_CHOMR = T.QOD
WHERE SHOLCH_MSR in (N'ל',N'מ')
AND STTOS = 0
AND T.TARO_ANGLIT_2_ART = '8052'

UNION ALL

SELECT 
'1' as 'EntityID',
MSPR_HZMNH AS 'DocID',
'Allocation' AS 'DocName',
2 AS 'DocType',
SUBSTRING(T_HZMNH,1,4) + '-' + SUBSTRING(T_HZMNH,5,2) + '-' + SUBSTRING(T_HZMNH,7,2) as 'Date'
,SUBSTRING(T_ASPQH,1,4) + '-' + SUBSTRING(T_ASPQH,5,2) + '-' + SUBSTRING(T_ASPQH,7,2) as 'DueDate'
,cast(CONVERT(INT, CONVERT(VARCHAR,HZ.MOTSR_MOZMN)) as varchar)+ '-' + cast(CONVERT(INT, CONVERT(VARCHAR,HZ.MOTSR_MOZMN)) as varchar) AS 'ItemKey'
,HZ.QOD_SHOLCH AS 'SupplierID'
,HqR.Cmot AS 'Quantity'
from HZMNOT HZ
inner join (
			select HZMNT_MCIRH
			,sum(CMOT_MOQTST_MTSTBRT) as Cmot
			from HQTSAOT_RCSH_HCHLPH
			group by HZMNT_MCIRH
			Having sum(CMOT_MOQTST_MTSTBRT) <> 0
			
			Union All
			select HZMNH
			,sum(HQTSAH) as Cmot
			from HQTSAOT_MMLAI
			group by HZMNH
			Having sum(HQTSAH) <> 0
		
			) HqR
	on  HZ.mspr_Hzmnh = HqR.HZMNT_MCIRH

where OrderStatus in (1,2)
AND Cast(SUBSTRING(T_HZMNH,1,4) as int) >=2018 and Cast(SUBSTRING(T_HZMNH,1,4) as int) <= Year(Getdate())