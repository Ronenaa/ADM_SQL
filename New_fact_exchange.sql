with SM  as ( 
select *, 
Case
	when sher = 0 then first_value(sher) over (partition by value_partition order by Tarikh) 
	else sher end as new_sher
from (select *, sum(case when sher=0 then 0 else 1 end) over (order by tarikh ) as value_partition from SHERI_MTBE) m
)


select 
	TM.QOD_SHOLCH as Supplier
	,W.SHM_GORM as 'Supplier Name'
	,b.MS_HZMNH as PurchaseOrderID
	,CONVERT(VARCHAR, TM.QOD_MOTSR)+'-'+CONVERT(VARCHAR, TM.QOD_MOTSR) AS 'ItemKey'
	,TM.MSHQL_NTO as qty_loaned
	,G.QOD_GORM as customer
	,g.SHM_GORM as Customer_Name
	,SUBSTRING(TARIKH_MSHLOCH,1,4) + '-' + SUBSTRING(TARIKH_MSHLOCH,5,2) + '-' + SUBSTRING(TARIKH_MSHLOCH,7,2) as 'Date'
	,CASE
	         WHEN TM.MTBE_SH = '$' THEN CAST(TM.MCHIR_ICH as decimal(12,2))
	         ELSE CAST(TM.MCHIR_ICH *(1/SM.NEW_SHER)  AS decimal (12,2))
         END AS 'UnitNetPriceUSD'

from TEODOT_MSHLOCH TM
left join GORMIM G on TM.QOD_MQBL = G.QOD_GORM --customer
left join GORMIM W on TM.QOD_SHOLCH = W.QOD_GORM --supplier
left join QISHOR_RCSH_LMCIRH a on MS_TEODT_MCIRH = TM.MS_TEODH
left join QISHOR_T_MSHLOCH_HZMNOT b on a.MS_TEODT_RCSH = b.MS_T_MSHLOCH
Left Join SM on SM.TARIKH=TM.TARIKH_MSHLOCH
--LEFT JOIN HOTSAOT_SHOROT HS ON HS.MS_MSMKH_QSHOR = b.MS_HZMNH
--LEFT JOIN PurchaseOrderLines POL ON CONCAT(
--										CAST(CONVERT(BIGINT, POL.POL_OrderID) AS VARCHAR(20)),
--										CAST(POL.POL_LineID AS VARCHAR(10))
--									)	
--										= CAST(b.MS_HZMNH AS VARCHAR(30))

where 1=1
--and TM.QOD_SHOLCH = 1280 -- השאלה ממילובר
and  TM.MCHIR_ICH = 0 and G.AOPI_PEILOT  NOT IN ('פחת','אחסון') -- exchange
--and b.MS_HZMNH = 141939
order by 'Date' desc, Supplier desc