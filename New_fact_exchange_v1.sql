with SM  as ( 
select *, 
Case
	when sher = 0 then first_value(sher) over (partition by value_partition order by Tarikh) 
	else sher end as new_sher
from (select *, sum(case when sher=0 then 0 else 1 end) over (order by tarikh ) as value_partition from SHERI_MTBE) m
)


select 
	b.MS_HZMNH as PurchaseOrderID
	--,TM.QOD_SHOLCH as DeliveredById
	,W.SHM_GORM as DeliveredBy
	,CONVERT(VARCHAR, TM.QOD_MOTSR)+'-'+CONVERT(VARCHAR, TM.QOD_MOTSR) AS 'ItemKey'
	,SUBSTRING(TARIKH_MSHLOCH,1,4) + '-' + SUBSTRING(TARIKH_MSHLOCH,5,2) as 'Year-Month'
	--,G.QOD_GORM as DeliveredToId
	,g.SHM_GORM as DeliveredTo
	,sum(TM.MSHQL_NTO) as QTY_Sold
	,'null' as FOT
	,'Sales'as 'Source'
	--
	--,CASE
	--         WHEN TM.MTBE_SH = '$' THEN CAST(TM.MCHIR_ICH as decimal(12,2))
	--         ELSE CAST(TM.MCHIR_ICH *(1/SM.NEW_SHER)  AS decimal (12,2))
 --        END AS 'UnitNetPriceUSD'

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
and  TM.MCHIR_ICH = 0 and G.AOPI_PEILOT  NOT IN ('פחת','אחסון') -- exchange
and b.MS_HZMNH like '20005111'
group by 
b.MS_HZMNH, 
TM.QOD_SHOLCH,
g.SHM_GORM,
w.SHM_GORM,
CONVERT(VARCHAR, TM.QOD_MOTSR)+'-'+CONVERT(VARCHAR, TM.QOD_MOTSR),
SUBSTRING(TARIKH_MSHLOCH,1,4) + '-' + SUBSTRING(TARIKH_MSHLOCH,5,2)
order by 'Year-Month' desc, PurchaseOrderID desc

--this is the purchases only, need to combine the price and the sales quantities

--valuedateתאריך לא תואם את ה 


