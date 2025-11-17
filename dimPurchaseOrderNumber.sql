with Purchase_order as (
select distinct
		HS.MS_MSMKH_QSHOR,
CASE 
			WHEN HS.MS_MSMKH_QSHOR = 0 
			  THEN CONVERT(date, HC.T_ERKH, 112)
			ELSE
			  MAX(
				CASE WHEN HS.QOD_SHROT = 0 
					 THEN CONVERT(date, HC.T_ERKH, 112) 
				END
					) OVER (PARTITION BY   CASE 
					WHEN HS.MS_MSMKH_QSHOR = 0  --?--
				  THEN HZ.MSPR_HZMNH 
					ELSE HS.MS_MSMKH_QSHOR 
				  END) END AS 'Value Date',
CASE 
			WHEN HS.MS_MSMKH_QSHOR = 0 
			  THEN format(CONVERT(date, HC.T_ERKH, 112),'yyyyMM')
			ELSE
			  MAX(
				CASE WHEN HS.QOD_SHROT = 0 
					 THEN format(CONVERT(date, HC.T_ERKH, 112),'yyyyMM')
				END
					) OVER (PARTITION BY   CASE 
					WHEN HS.MS_MSMKH_QSHOR = 0  --?--
				  THEN HZ.MSPR_HZMNH 
					ELSE HS.MS_MSMKH_QSHOR 
				  END) END AS [Year Month]

FROM HOTSAOT_COTROT HC
LEFT JOIN HOTSAOT_SHOROT HS ON HC.NOMRTOR = HS.NOMRTOR
LEFT JOIN PurchaseOrderLines POL ON CONCAT(
										CAST(CONVERT(BIGINT, POL.POL_OrderID) AS VARCHAR(20)),
										CAST(POL.POL_LineID AS VARCHAR(10))
									)	
										= CAST(HS.MS_MSMKH_QSHOR AS VARCHAR(30))
LEFT JOIN HZMNOT HZ ON (HS.MS_MSMKH_QSHOR = HZ.MSPR_HZMNH)
--where HZ.MSPR_HZMNH = 12345

),

ExchangeOrders as (
SELECT 
	 HZ.MSPR_HZMNH																									AS OrderID
	,CAST(SUBSTRING(HZ.T_ASPQH,1,4) + '-' + SUBSTRING(HZ.T_ASPQH,5,2) + '-' + SUBSTRING(HZ.T_ASPQH,7,2) AS DATE)	AS [Value Date]
	,FORMAT(CAST(SUBSTRING(HZ.T_ASPQH,1,4) + '-' + SUBSTRING(HZ.T_ASPQH,5,2) + '-' + SUBSTRING(HZ.T_ASPQH,7,2) AS DATE), 'yyyyMM') AS [Year Month]
FROM HZMNOT HZ
)
select 
		a. MS_TEODT_MCIRH			as DeliveryNote, 
		a.MS_TEODT_RCSH				, 
		b.MS_HZMNH					as PurchaseOrderID
		,ISNULL(sl.ShipDesc, '-')	as ShipDesc
		,ISNULL(p.[Value Date],o.[Value Date])  as [Value Date]
		,format(ISNULL(p.[Value Date],o.[Value Date]), 'MMM yy')		as 'ValueDateMonth'
		,dense_rank () over (order by case when p.[Year Month] is null then o.[Year Month] else p.[Year Month] end desc ) as Sort
from QISHOR_RCSH_LMCIRH a
left join QISHOR_T_MSHLOCH_HZMNOT b
	on a.MS_TEODT_RCSH = b.MS_T_MSHLOCH
left join PurchaseOrderLines POL
	ON b.MS_HZMNH = POL.POL_SQL_POID
LEFT JOIN ShipsArrivals sa ON POL.POL_ShipArrivalID = sa.SA_ID
LEFT JOIN ShipList sl ON sa.SA_ShipID = sl.ShipID

Left join Purchase_order p on b.MS_HZMNH = p.MS_MSMKH_QSHOR
LEFT JOIN ExchangeOrders o ON b.MS_HZMNH = o.OrderID