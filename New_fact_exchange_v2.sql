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
Select 
	OrderId,
	MAX(CreateDate) AS LastCreation
From tblOrderPriceS
Group By OrderID
)

,LastVersion as (
Select 
	o.OrderId,
	Max(DayVersion) as LastVersion
From tblOrderPriceS o
Inner Join LastCreation LC
	ON o.OrderID=LC.OrderID AND o.CreateDate=LC.LastCreation
Group By o.OrderID
)

,OrderPrices as (
Select 
	O.*
from tblOrderPriceS o
Inner Join LastCreation LC
	ON o.OrderID=LC.OrderID And o.CreateDate=LC.LastCreation
Inner Join LastVersion LV
	ON o.OrderID=LV.OrderID AND o.DayVersion = LV.LastVersion
Where 1=1
)
, totals as (
select
	CONCAT(
			CAST(CONVERT(BIGINT, POL.POL_OrderID) AS VARCHAR(20)),
			CAST(POL.POL_LineID AS VARCHAR(10))
		 )	as 'PurchaseOrderID'

	,CASE
	         WHEN HS.MTBE = '$' THEN  HS.MCHIR_ICH*HS.CMOT 
			 WHEN HS.MTBE = 'Eur' THEN HS.MCHIR_ICH*HS.CMOT * (SM.NEW_SHEREURO/SM.NEW_SHER)
	         ELSE HS.MCHIR_ICH*HS.CMOT*(1/CASE WHEN HC.SHER_MTBE=0 THEN 1 ELSE HC.SHER_MTBE END)
         END AS 'LineTotalNetUSD'
	,CASE WHEN HS.QOD_SHROT = 0 then HS.CMOT
				ELSE 0
				END									AS 'OrderQuantity'
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
	,cast(CONVERT(INT, CONVERT(VARCHAR,HS.QOD_MOTSR)) as varchar)+ '-' + cast(CONVERT(INT, CONVERT(VARCHAR,HS.QOD_MOTSR)) as varchar) AS 'ItemKey'
	,hs.QOD_GORM_MQBL as DelivereTo
FROM HOTSAOT_COTROT HC
LEFT JOIN HOTSAOT_SHOROT HS ON HC.NOMRTOR = HS.NOMRTOR
LEFT JOIN PurchaseOrderLines POL ON CONCAT(CAST(CONVERT(BIGINT, POL.POL_OrderID) AS VARCHAR(20)),CAST(POL.POL_LineID AS VARCHAR(10)))= CAST(HS.MS_MSMKH_QSHOR AS VARCHAR(30))
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
and OrderType = 'Purchase'

)

select
	CONCAT(
				CAST(CONVERT(BIGINT,POL_OrderID) AS VARCHAR(20)),
				CAST(POL_LineID AS VARCHAR(10))
			 )	as 'PurchaseOrderID'
	,CustomerID as 'Delivered_By'
	,t.ItemKey
	,concat(year(t.[Value Date]),'-',month(t.[Value Date])) as 'Year-Month'
	,'null' as DeliveredTo
	,'null' as Quantity
	,sum(t.LineTotalNetUSD)/sum(t.OrderQuantity) 'FOT'
	,'Purchase' as 'Source'

FROM PurchaseOrderLines POL
LEFT JOIN OrderPrices OP ON op.OrderID = POL_OrderID AND op.OrderIDLine = POL.POL_LineID
LEFT JOIN totals t on t.PurchaseOrderID = CONCAT(CAST(CONVERT(BIGINT,POL_OrderID) AS VARCHAR(20)),CAST(POL_LineID AS VARCHAR(10)))

where 1=1
and OrderType = 'PURCHASE'
and CONCAT(CAST(CONVERT(BIGINT,POL_OrderID) AS VARCHAR(20)),CAST(POL_LineID AS VARCHAR(10))) = '20005111'
group by 
			CONCAT(
			CAST(CONVERT(BIGINT, POL.POL_OrderID) AS VARCHAR(20)),
			CAST(POL.POL_LineID AS VARCHAR(10)))
			,concat(year(t.[Value Date]),'-',month(t.[Value Date]))
			,CustomerID
			,t.ItemKey


