Select
'1' as 'EntityID'
--,'1' + w.WARHS as 'WarehsKey'
--,CONCAT('1' , w.WARHS) as 'WarehsKey'
,G.QOD_GORM as 'WarehsKey'
---,w.TYPE as 'WarehouseType'
,G.SHM_GORM as 'WarehouseName'
,G.EntityType as 'WarehouseType'
,G.AOPI_PEILOT as 'WarehouseActivity'
,CASE
	WHEN QOD_GORM = 1144 ----לוגוס
		THEN 1
	WHEN QOD_GORM = 1411 ----גורן הכפר
		THEN 2
	WHEN QOD_GORM = 1220 ----מספנות ישראל
		THEN 3
	ELSE ROW_NUMBER () OVER (Order By G.Qod_Gorm) + 3
END AS 'WarehouseOrder'
,case
	when G.QOD_GORM = 1144 then 'South'
	else 'North'
	end as Area
,GETDATE() AS RowInsertDatetime
From  GORMIM G
WHERE EntityType = N'מקום אספקה'

