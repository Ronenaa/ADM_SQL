
WITH STG_1 AS(
		 SELECT 
		ProductID AS 'ItemKey'
		,inv.DueDate AS 'Date'
		,NULL as 'Type'
		,NULL AS 'Status'
		,NULL AS 'WarehsKey'
		,SupplierID AS 'SupplierKey'
		--,null AS 'BranchKey'
		,NULL AS 'BranchKey' -- Only Retail with POS
		--,CONVERT(INT, CONVERT(VARCHAR, b.BRANCH)) AS 'BranchKey' -- Only Retail without POS
		,NULL as 'CostperUnit'
		,AvgPrice AS 'UnitPrice'
		,FotFlat AS 'FOTprice'
		,CFFlat AS 'CFPrice'
		,INV.TotalInventory 'Quantity' 
		,ROUND(CAST(PCOST.AvgPrice AS decimal),2) * INV.TotalInventory as 'StockValue'
		,ROUND(CAST(PCOST.FotFlat AS decimal),2) * INV.TotalInventory as 'FOTStockValue'
		,ROUND(CAST(PCOST.CFFlat AS decimal),2) * INV.TotalInventory as 'CFStockValue'
		,CASE WHEN PCOST.FotFlat IS NULL THEN LAG(PCOST.FotFlat) OVER (PARTITION BY ProductCode order by PCOST.DueDate)* INV.TotalInventory ELSE PCOST.FotFlat* INV.TotalInventory END AS 'SVal'
		,WeightedExpenses
		,GETDATE() AS RowInsertDatetime
		,Inv.Version
		,ROW_NUMBER() OVER (partition by inv.DueDate,inv.SupplierID,inv.ProductID ORDER BY inv.DueDate,inv.Version desc) AS 'MaxVersion' --row num 1 will be the latest
FROM  tblInventory Inv
/*INNER JOIN (
SELECT MAX(DueDate) AS MD
from tblInventory
)DD
ON INV.DueDate = DD.MD*/
LEFT JOIN tblProductsCost PCost
ON (INV.DueDate = PCost.DueDate
AND INV.ProductID = PCost.ProductCode
AND INV.Version = PCost.Version)
LEFT JOIN MOTSRIM M ON INV.ProductID=M.QOD_MOTSR
WHERE  1 = 1 
)

,STG_2 AS (
SELECT TM.QOD_PRIT AS 'ItemKey'
		,SUBSTRING(T_TNOEH,1,4) + '-' + SUBSTRING(T_TNOEH,5,2) + '-' + SUBSTRING(T_TNOEH,7,2) AS 'Date'
		,1144 AS 'SupplierKey'
		,SUM(TM.CMOT_LOGOS) AS 'Quantity' 
 --row num 1 will be the latest

FROM BT.[dbo].[TNOEOT_MLAI_CLLI] TM
WHERE 1=1
AND QOD_GORM <> 1
AND Cast(SUBSTRING(TM.T_TNOEH,1,4) as int) >=2018 and Cast(SUBSTRING(TM.T_TNOEH,1,4) as int) <= Year(Getdate())
AND SOG_TNOEH_CN_ITS_SP_HE LIKE N'%ë%'
GROUP BY TM.QOD_PRIT,TM.T_TNOEH

Union ALL

SELECT TM.QOD_PRIT AS 'ItemKey'
		,SUBSTRING(T_TNOEH,1,4) + '-' + SUBSTRING(T_TNOEH,5,2) + '-' + SUBSTRING(T_TNOEH,7,2) AS 'Date'
		,1144 AS 'SupplierKey'
		,SUM(TM.CMOT_LOGOS)*-1 AS 'Quantity' 
 --row num 1 will be the latest

FROM BT.[dbo].[TNOEOT_MLAI_CLLI] TM
WHERE 1=1
AND QOD_GORM <> 1
AND Cast(SUBSTRING(TM.T_TNOEH,1,4) as int) >=2018 and Cast(SUBSTRING(TM.T_TNOEH,1,4) as int) <= Year(Getdate())
AND SOG_TNOEH_CN_ITS_SP_HE LIKE N'%é%'
GROUP BY TM.QOD_PRIT,TM.T_TNOEH
)

,STG_3 AS (
SELECT TM.ItemKey AS 'ItemKey'
		,PC.DueDate AS 'Date'
		,NULL as 'Type'
		,NULL AS 'Status'
		,NULL AS 'WarehsKey'
		,1144 AS 'SupplierKey'
		--,null AS 'BranchKey'
		,NULL AS 'BranchKey' -- Only Retail with POS
		--,CONVERT(INT, CONVERT(VARCHAR, b.BRANCH)) AS 'BranchKey' -- Only Retail without POS
		,NULL as 'CostperUnit'
		,AvgPrice AS 'UnitPrice'
		,FotFlat AS 'FOTprice'
		,CFFlat AS 'CFPrice'
		,SUM(TM.Quantity) AS 'Quantity' 
		,ROUND(CAST(PC.AvgPrice AS decimal),2) * SUM(TM.Quantity) as 'StockValue'
		,ROUND(CAST(PC.FotFlat AS decimal),2) * SUM(TM.Quantity) as 'FOTStockValue'
		,ROUND(CAST(PC.CFFlat AS decimal),2) * SUM(TM.Quantity) as 'CFStockValue'
		,CASE WHEN PC.FotFlat IS NULL THEN LAG(PC.FotFlat) OVER (PARTITION BY TM.ItemKey order by PC.DueDate)* SUM(TM.Quantity) ELSE PC.FotFlat* SUM(TM.Quantity) END AS 'SVal'
		,PC.WeightedExpenses
		,GETDATE() AS RowInsertDatetime
		,MV.MAXVERSION as 'Version'
		,ROW_NUMBER() OVER (partition by PC.DueDate,TM.ItemKey ORDER BY PC.DueDate,MV.MAXVERSION desc) AS 'MaxVersion' --row num 1 will be the latest

FROM STG_2 TM
LEFT JOIN (SELECT DueDate,ProductCode,MAX([Version]) as MAXVERSION
			FROM tblProductsCost
			Group By DueDate,ProductCode) MV
		ON MV.DueDate=TM.Date
		AND MV.ProductCode = TM.ItemKey
LEFT JOIN tblProductsCost PC
	ON PC.Version =MV.MAXVERSION
	AND PC.DueDate = MV.DueDate
	AND PC.ProductCode = MV.ProductCode
WHERE 1=1
GROUP BY TM.ItemKey,PC.DueDate,PC.AvgPrice,PC.CFFlat,PC.FotFlat,PC.WeightedExpenses,MV.MAXVERSION
)


SELECT
'1' as 'EntityID'
--,'1' + Inv.ItemKey as 'ItemKey'
--,CONCAT('1', Inv.ItemKey) as 'ItemKey'
,cast(Inv.ItemKey as varchar) +'-'+ cast(Inv.ItemKey as varchar) as 'ItemKey'
,Inv.Date AS 'Date'
,NULL as 'Type'
,NULL as 'Status'
--,'1' + Inv.[WarehsKey] as 'WarehsKey'
--,CONCAT('1' , Inv.[WarehsKey]) as 'WarehsKey'
,cast( Inv.SupplierKey as varchar) as 'SupplierKey'
,cast( Inv.[WarehsKey] as varchar) as 'WarehsKey'
--,'1' + Inv.[BranchKey] as 'BranchKey'
--,Concat('1', Inv.[BranchKey]) as 'BranchKey'
,cast(Inv.[BranchKey] as varchar) as 'BranchKey'
,Inv.[CostperUnit]
,UnitPrice
,FOTprice
,CFPrice
,SUM(Quantity) as 'Quantity'
,SUM([StockValue]) as 'StockValue'
,SUM([FOTStockValue]) as 'FOTStockValue'
,SUM([CFStockValue]) as 'CFStockValue'
,WeightedExpenses
,Inv.[RowInsertDatetime]
FROM (SELECT * 
		FROM STG_1
	UNION ALL
	SELECT *
		FROM STG_3
		)Inv
WHERE MaxVersion=1
GROUP BY 
Inv.ItemKey
,Inv.Date
,Inv.TYPE
,Inv.Status
,Inv.WarehsKey
,SupplierKey
,Inv.BranchKey
,Inv.CostperUnit
,Inv.RowInsertDatetime
,WeightedExpenses
,UnitPrice
,FOTprice
,CFPrice
Order By SupplierKey,ItemKey,Date