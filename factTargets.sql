WITH DateList AS (
                  SELECT CAST(DATEFROMPARTS(YEAR(GETDATE())-1,1,1) AS DATE) AS [Date]
                  UNION ALL
                  SELECT  DATEADD(DAY, 1, [Date]) AS 'Date'
                  FROM DateList
                  WHERE [Date]<DATEFROMPARTS(YEAR(GETDATE()),12,31)
)
,STG1 AS (
          SELECT 
          '1' as 'EntityID'
		  ,CPT.CPT_Year AS 'Year'
          ,CAST(CONVERT(INT, CONVERT(VARCHAR,AM.QOD)) AS VARCHAR) AS 'AgentKey'
          ,CAST(CONVERT(INT, CONVERT(VARCHAR,G.QOD_GORM))AS VARCHAR) AS 'AccountKey'
          ,NULL AS 'CategoryCode' 
          ,CAST(CONVERT(INT, CONVERT(VARCHAR,CPT.CPT_ProductID)) AS VARCHAR)+ '-' + CAST(CONVERT(INT, CONVERT(VARCHAR,CPT.CPT_ProductID)) AS VARCHAR) AS 'ItemKey'
          --,DATENAME(MONTH,CONVERT(DATETIME,'1'+ (replace(Date,'PRICE','')) + '1')) AS 'Date'
          --,Year
          ,CPT.[CPT_PotentialInTone] AS 'PotentialInTone'
          ,CPT.[CPT_TargetInPercent] AS 'TargetInPercent'
          ,CPT.[CPT_PotentialInTone] * (CPT.[CPT_TargetInPercent]/100) as 'Target'
          ,NULL as 'TargetVAT'
          ,null as 'BranchKey'
         -- ,GETDATE() AS RowInsertDatetime
          FROM [tblCustomerProductYearTargets] CPT
          LEFT JOIN GORMIM G ON CPT.CPT_CustomerID=G.QOD_GORM
          LEFT JOIN TBLT_ANSHI_MCIROT AM ON AM.SHM_AISH_MCIROT = G.AISH_MCIROT_MTPL
          WHERE CPT.CPT_IsActive=1 

		  UNION ALL

		  SELECT 
          '1' as 'EntityID'
		  ,CPT.POT_Year AS 'Year'
          ,CAST(CONVERT(INT, CONVERT(VARCHAR,AM.QOD)) AS VARCHAR) AS 'AgentKey'
          ,CAST(CONVERT(INT, CONVERT(VARCHAR,G.QOD_GORM))AS VARCHAR) AS 'AccountKey'
          ,NULL AS 'CategoryCode' 
          ,CAST(CONVERT(INT, CONVERT(VARCHAR,CPT.POT_Product)) AS VARCHAR)+ '-' + CAST(CONVERT(INT, CONVERT(VARCHAR,CPT.POT_Product)) AS VARCHAR) AS 'ItemKey'
          --,DATENAME(MONTH,CONVERT(DATETIME,'1'+ (replace(Date,'PRICE','')) + '1')) AS 'Date'
          --,Year
          ,CPT.[POT_PotentialInTone] AS 'PotentialInTone'
          ,CPT.[POT_TargetInPercent] AS 'TargetInPercent'
          ,CPT.[POT_TargetInTone] as 'Target'
          ,NULL as 'TargetVAT'
          ,null as 'BranchKey'
         -- ,GETDATE() AS RowInsertDatetime
          FROM [dbo].[tblCustomerPotential] CPT
          LEFT JOIN GORMIM G ON CPT.POT_Customer=G.QOD_GORM
          LEFT JOIN TBLT_ANSHI_MCIROT AM ON AM.SHM_AISH_MCIROT = G.AISH_MCIROT_MTPL
		  WHERE POT_Year>=2024
)
,STG2 AS (
SELECT EntityID
      ,[Year]
	  --,DATEPART(WEEKDAY,[Date]) AS 'DayOfWeek'
	  --,CASE WHEN DATEPART(WEEKDAY,[Date]) IN (6,7) THEN 0 ELSE 1 END AS DayIndic
      ,[Date]
      ,AgentKey
	  ,AccountKey
	  ,CategoryCode
	  ,ItemKey
	  ,PotentialInTone
	  ,[Target]
	  ,TargetInPercent
	--,ROUND(CAST(((PotentialInTone) / 
    --       (CASE WHEN YEAR([Date]) = [Year] THEN DATEDIFF(DAY, DATEFROMPARTS([Year], 1, 1), DATEFROMPARTS([Year], 12, 31)) + 1 ELSE 365 END))as decimal (8,3)),3) AS 'PotentialInTone'
	--,ROUND(CAST(((PotentialInTone * (TargetInPercent / 100)) / 
    --       (CASE WHEN YEAR([Date]) = [Year] THEN DATEDIFF(DAY, DATEFROMPARTS([Year], 1, 1), DATEFROMPARTS([Year], 12, 31)) + 1 ELSE 365 END))as decimal (8,3)),3) AS 'Target'
	  ,TargetVAT
	  ,BranchKey
	  
FROM DateList DL
CROSS APPLY (
             SELECT *
             FROM STG1
             WHERE [Year] = YEAR(DL.[Date])
         ) AS STG
)
,NumOfDays AS (
SELECT *
	  ,SUM(VacationRate) OVER (PARTITION BY YEAR(VacationDate)) AS 'DaysOff'
	  ,DATEDIFF(DAY,DATEFROMPARTS(YEAR(VacationDate),01,01), DATEFROMPARTS(YEAR(VacationDate),12,31))+1 - SUM(VacationRate) OVER (PARTITION BY YEAR(VacationDate)) AS 'DaysInYear'
FROM ADM.dbo.tblVacationSettings

)
,TotalDaysInYear AS (
SELECT DISTINCT DL.*
,CASE WHEN N.VacationType IS NULL THEN 'יום עבודה' ELSE N.VacationType END AS 'VacationType' 
,CASE WHEN N.VacationRate IS NULL THEN 0 ELSE N.VacationRate END AS DayOff,N2.DaysInYear
FROM DateList DL
LEFT join NumOfDays N
ON DL.Date = N.VacationDate
LEFT JOIN NumOfDays N2 ON YEAR(DL.Date)=YEAR(N2.VacationDate)

)

SELECT EntityID
	  ,STG2.[Date]
	  ,AgentKey
	  ,AccountKey
	  ,CategoryCode
	  ,ItemKey
	  ,ROUND(CAST((PotentialInTone / (DaysInYear))as decimal (8,4)),4) AS 'PotentialInTone'
      ,ROUND(CAST(CASE WHEN DayOff>0 THEN 0 ELSE [TARGET] / (DaysInYear) END AS DECIMAL(8,5)),5) AS 'Target'
	  ,TargetVAT
      ,BranchKey
FROM STG2
LEFT JOIN TotalDaysInYear TD ON STG2.[Date]=TD.[Date]
order by Date
OPTION (MAXRECURSION 0);