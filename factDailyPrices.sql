WITH DateList AS (
  --SELECT CAST(DATEADD(MONTH,DATEDIFF(month,0,DATEADD(MM,+1,MIN(AsOfDate))),0) AS date) AS 'Date'
Select Cast(dateadd(dd,[index]-1,'2023-09-01') as date) as date
FROM (select ROW_NUMBER () over (order by name) as 'index' from sys.all_columns) O
Where Cast(dateadd(dd,[index]-1,'2023-09-01') as date) <= (EOMONTH(DATEADD(MONTH,DATEDIFF(month,0,DATEADD(MM,+12,GETDATE())),0)) )


  
)

,STG1 AS (
SELECT
    ProductID,
	AsOfDate,
    Months,
	MONTH(DATEADD(MM, case when RIGHT(MONTHS,2) IN ('10','11','12') THEN CAST(RIGHT(MONTHS,2) AS INT)-1 ELSE CAST(RIGHT(MONTHS,1) AS INT)-1 END, ASOFDATE)) AS MonthNum,
	YEAR(DATEADD(MM, case when RIGHT(MONTHS,2) IN ('10','11','12') THEN CAST(RIGHT(MONTHS,2) AS INT)-1 ELSE CAST(RIGHT(MONTHS,1) AS INT)-1 END, ASOFDATE)) AS YearNum,
	YEAR(DATEADD(MM, case when RIGHT(MONTHS,2) IN ('10','11','12') THEN CAST(RIGHT(MONTHS,2) AS INT)-1 ELSE CAST(RIGHT(MONTHS,1) AS INT)-1 END, ASOFDATE)) *100 + MONTH(DATEADD(MM, case when RIGHT(MONTHS,2) IN ('10','11','12') THEN CAST(RIGHT(MONTHS,2) AS INT)-1 ELSE CAST(RIGHT(MONTHS,1) AS INT)-1 END, ASOFDATE)) as YearMonthNum,
	LEFT(Months,3)AS 'MinMaxIndicator',
	Price,
	Case When Price = 0 Then 0 Else 1 End as Flag
FROM (
    SELECT
        ProductID,
		AsOfDate,
        MinPriceMonth1,
        MaxPriceMonth1,
        MinPriceMonth2,
        MaxPriceMonth2,
		MinPriceMonth3,
        MaxPriceMonth3,
		MinPriceMonth4,
        MaxPriceMonth4,
		MinPriceMonth5,
        MaxPriceMonth5,
		MinPriceMonth6,
        MaxPriceMonth6,
		MinPriceMonth7,
        MaxPriceMonth7,
		MinPriceMonth8,
        MaxPriceMonth8,
		MinPriceMonth9,
        MaxPriceMonth9,
		MinPriceMonth10,
        MaxPriceMonth10,
		MinPriceMonth11,
        MaxPriceMonth11,
		MinPriceMonth12,
        MaxPriceMonth12

    FROM [tblDailyPrices]
) AS SourceTable
UNPIVOT (
    Price FOR Months IN (
        MinPriceMonth1, MaxPriceMonth1,
        MinPriceMonth2, MaxPriceMonth2,
		MinPriceMonth3, MaxPriceMonth3,
		MinPriceMonth4, MaxPriceMonth4,
		MinPriceMonth5, MaxPriceMonth5,
		MinPriceMonth6, MaxPriceMonth6,
		MinPriceMonth7, MaxPriceMonth7,
		MinPriceMonth8, MaxPriceMonth8,
		MinPriceMonth9, MaxPriceMonth9,
		MinPriceMonth10, MaxPriceMonth10,
		MinPriceMonth11, MaxPriceMonth11,
		MinPriceMonth12, MaxPriceMonth12
    )
) AS UnpivotedTable
)

,STG2 AS (
Select ProductID,
AsOfDate,
Months,
MonthNum,
YearNum,
YearMonthNum,
MinMaxIndicator,
Price,
Sum (Flag) over (partition by ProductID,MinMaxIndicator,YearMonthNum Order by AsOfDate) as Ranking
from STG1
)

,STG3 AS (
Select ProductID,
AsOfDate,
Months,
MonthNum,
YearNum,
YearMonthNum,
MinMaxIndicator,
Max (Price) over (partition by ProductID,MinMaxIndicator,YearMonthNum,Ranking Order by AsOfDate) AS Price
from STG2
)

,MAXPRICE as (
Select ProductID,
AsOfDate,
Months,
MonthNum,
YearNum,
YearMonthNum,
Price as 'MAXPrice'
From STG3
Where MinMaxIndicator = 'MAX'
)

,MINPRICE as (
Select ProductID,
AsOfDate,
Months,
MonthNum,
YearNum,
YearMonthNum,
Price as 'MINPrice'
From STG3
Where MinMaxIndicator = 'MIN'
)

,MARKETPRICE as (
Select ProductID,
AsOfDate,
MonthNum,
YearNum,
YearMonthNum,
AVG(Price) as 'MarketPrice'
From STG3
Group By ProductID,AsOfDate,MonthNum,YearNum,YearMonthNum
)

,STG4 AS (
Select MX.ProductID,
MX.AsOfDate,
MX.Months,
MX.MonthNum,
MX.YearNum,
MX.YearMonthNum,
MAXPrice,
MINPrice,
MarketPrice
From MAXPRICE MX
Left Join MINPRICE MN
	ON MX.AsOfDate = MN.AsOfDate AND MX.YearMonthNum = MN.YearMonthNum AND MX.ProductID=MN.ProductID
Left Join MARKETPRICE MK
	ON MX.AsOfDate = MK.AsOfDate AND MX.YearMonthNum = MK.YearMonthNum AND MX.ProductID=MK.ProductID
)

,STG5 AS(
SELECT *
FROM STG4
CROSS APPLY (
SELECT *
FROM DateList 
)dl
WHERE DL.Date>=STG4.AsOfDate 
      AND Month(DL.Date)- Month(STG4.AsOfDate)<=12 
	  AND ( CASE --WHEN MonthNum = MONTH(STG_2.AsOfDate) THEN 1 
	             WHEN MONTH(DL.Date) = MonthNum AND  Year(DL.Date) = YearNum  THEN 1
				 ELSE 0 END=1)
	--AND MonthNum = 3
)

SELECT  CAST( CONVERT(BIGINT, CONVERT(VARCHAR, ProductID)) as varchar) +'-'+ CAST( CONVERT(BIGINT, CONVERT(VARCHAR, ProductID)) as varchar)AS 'ItemKey'
      ,AsOfDate
	  ,[Date]
	  ,MAXPrice
	  ,MINPrice
	  ,MarketPrice
FROM STG5


