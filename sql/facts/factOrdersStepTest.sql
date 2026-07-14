------ active in prod 21-1-25
-------------- מחירון יומי להשוואה-----------------
With LastCreation as (
Select OrderId,MAX(CreateDate) AS LastCreation
From tblOrderPriceS
Group By OrderID
)

,LastVersion as (
Select o.OrderId,Max(DayVersion) as LastVersion
From tblOrderPriceS o
Inner Join LastCreation LC
	ON o.OrderID=LC.OrderID AND o.CreateDate=LC.LastCreation
Group By o.OrderID
)

,OrderPrices as (
Select O.*
from tblOrderPriceS o
Inner Join LastCreation LC
	ON o.OrderID=LC.OrderID And o.CreateDate=LC.LastCreation
Inner Join LastVersion LV
	ON o.OrderID=LV.OrderID AND o.DayVersion = LV.LastVersion
Where 1=1
),

BasisContract as (
Select a.POSO_SOID as ID,
ISNULL(SUM(b.TotalSuccessfullyClosed),0) as TotalContracts
FRom [dbo].[SaleOrder2PurchaseOrderLines] a
LEFT JOIN [dbo].[viewStockExchangeOrders] b
	ON a.POSO_POID = b.SEO_PurchaseOrderID AND a.POSO_POLID = b.SEO_PurchaseOrderLine
Where 1=1
and POSO_isActive = 1
and LEFT(POSO_POID,1) = 9
Group BY a.POSO_SOID
)

,SMPE_1 as (
select * 
,ROW_NUMBER() OVER (PARTITION BY SMPE_ProductID,SMPE_Year,SMPE_Month ORDER BY SMPE_CreateDate DESC,SMPE_DueDate DESC) as rownum
from [dbo].[StockMarketProductEOM]
)

,SMPE as (
select * from SMPE_1 where  rownum = 1
)

,SalesOrder2Purchase as (
select * from [dbo].[SaleOrder2PurchaseOrderLines]
where POSO_IsActive = '1'
--and POSO_SOID = 128444 --131989
and POSO_HasRolloverBeenMade = 0
and LEFT(POSO_POID,2) = 90   ---- Temp Order Only
)



--------------------הזמנות------------------
,Orders as (
SELECT 

'1' as 'EntityID'
        ,CAST(HZ.MSPR_HZMNH AS VARCHAR) AS 'OrderID'
		,1 AS 'OrderLineNumber'
		,'Orders' AS 'DocName'
		,CAST(CONVERT(INT, CONVERT(VARCHAR,HZ.QOD_MQBL))as varchar) AS 'AccountKey' -- customer from invoices in case you need just invoices 
		,SUBSTRING(HZ.T_ASPQH,1,4) + '-' + SUBSTRING(HZ.T_ASPQH,5,2) + '-' + SUBSTRING(HZ.T_ASPQH,7,2) as 'תאריך אספקה'
        ,SUBSTRING(HZ.T_HZMNH,1,4) + '-' + SUBSTRING(HZ.T_HZMNH,5,2) + '-' + SUBSTRING(HZ.T_HZMNH,7,2) as 'Date'
		,null AS 'Time'
		,cast(CONVERT(INT, CONVERT(VARCHAR,HZ.AISH_MCIROT)) as varchar) AS 'AgentKey' 
		,cast(CONVERT(INT, CONVERT(VARCHAR,HZ.MOTSR_MOZMN)) as varchar)+ '-' + cast(CONVERT(INT, CONVERT(VARCHAR,HZ.MOTSR_MOZMN)) as varchar) AS 'ItemKey'
		--,CAST('1' as varchar) + CAST(CONVERT(INT, CONVERT(VARCHAR, i.BRANCH)) as varchar) AS 'BranchKey'
		,null AS 'EmployeeKey'
		,null AS 'חבר מועדון'
		--,(1 - (1 - ii.[T$PERCENT] / 100) * (1 - i.[T$PERCENT] / 100)) AS 'DiscountPercent' -- the DiscountPercent for the invoice and invoice line
		,null AS 'DiscountPercent'
		 --,(1 - (1 - ii.[T$PERCENT] / 100) * (1 - i.[T$PERCENT] / 100)) * ii.TQUANT / 1000 * ROUND(ii.PRICE, 2) * ii.IEXCHANGE AS 'LineTotalDiscount' -- the discount from gross price to the net
		,NULL AS 'LineTotalDiscount'
		--,ROUND(ii.PRICE, 2) * ii.IEXCHANGE AS 'UnitGrossPrice'
		,NULL AS 'UnitGrossPrice'
		--,ROUND(ii.PRICE * (1 - ii.[T$PERCENT] / 100) * (1 - i.[T$PERCENT] / 100), 2) * ii.IEXCHANGE AS 'UnitNetPrice'
		,CASE
	         WHEN HZ.MTBE = '$' THEN MCHIR_ICH
		   --THEN CAST(HZ.MCHIR_ICH AS decimal (12,4))
	         ELSE CAST(HZ.MCHIR_ICH *(1/SM.NEW_SHER)  AS decimal (12,4))
         END AS 'UnitNetPriceUSD'
		,HZ.CMOT_MOZMNT AS 'Quantity'
		,HZ.CMOT_SHSOPQH AS 'QuantitySupply'
		,HZ.CMOT_MOZMNT - HZ.CMOT_SHSOPQH AS 'QuantityLeft'
		--The cost can be tor.cost * Quant or just tor.cost
		--Option 1
		/*,CASE WHEN tor.COSTFLAG NOT IN ('C', '\0') 
		      THEN tor.COST * (CASE WHEN (tor.QUANT) < 0.0 THEN - 1 ELSE 1 END)
		      ELSE p.COST * ((ii.QUANT / 1000) / p.COSTQUANT) * (CASE WHEN i.DEBIT = 'C' THEN - 1 ELSE 1 END) * (CASE WHEN ii.CREDITFLAG = 'Y' THEN 0 ELSE 1 END)
		END AS 'LineTotalCost' --------Cost without Quant
		*/
		,NULL AS 'LineTotalCost'
		/*,(CASE WHEN ii.CURRENCY <> '-1'
			THEN round(ii.IVCOST, 2) * ii.IEXCHANGE
			ELSE round(ii.IVCOST, 2) END) * (CASE WHEN (i.DEBIT = 'C') 	THEN - 1 ELSE 1 END) AS 'LineTotalNet'*/
		,CASE
	         WHEN HZ.MTBE = '$'
		     THEN CAST((HZ.MCHIR_ICH)*CMOT_MOZMNT AS decimal (12,4))
	         ELSE CAST(HZ.MCHIR_ICH*HZ.CMOT_MOZMNT*(1/SM.NEW_SHER) AS decimal (12,4))
         END AS 'LineTotalNetUSD'
		,CASE
	         WHEN HZ.MTBE = '$'
		     THEN CAST((HZ.MCHIR_ICH)*(HZ.CMOT_MOZMNT - HZ.CMOT_SHSOPQH) AS decimal (12,4))
	         ELSE CAST(HZ.MCHIR_ICH*(HZ.CMOT_MOZMNT - HZ.CMOT_SHSOPQH)*(1/SM.NEW_SHER) AS decimal (12,4))
         END AS 'LineTotalBalanceUSD'
					
		--, (round(ii.IVCOST, 2)/ii.IEXCHANGE) *fnc.EXCHANGE2   *(CASE WHEN (i.DEBIT = 'C') THEN - 1 ELSE 1 END) AS 'LineTotalNet_USD'
		,CASE
	         WHEN HZ.MTBE = '$'
		     THEN CAST(HZ.MCHIR_ICH*SM.NEW_SHER AS decimal (12,4))
	         ELSE CAST(HZ.MCHIR_ICH AS decimal (12,4))
         END AS 'UnitNetPriceNIS'
	    ,CASE
	         WHEN HZ.MTBE = '$'
		     THEN CAST((HZ.MCHIR_ICH*SM.NEW_SHER)*CMOT_MOZMNT AS decimal (12,4))
	         ELSE CAST(HZ.MCHIR_ICH*HZ.CMOT_MOZMNT AS decimal (12,4))
         END AS 'LineTotalNetNIS'

/*		 ,CASE
			WHEN ps.OrderID IS NOT NULL THEN OP.CFFLAT_CC
			ELSE OP.CFFlat END																	 AS 'UnitPriceCF'
		 ,CASE
			WHEN ps.OrderID IS NOT NULL THEN OP.CFFLAT_CC*ps.FlatQTY
			ELSE Op.CFFlat*OP.OriginalQty END													 AS 'LineTotalCF_USD'
		 ,CASE
			WHEN ps.OrderID IS NOT NULL THEN OP.CFFLAT_CC*ps.FlatQTY*OP.DollarRate
			ELSE OP.CFFlat*OP.DollarRate*OP.OriginalQty END										 AS 'LineTotalCF_NIS'
		 ,CASE
			WHEN ps.OrderID IS NOT NULL THEN OP.FOT_CC
			ELSE OP.FotPrice END																 AS 'UnitPriceFOT'
		 ,CASE
			WHEN ps.OrderID IS NOT NULL THEN OP.FOT_CC*ps.FlatQTY
			ELSE Op.FotPrice*OP.OriginalQty END													 AS 'LineTotalFOT_USD'
		 ,CASE
			WHEN ps.OrderID IS NOT NULL THEN OP.FOT_CC*ps.FlatQTY*OP.DollarRate
			ELSE OP.FotPrice*OP.DollarRate*OP.OriginalQty END									 AS 'LineTotalFOT_NIS'
		 --,OP.CFPremium AS 'UnitPricePremium'
		 --,Op.CFPremium*OP.OriginalQty AS 'LineTotalPremium_USD'
		 --,OP.CFPremium*OP.DollarRate*OP.OriginalQty AS 'LineTotalPremium_NIS'
		 ,POL.POL_StockMarketTempPriceForNoneClosed*ps.BasisQTY									 AS 'LineTotalFlatValueBalanceUSD'
		 ,CASE
			WHEN ps.OrderID IS NOT NULL THEN OP.CFFLAT_NCC
			ELSE NULL	END																		 AS 'MarketTempPriceCFF_USD'
		 ,CASE
			WHEN ps.OrderID IS NOT NULL THEN OP.CFFLAT_NCC*ps.BasisQTY
			ELSE NULL END																		 AS 'LineTotalFlatValueBalanceCFFUSD'
		 ,CASE
			WHEN ps.OrderID IS NOT NULL THEN OP.FOT_NCC
			ELSE NULL	END																		 AS 'MarketTempPriceFOT_USD'
		 ,CASE
			WHEN ps.OrderID IS NOT NULL THEN OP.FOT_NCC*ps.BasisQTY
			ELSE NULL END																		 AS 'LineTotalFlatValueBalanceFOTUSD'
*/
		 ,Case
			WHEN OP.CFPremium >0
				THEN 1
			ELSE 0
		END AS 'PremiumFlag'
		,CONVERT(INT, CONVERT(VARCHAR,SUBSTRING(HZ.T_ASPQH,1,4) + SUBSTRING(HZ.T_ASPQH,5,2))) AS YearMonth
		----
		,HZ.OrderStatus AS 'סטטוס'
		,NULL AS 'ChargeFlag'  
		,NULL AS 'סטטוס ליקוט כותרת'
	    ,NULL AS 'סטטוס ליקוט שורה'
		,NULL AS 'דגל סטורנו'
		
		,CASE
			WHEN DATEDIFF(dd,
			CAST(SUBSTRING(HZ.T_HZMNH,1,4) + '-' + SUBSTRING(HZ.T_HZMNH,5,2) + '-' + SUBSTRING(HZ.T_HZMNH,7,2) as Date)
			,CAST(SUBSTRING(HZ.T_ASPQH,1,4) + '-' + SUBSTRING(HZ.T_ASPQH,5,2) + '-' + SUBSTRING(HZ.T_ASPQH,7,2) AS Date)) > 60
				THEN 1
			ELSE 2
		END as 'SpotFlag'
		,CASE
			WHEN DATEDIFF(dd,
			CAST(SUBSTRING(HZ.T_ASPQH,1,4) + '-' + SUBSTRING(HZ.T_ASPQH,5,2) + '-' + SUBSTRING(HZ.T_ASPQH,7,2) AS Date)
			,GETDATE()) > 80 AND HZ.OrderStatus in (1,2)
				THEN 1
			ELSE 0
		END as 'OldFlag'
		,QOD_SHOLCH as 'SupplierWarehouse'
		,OP.ActionType AS 'TransactionType'
		,t.PO_Family as 'Family'
		,CASE 
			WHEN HZ.OrderStatus = 4 or HZ.OrderStatus = 5
				THEN NULL
			WHEN t.PO_Family = 2
				THEN CASE
						WHEN isNULL(ROUND(OP.OriginalQty/OP.QtyTonePerContract,0),0) > 0 
							THEN ((ROUND(OP.OriginalQty/OP.QtyTonePerContract,0)-ISNULL(bc.Totalcontracts,0))/ROUND(OP.OriginalQty/OP.QtyTonePerContract,0))*HZ.CMOT_MOZMNT ---- כמות שלא תומחרה =כמות הזמנה כפול(סך הכל חוזים אפשריים - חוזים שנחתמו חלקי סך כל החוזים האפשריים
						ELSE POB.Balance
					END
			WHEN t.PO_Family = 1
				Then Null
			ELSE NULL
		END AS BasisQTY
		,CASE 
			WHEN POB.Balance IS NULL THEN HZ.CMOT_MOZMNT
			WHEN HZ.OrderStatus = 4 or HZ.OrderStatus = 5
				Then POB.Balance
			WHEN t.PO_Family = 2
				THEN CASE
						WHEN isNULL(ROUND(OP.OriginalQty/OP.QtyTonePerContract,0),0) > 0 
							THEN (ISNULL(bc.Totalcontracts,0)/ROUND(OP.OriginalQty/OP.QtyTonePerContract,0))*HZ.CMOT_MOZMNT ---- כמות שתומחרה =כמות הזמנה כפול חוזים שנחתמו חלקי סך כל החוזים האפשריים
						ELSE NULL
					END
			WHEN t.PO_Family = 1
				Then POB.Balance
			ELSE  POB.Balance
		END AS FlatQTY
		--,bc.TotalContracts as ContractSigned
		,POL_StockExchangeDate										
		,sf.PF_GeneralFactor
		,POL_PremiumPrice
		,POL_StockMarketPrice
		,smp.SMP_Symbol
		,CAST (smpe.SMPE_TempPrice as float) as SMPE_TempPrice
		,ACT.TAOR_AOPTSIH as OrderType
		,POL.POL_StockMarketTempPriceForNoneClosed as MarketTempPrice
		,POL_FinalPriceForClosedContract			as FinalPriceClosedContract

		,OP.CFFlat
		,OP.CFFLAT_CC
		,OP.CFFLAT_NCC
		,OP.FotPrice
		,OP.FOT_CC
		,OP.FOT_NCC
		,OP.OriginalQty
		,OP.DollarRate
		,POL.POL_StockMarketTempPriceForNoneClosed
		,GETDATE() AS RowInsertDatetime
FROM [HZMNOT] HZ
LEFT JOIN (
           SELECT *, 
                    CASE
                    	WHEN sher = 0
                    	THEN FIRST_VALUE(sher) OVER (PARTITION BY value_partition ORDER BY Tarikh) 
                    	ELSE sher
                    END AS new_sher
            FROM (
                     SELECT *
					 ,SUM(CASE WHEN sher=0 THEN 0 ELSE 1 END) OVER (ORDER BY tarikh ) AS value_partition
                     FROM SHERI_MTBE) m
           
           ) SM 
ON SM.TARIKH=HZ.T_HZMNH
Left Join OrderPrices OP
	ON op.OrderID = Hz.MSPR_HZMNH
LEFT JOIN PurchaseOrderTypes t
	ON t.PO_TypeID = OP.ActionType
Left Join BasisContract bc
	ON op.OrderID = bc.ID
----
left join SalesOrder2Purchase sol
	on HZ.MSPR_HZMNH = sol.POSO_SOID
left join PurchaseOrderLines POL
	on sol.POSO_POID = pol.POL_OrderID and sol.POSO_POLID = pol.POL_LineID
left join ProductStockMarketFactors sf
	on pol.POL_ProductID = sf.PF_ProductID and POL.POL_FactorCode = sf.PF_FactorID 
left join StockMarketProduct smp
	on POL.POL_ProductID = smp.SMP_ProductID AND MONTH(POL.POL_StockExchangeDate) = smp.SMP_MonthID
left join SMPE as smpe --(select * from SMPE where  rownum = 1) smpe
	on POL.POL_ProductID = smpe.SMPE_ProductID AND YEAR(POL.POL_StockExchangeDate) = smpe.SMPE_Year AND MONTH(POL.POL_StockExchangeDate) = smpe.SMPE_Month
Left Join viewPurchaseOrderBalance POB
	ON POL.POL_OrderID = POB.POID AND POL.POL_LineID = POB.POLineID
LEFT JOIN TBLT_PEOLOT_HZMNH_T_MSHLOCH ACT
	ON HZ.actionType = ACT.MS_AOPTSIH

WHERE Cast(SUBSTRING(T_HZMNH,1,4) as int) >=2018 and Cast(SUBSTRING(T_HZMNH,1,4) as int) <= Year(Getdate())
AND HZ.OrderStatus<>3
AND HZ.ActionType IN (1,11,12,14)--NOT IN (2,6,7,10/*,11*/)

--AND HZ.MSPR_HZMNH IN ('139761')--,'136892','136030','136532','137242', '135805')--'135671'-- '136227' --'133900'
--AND SUBSTRING(HZ.T_ASPQH,1,4) = 2025 AND SUBSTRING(HZ.T_ASPQH,5,2) = 01
)
,test as(

SELECT	o.*																		
		,CASE 
			WHEN o.PremiumFlag = 1 THEN o.FlatQTY * FinalPriceClosedContract
			ELSE LineTotalBalanceUSD
		 END																							AS LineTotalFlatBalanceUSD
		,CASE 
			WHEN o.PremiumFlag = 1 THEN o.FlatQTY * FinalPriceClosedContract
			ELSE LineTotalNetUSD
		 END																							AS LineTotalFlatUSD
		,CASE 
			WHEN o.PremiumFlag = 1 THEN o.BasisQTY * MarketTempPrice
			ELSE 0
		 END
		 +
		CASE 
			WHEN o.PremiumFlag = 1 THEN o.FlatQTY * FinalPriceClosedContract
			ELSE LineTotalNetUSD
		 END																							AS LineTotalFlatValueUSD

    ,CASE 
        WHEN o.PremiumFlag = 1 THEN o.CFFLAT_CC 
        ELSE o.CFFlat 
    END AS UnitPriceCF,
    
    -- Calculate LineTotalCF_USD using PremiumFlag and FlatQTY (or OriginalQty)
    CASE 
        WHEN o.PremiumFlag = 1 THEN o.CFFLAT_CC * o.FlatQTY 
        ELSE o.CFFlat * o.OriginalQty 
    END AS LineTotalCF_USD,
    
    -- Calculate LineTotalCF_NIS (apply DollarRate to the above)
    CASE 
        WHEN o.PremiumFlag = 1 THEN o.CFFLAT_CC * o.FlatQTY * o.DollarRate 
        ELSE o.CFFlat * o.DollarRate * o.OriginalQty 
    END AS LineTotalCF_NIS,
    
    -- Calculate UnitPriceFOT based on the PremiumFlag
    CASE 
        WHEN o.PremiumFlag = 1 THEN o.FOT_CC 
        ELSE o.FotPrice 
    END AS UnitPriceFOT,
    
    -- Calculate LineTotalFOT_USD using PremiumFlag and FlatQTY (or OriginalQty)
    CASE 
        WHEN o.PremiumFlag = 1 THEN o.FOT_CC * o.FlatQTY 
        ELSE o.FotPrice * o.OriginalQty 
    END AS LineTotalFOT_USD,
    
    -- Calculate LineTotalFOT_NIS (apply DollarRate to the above)
    CASE 
        WHEN o.PremiumFlag = 1 THEN o.FOT_CC * o.FlatQTY * o.DollarRate 
        ELSE o.FotPrice * o.DollarRate * o.OriginalQty 
    END AS LineTotalFOT_NIS,
    
    -- Use BasisQTY to compute the flat value balance in USD
    CASE
		WHEN o.PremiumFlag = 1 THEN (o.POL_StockMarketTempPriceForNoneClosed * o.BasisQTY)+(o.FlatQTY * FinalPriceClosedContract) 
		ELSE LineTotalBalanceUSD
		END AS LineTotalFlatValueBalanceUSD,
    
    -- Calculate MarketTempPriceCFF_USD and its related total using BasisQTY
    CASE 
        WHEN o.PremiumFlag = 1 THEN o.CFFLAT_NCC 
        ELSE NULL 
    END AS MarketTempPriceCFF_USD,
    
    CASE 
        WHEN o.PremiumFlag = 1 THEN (o.CFFLAT_NCC * o.BasisQTY) + (o.FlatQTY * o.CFFLAT_NCC)
        ELSE QuantityLeft*o.CFFlat 
    END AS LineTotalFlatValueBalanceCFFUSD,
	CASE 
        WHEN o.PremiumFlag = 1 THEN (o.FlatQTY * o.CFFLAT_NCC)
        ELSE QuantityLeft*o.CFFlat 
    END AS LineTotalFlatBalanceCFFUSD,
    
    -- Calculate MarketTempPriceFOT_USD and its related total using BasisQTY
    CASE 
        WHEN o.PremiumFlag = 1 THEN o.FOT_NCC 
        ELSE NULL 
    END AS MarketTempPriceFOT_USD,
    
    CASE 
        WHEN o.PremiumFlag = 1 THEN  (o.FOT_NCC * o.BasisQTY) + (o.FlatQTY * o.FOT_NCC)--(o.FOT_NCC * o.BasisQTY)--
        ELSE QuantityLeft*o.FotPrice 
    END AS LineTotalFlatValueBalanceFOTUSD,
    CASE 
        WHEN o.PremiumFlag = 1 THEN (o.FOT_NCC * o.FlatQTY)
        ELSE QuantityLeft*o.FotPrice 
    END AS LineTotalFlatBalanceFOTUSD,

    CASE 
        WHEN o.PremiumFlag = 1 
             THEN (o.CFFLAT_NCC * o.BasisQTY) + (o.CFFLAT_CC * o.FlatQTY)
        ELSE (o.CFFlat * o.OriginalQty)
    END AS LineTotalFlatValueCFFUSD,
    
    -- Combined calculation for FOT pricing
    CASE 
        WHEN o.PremiumFlag = 1 
             THEN (o.FOT_NCC * o.BasisQTY) + (o.FOT_CC * o.FlatQTY)
        ELSE (o.FotPrice * o.OriginalQty)
    END AS LineTotalFlatValueFOTUSD,

	CASE
		WHEN o.[סטטוס] = 1 then 'Open Order'
		else null
	END AS 'Open_Slicer'

/*		,CASE
			WHEN o.PremiumFlag = 1 THEN o.LineTotalFlatValueBalanceCFFUSD + LineTotalCF_USD
			ELSE LineTotalCF_USD END																	AS LineTotalFlatValueCFFUSD
		,CASE
			WHEN o.PremiumFlag = 1 THEN o.LineTotalFlatValueBalanceFOTUSD + LineTotalCF_USD
			ELSE LineTotalCF_USD END																	AS LineTotalFlatValueFOTUSD
*/

FROM Orders o
where 1=1
--and OrderID = 140452
--and BasisQTY is not null
--and BasisQTY <>0
)
,cte as (
SELECT
        orderid,
        [Date],
        Quantity,
        LineTotalFlatValueUSD,

        SUM(Quantity) OVER (
            ORDER BY [Date] desc, orderid
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS RunningQty,

        SUM(LineTotalFlatValueUSD) OVER (
            ORDER BY [Date] desc, orderid
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS RunningValue
    FROM test
	where [Date] >= '2026-04-01'
	)


,
labeled AS (
    SELECT *,
           CASE
               WHEN RunningQty >= 10000 THEN 10000
               WHEN RunningQty >= 5000  THEN 5000
               WHEN RunningQty >= 2000  THEN 2000
               WHEN RunningQty >= 1000  THEN 1000
               WHEN RunningQty >= 500   THEN 500
           END AS QtyBucket,
		   CASE
               WHEN RunningQty >= 10000 THEN RunningQty
               WHEN RunningQty >= 5000  THEN RunningQty
               WHEN RunningQty >= 2000  THEN RunningQty
               WHEN RunningQty >= 1000  THEN RunningQty
               WHEN RunningQty >= 500   THEN RunningQty
           END AS test
    FROM cte
)
SELECT
    QtyBucket,
	test,
    MIN(RunningValue / RunningQty) AS PriceUSD
FROM labeled
WHERE QtyBucket IS NOT NULL
GROUP BY QtyBucket,test
ORDER BY QtyBucket;





--select 
--orderid,
--[Date],
--Quantity,
--LineTotalFlatValueUSD,
--sum(Quantity) over (order by [date] desc) as qty_counter
--from test 
--where [Date] >= '2026-04-01'
--order by [Date] desc
