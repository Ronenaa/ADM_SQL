SET NOCOUNT ON;
IF OBJECT_ID('tempdb..#am') IS NOT NULL DROP TABLE #am;
IF OBJECT_ID('tempdb..#ob') IS NOT NULL DROP TABLE #ob;
IF OBJECT_ID('tempdb..#raw') IS NOT NULL DROP TABLE #raw;

SELECT ItemKey, Article_Description
INTO #am
FROM (

  SELECT cast(CONVERT(INT, CONVERT(VARCHAR,M.QOD_MOTSR)) as varchar)+ '-' + cast(CONVERT(INT, CONVERT(VARCHAR,M.QOD_MOTSR)) as varchar) AS ItemKey
        ,CASE
            WHEN A.ART_DESC is NULL OR A.ART_DESC = ''
              THEN 'Other'
            ELSE A.ART_DESC
         END AS Article_Description
  FROM MOTSRIM M
  LEFT JOIN (SELECT * FROM [dbo].[TRGOM_ANGLIT]
             WHERE SOG_PRIT_SOCN_BNQ_TM_SH_CHBRH = NCHAR(1508)) T
         ON M.QOD_MOTSR = T.QOD
  LEFT JOIN (SELECT distinct [TARO_ANGLIT_2_ART],[ART_DESC] FROM [dbo].[TRGOM_ANGLIT]
             WHERE SOG_PRIT_SOCN_BNQ_TM_SH_CHBRH = NCHAR(1508) and ART_DESC<>'' and TARO_ANGLIT_3_PNL_HCNSH = 8052) A
         ON A.TARO_ANGLIT_2_ART = T.TARO_ANGLIT_2_ART

) src;
CREATE CLUSTERED INDEX ix_am ON #am(ItemKey);

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
		,HZ.OrderCreateTime AS 'OrderCreateTime'
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
,ordersBase as (
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

	-- Priced quantity: zeroed on rows whose paired price hasn't landed yet, so averages aren't diluted by unpriced rows
	CASE
		WHEN o.PremiumFlag = 1 THEN CASE WHEN ISNULL(FinalPriceClosedContract,0) = 0 THEN 0 ELSE o.Quantity END
		ELSE CASE WHEN ISNULL(o.UnitNetPriceUSD,0) = 0 THEN 0 ELSE o.Quantity END
	END AS PricedQuantity,
	CASE
		WHEN o.PremiumFlag = 1 AND ISNULL(MarketTempPrice,0) <> 0 THEN o.BasisQTY
		ELSE 0
	END AS PricedBasisQTY,
	CASE
		WHEN o.PremiumFlag = 1 THEN CASE WHEN ISNULL(FinalPriceClosedContract,0) = 0 THEN 0 ELSE o.FlatQTY END
		ELSE CASE WHEN ISNULL(o.UnitNetPriceUSD,0) = 0 THEN 0 ELSE o.FlatQTY END
	END AS PricedFlatQTY,
	-- CF-specific priced quantity: a row can have a valid Order/FOT price but a blank/zero CF price, so CF averages need their own zero-check
	CASE
		WHEN o.PremiumFlag = 1 THEN CASE WHEN ISNULL(o.CFFLAT_CC,0) = 0 THEN 0 ELSE o.Quantity END
		ELSE CASE WHEN ISNULL(o.CFFlat,0) = 0 THEN 0 ELSE o.Quantity END
	END AS PricedQuantityCF,
	-- FOT-specific priced quantity: same idea for the FOT price view
	CASE
		WHEN o.PremiumFlag = 1 THEN CASE WHEN ISNULL(o.FOT_CC,0) = 0 THEN 0 ELSE o.Quantity END
		ELSE CASE WHEN ISNULL(o.FotPrice,0) = 0 THEN 0 ELSE o.Quantity END
	END AS PricedQuantityFOT,
	-- Combined open-quantity (Basis+Flat as QuantityLeft on non-premium rows) zeroed per price type, to match LineTotalFlatValueBalanceCFFUSD/FOTUSD's own grain
	CASE
		WHEN o.PremiumFlag = 1 THEN CASE WHEN ISNULL(o.CFFLAT_NCC,0) = 0 THEN 0 ELSE o.BasisQTY+o.FlatQTY END
		ELSE CASE WHEN ISNULL(o.CFFlat,0) = 0 THEN 0 ELSE o.QuantityLeft END
	END AS PricedOpenQtyCF,
	CASE
		WHEN o.PremiumFlag = 1 THEN CASE WHEN ISNULL(o.FOT_NCC,0) = 0 THEN 0 ELSE o.BasisQTY+o.FlatQTY END
		ELSE CASE WHEN ISNULL(o.FotPrice,0) = 0 THEN 0 ELSE o.QuantityLeft END
	END AS PricedOpenQtyFOT,

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

-- Step 1: materialise the base exactly as v1 did (no join) - this is the fast path.
SELECT ItemKey, OrderID, [Date], OrderCreateTime,
       PricedQuantity, PricedQuantityCF, PricedQuantityFOT,
       LineTotalFlatValueUSD, LineTotalFlatValueCFFUSD, LineTotalFlatValueFOTUSD
INTO #raw FROM ordersBase;

-- Step 2: attach the Article and drop unmapped items.
SELECT am.Article_Description AS Article, r.OrderID, r.[Date], r.OrderCreateTime,
       r.PricedQuantity, r.PricedQuantityCF, r.PricedQuantityFOT,
       r.LineTotalFlatValueUSD, r.LineTotalFlatValueCFFUSD, r.LineTotalFlatValueFOTUSD
INTO #ob
FROM #raw r
JOIN #am am ON am.ItemKey = r.ItemKey
WHERE am.Article_Description <> 'Other';
CREATE CLUSTERED INDEX ix_ob ON #ob(Article, OrderCreateTime DESC, OrderID DESC);

WITH Steps as (
    SELECT TOP (20)
           500 * CAST(ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS int) AS Step
    FROM sys.all_objects
)

/* One tail per (Article, price type). Pooled across every ItemKey in the
   Article, so "last 500 of Corn" is 500 units, not 500 per item. */
,OrdersLong as (
    SELECT 1 AS PriceTypeID, Article, OrderID, [Date], OrderCreateTime,
           PricedQuantity AS PricedQty, LineTotalFlatValueUSD AS LineValueUSD
    FROM #ob
    UNION ALL
    SELECT 2, Article, OrderID, [Date], OrderCreateTime, PricedQuantityCF, LineTotalFlatValueCFFUSD FROM #ob
    UNION ALL
    SELECT 3, Article, OrderID, [Date], OrderCreateTime, PricedQuantityFOT, LineTotalFlatValueFOTUSD FROM #ob
)

,Priced as (
    SELECT PriceTypeID, Article, OrderID, [Date], OrderCreateTime, PricedQty, LineValueUSD,
           LineValueUSD / PricedQty AS UnitPriceUSD
    FROM OrdersLong
    WHERE ISNULL(PricedQty, 0) > 0 AND LineValueUSD IS NOT NULL
)

,Tail as (
    SELECT p.*,
           ISNULL(SUM(p.PricedQty) OVER (PARTITION BY p.PriceTypeID, p.Article
                ORDER BY p.OrderCreateTime DESC, p.OrderID DESC
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) AS RunQtyBefore,
           SUM(p.PricedQty) OVER (PARTITION BY p.PriceTypeID, p.Article
                ORDER BY p.OrderCreateTime DESC, p.OrderID DESC
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)     AS RunQtyAfter,
           ISNULL(SUM(p.LineValueUSD) OVER (PARTITION BY p.PriceTypeID, p.Article
                ORDER BY p.OrderCreateTime DESC, p.OrderID DESC
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) AS RunValBefore
    FROM Priced p
)

,TailPruned as ( SELECT * FROM Tail WHERE RunQtyBefore < 10000 )

,TailTotal as (
    SELECT PriceTypeID, Article,
           MAX(RunQtyAfter)                 AS TotalQty,
           MAX(RunValBefore + LineValueUSD) AS TotalValue
    FROM Tail GROUP BY PriceTypeID, Article
)

/* Long form: one row per (Article, Step, price type). Pivoted below. */
,Stepped as (
    SELECT k.Article, k.PriceTypeID, s.Step,
           CASE WHEN b.OrderID IS NULL THEN tt.TotalQty ELSE s.Step END AS Qty,
           CASE WHEN b.OrderID IS NULL THEN tt.TotalValue
                ELSE b.RunValBefore + (s.Step - b.RunQtyBefore) * b.UnitPriceUSD
           END AS Val,
           b.OrderID          AS BoundaryOrderID,
           b.[Date]           AS BoundaryOrderDate,
           b.OrderCreateTime  AS BoundaryOrderCreateTime,
           CASE WHEN b.OrderID IS NULL THEN NULL
                ELSE s.Step - b.RunQtyBefore END AS BoundaryQtyTaken
    FROM (SELECT DISTINCT PriceTypeID, Article FROM TailPruned) k
    JOIN Steps s ON 1 = 1
    LEFT JOIN TailPruned b
           ON b.PriceTypeID = k.PriceTypeID AND b.Article = k.Article
          AND b.RunQtyBefore < s.Step AND b.RunQtyAfter >= s.Step
    LEFT JOIN TailTotal tt
           ON tt.PriceTypeID = k.PriceTypeID AND tt.Article = k.Article
)

/* ============ QA CHECK 1 ============
   Every step must return exactly its own step value, for every article,
   UNLESS the article's whole priced history is shorter than the step.
   Any row here where ShortFilled = 0 and Qty <> Step is a BUG. */
,Chk AS (
  SELECT Article, PriceTypeID, Step, Qty,
         CASE WHEN BoundaryOrderID IS NULL THEN 1 ELSE 0 END AS ShortFilled
  FROM Stepped
)
SELECT 'CHK1_step_equals_qty' AS Check_Name,
       SUM(CASE WHEN ShortFilled = 0 AND ABS(Qty - Step) > 0.001 THEN 1 ELSE 0 END) AS Violations,
       COUNT(*) AS RowsChecked
FROM Chk

UNION ALL
/* ============ QA CHECK 2 ============
   A short-filled step must report LESS than the step, never more. */
SELECT 'CHK2_shortfill_lt_step',
       SUM(CASE WHEN ShortFilled = 1 AND Qty > Step + 0.001 THEN 1 ELSE 0 END),
       SUM(CASE WHEN ShortFilled = 1 THEN 1 ELSE 0 END)
FROM Chk

UNION ALL
/* ============ QA CHECK 3 ============
   Cumulative: value and qty must never decrease as Step grows. */
SELECT 'CHK3_monotonic',
       SUM(CASE WHEN nxt_val < val - 0.01 OR nxt_qty < qty - 0.001 THEN 1 ELSE 0 END), COUNT(*)
FROM (
  SELECT Val AS val, Qty AS qty,
         LEAD(Val) OVER (PARTITION BY Article, PriceTypeID ORDER BY Step) AS nxt_val,
         LEAD(Qty) OVER (PARTITION BY Article, PriceTypeID ORDER BY Step) AS nxt_qty
  FROM Stepped
) m WHERE nxt_val IS NOT NULL

UNION ALL
/* ============ QA CHECK 4 ============
   The step-10000 total must never exceed the article's entire priced history. */
SELECT 'CHK4_not_exceeding_history',
       SUM(CASE WHEN s.Qty > t.TotalQty + 0.001 THEN 1 ELSE 0 END), COUNT(*)
FROM Stepped s
JOIN TailTotal t ON t.Article = s.Article AND t.PriceTypeID = s.PriceTypeID

UNION ALL
/* ============ QA CHECK 5 ============
   Boundary slice must be positive and never exceed the boundary order itself. */
SELECT 'CHK5_boundary_slice_sane',
       SUM(CASE WHEN BoundaryQtyTaken IS NOT NULL
                 AND (BoundaryQtyTaken <= 0 OR BoundaryQtyTaken > 1000000) THEN 1 ELSE 0 END),
       COUNT(*)
FROM Stepped WHERE BoundaryQtyTaken IS NOT NULL

UNION ALL
/* ============ QA CHECK 6 ============
   THE KEY RECONCILIATION. The largest step of an article whose entire history
   is shorter than 10000 must equal a plain weighted average over ALL of that
   article's priced factOrders rows - computed here completely independently
   of the tail/step logic. This is the real tie-out to factOrders. */
SELECT 'CHK6_ties_to_factOrders',
       SUM(CASE WHEN ABS(a.StepAvg - b.DirectAvg) > 0.01 THEN 1 ELSE 0 END), COUNT(*)
FROM (
  SELECT s.Article, s.PriceTypeID, s.Val / NULLIF(s.Qty,0) AS StepAvg
  FROM Stepped s
  JOIN TailTotal t ON t.Article = s.Article AND t.PriceTypeID = s.PriceTypeID
  WHERE s.Step = 10000 AND t.TotalQty < 10000
) a
JOIN (
  SELECT Article, PriceTypeID, SUM(LineValueUSD) / NULLIF(SUM(PricedQty),0) AS DirectAvg
  FROM Priced GROUP BY Article, PriceTypeID
) b ON b.Article = a.Article AND b.PriceTypeID = a.PriceTypeID
;
