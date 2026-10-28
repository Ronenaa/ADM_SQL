-- factOrders (current) - extracted from TMDL by extract_queries.py
-- Run on STG only. Before running: Query > Query Options > Results > Grid >
-- tick 'Discard results after execution', so SSMS does not render the rows.
PRINT '===== factOrders | current =====';
SET STATISTICS IO, TIME ON;
GO

-- factOrders - fixes PricedQuantity / PricedQuantityCF / PricedQuantityFOT for Basis (PremiumFlag=1) rows.
--
-- Bug: for Basis rows, LineTotalFlatValueUSD/CFFUSD/FOTUSD already split value as
--   (FlatQTY * closed rate) + (BasisQTY * open/market rate)
-- i.e. they count BOTH the priced (closed) and the still-open (market-priced) portion of the order.
-- But PricedQuantity/CF/FOT only counted a row's full Quantity when the CLOSED rate was present,
-- and contributed 0 otherwise - so an order that's open/unpriced on its Basis leg added its
-- open-leg dollars to the numerator of [Avg Unit Price] while contributing 0 to the denominator,
-- inflating the average price for any month with open Basis orders (e.g. the "Switch Measures
-- Price" Sales card going to $644 for Dec 2026 while the per-order detail averaged ~$504-510).
--
-- Fix: PricedQuantity/CF/FOT now mirror the same split as their matching LineTotalFlatValue*USD -
-- FlatQTY counted when the closed rate is present, PLUS BasisQTY counted when the open/market rate
-- is present - so every dollar in the numerator has matching quantity in the denominator.
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

--,TRGOM_ANGLIT_ as(
--SELECT
--		T.*,
--		S_IN.PNL_Desc as PNL_Desc_Sales ,
--		P_OUT.PNL_Desc as PNL_Desc_Purchase
--FROM [dbo].[TRGOM_ANGLIT] T
--	LEFT JOIN (SELECT * FROM tblPnlList where PNL_Type = 'IN') S_IN
--		ON	T.TARO_ANGLIT_3_PNL_HCNSH = S_IN.PNL_ID
--	LEFT JOIN (SELECT * FROM tblPnlList where PNL_Type = 'OUT') P_OUT
--		ON	T.TARO_ANGLIT_4_PNL_HOTSAH = P_OUT.PNL_ID
--WHERE SOG_PRIT_SOCN_BNQ_TM_SH_CHBRH = N'פ'
--)

--,items as(
--SELECT  CAST( CONVERT(BIGINT, CONVERT(VARCHAR, M.QOD_MOTSR)) as varchar) AS 'ItemKey'
--,Case
--				WHEN A.ART_DESC is NULL OR A.ART_DESC = ''
--					THEN 'Other'
--				ELSE A.ART_DESC
--			END AS 'Article_Description'
--  FROM MOTSRIM M
--  LEFT JOIN TRGOM_ANGLIT_ T on M.QOD_MOTSR = T.QOD
--  LEFT JOIN (SELECT distinct [TARO_ANGLIT_2_ART],[ART_DESC]
--			FROM [dbo].[TRGOM_ANGLIT]
--			WHERE SOG_PRIT_SOCN_BNQ_TM_SH_CHBRH = N'פ' and ART_DESC<>'' and TARO_ANGLIT_3_PNL_HCNSH = 8052) A
--			ON A.TARO_ANGLIT_2_ART = T.TARO_ANGLIT_2_ART
			--)

--------------------הזמנות------------------

,Orders as (
SELECT

'1' as 'EntityID'
        ,CAST(HZ.MSPR_HZMNH AS VARCHAR) AS 'OrderID'
		,1 AS 'OrderLineNumber'
		,'Orders' AS 'DocName'
		,CAST(CONVERT(INT, CONVERT(VARCHAR,HZ.QOD_MQBL))as varchar) AS 'AccountKey' -- customer from invoices in case you need just invoices
		,TRY_CAST(SUBSTRING(HZ.T_ASPQH,1,4) + '-' + SUBSTRING(HZ.T_ASPQH,5,2) + '-' + SUBSTRING(HZ.T_ASPQH,7,2) AS date) as 'תאריך אספקה'
        ,TRY_CAST(SUBSTRING(HZ.T_HZMNH,1,4) + '-' + SUBSTRING(HZ.T_HZMNH,5,2) + '-' + SUBSTRING(HZ.T_HZMNH,7,2) AS date) as 'Date'
		,HZ.OrderCreateTime AS 'OrderCreateTime'
		,null AS 'Time'
		,cast(CONVERT(INT, CONVERT(VARCHAR,HZ.AISH_MCIROT)) as varchar) AS 'AgentKey'
		,cast(CONVERT(INT, CONVERT(VARCHAR,HZ.MOTSR_MOZMN)) as varchar)+ '-' + cast(CONVERT(INT, CONVERT(VARCHAR,HZ.MOTSR_MOZMN)) as varchar) AS 'ItemKey'
		--,it.Article_Description
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
		,NULL AS 'LineTotalCost'
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
		--,CASE
		--	when POL_FinalPriceForClosedContract is not null or OP.CFPremium = 0 then sum(HZ.CMOT_MOZMNT) over (partition by it.Article_Description order by HZ.OrderCreateTime desc)
		--	else 0
		--END as 'Recent Orders Step Count'
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
--LEFT JOIN items it
--	ON it.ItemKey = HZ.MOTSR_MOZMN
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

	-- Priced quantity: zeroed on rows whose paired price hasn't landed yet, so averages aren't diluted by unpriced rows.
	-- On Basis rows this must mirror LineTotalFlatValue*USD's own split (FlatQTY at the closed rate + BasisQTY at the open/market rate),
	-- otherwise dollars from the open BasisQTY leg get counted in the numerator while that quantity is dropped from the denominator.
	CASE
		WHEN o.PremiumFlag = 1 THEN
			(CASE WHEN ISNULL(FinalPriceClosedContract,0) = 0 THEN 0 ELSE o.FlatQTY END)
			+ (CASE WHEN ISNULL(MarketTempPrice,0) = 0 THEN 0 ELSE o.BasisQTY END)
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
		WHEN o.PremiumFlag = 1 THEN
			(CASE WHEN ISNULL(o.CFFLAT_CC,0) = 0 THEN 0 ELSE o.FlatQTY END)
			+ (CASE WHEN ISNULL(o.CFFLAT_NCC,0) = 0 THEN 0 ELSE o.BasisQTY END)
		ELSE CASE WHEN ISNULL(o.CFFlat,0) = 0 THEN 0 ELSE o.Quantity END
	END AS PricedQuantityCF,
	-- FOT-specific priced quantity: same idea for the FOT price view
	CASE
		WHEN o.PremiumFlag = 1 THEN
			(CASE WHEN ISNULL(o.FOT_CC,0) = 0 THEN 0 ELSE o.FlatQTY END)
			+ (CASE WHEN ISNULL(o.FOT_NCC,0) = 0 THEN 0 ELSE o.BasisQTY END)
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
--and ItemKey = '42-42'
--order by OrderCreateTime desc

)

/* Article map for the Last Qty Steps tail (same mapping as dimItemsSimple / factLastQtySteps #am) */
,LQ_ArticleMap as (
  SELECT cast(CONVERT(INT, CONVERT(VARCHAR,M.QOD_MOTSR)) as varchar)+ '-' + cast(CONVERT(INT, CONVERT(VARCHAR,M.QOD_MOTSR)) as varchar) AS ItemKey
        ,A.ART_DESC AS Article
  FROM MOTSRIM M
  JOIN (SELECT * FROM [dbo].[TRGOM_ANGLIT]
        WHERE SOG_PRIT_SOCN_BNQ_TM_SH_CHBRH = NCHAR(1508)) T
       ON M.QOD_MOTSR = T.QOD
  JOIN (SELECT distinct [TARO_ANGLIT_2_ART],[ART_DESC] FROM [dbo].[TRGOM_ANGLIT]
        WHERE SOG_PRIT_SOCN_BNQ_TM_SH_CHBRH = NCHAR(1508) and ART_DESC<>'' and TARO_ANGLIT_3_PNL_HCNSH = 8052) A
       ON A.TARO_ANGLIT_2_ART = T.TARO_ANGLIT_2_ART
)

/* LQ_RunQtyBefore*: priced units sold in the same Article AFTER this order
   (newest first: OrderCreateTime DESC, OrderID DESC), i.e. the tail position this
   order starts at. NULL for rows outside the tail (unpriced, or no Article).
   Three views, each with its OWN tail: LQ_RunQtyBefore = Order view (PricedQuantity),
   ...CF = CIF view (PricedQuantityCF), ...FOT = FOT view (PricedQuantityFOT).
   A row with no price in a view (blank/zero) is skipped in that view: it takes no tail
   position and adds nothing to the average. Drives the Last Qty Steps (Orders) measures;
   same ordering as factLastQtySteps. */
SELECT ob.*,
       CASE WHEN am.Article IS NOT NULL AND ISNULL(ob.PricedQuantity,0) > 0 AND ob.LineTotalFlatValueUSD IS NOT NULL THEN
            ISNULL(SUM(ob.PricedQuantity)
                   OVER (PARTITION BY am.Article,
                                      CASE WHEN ISNULL(ob.PricedQuantity,0) > 0 AND ob.LineTotalFlatValueUSD IS NOT NULL THEN 1 ELSE 0 END
                         ORDER BY ob.OrderCreateTime DESC, ob.OrderID DESC
                         ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0)
       END AS LQ_RunQtyBefore,
       CASE WHEN am.Article IS NOT NULL AND ISNULL(ob.PricedQuantityCF,0) > 0 AND ob.LineTotalFlatValueCFFUSD IS NOT NULL THEN
            ISNULL(SUM(ob.PricedQuantityCF)
                   OVER (PARTITION BY am.Article,
                                      CASE WHEN ISNULL(ob.PricedQuantityCF,0) > 0 AND ob.LineTotalFlatValueCFFUSD IS NOT NULL THEN 1 ELSE 0 END
                         ORDER BY ob.OrderCreateTime DESC, ob.OrderID DESC
                         ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0)
       END AS LQ_RunQtyBeforeCF,
       CASE WHEN am.Article IS NOT NULL AND ISNULL(ob.PricedQuantityFOT,0) > 0 AND ob.LineTotalFlatValueFOTUSD IS NOT NULL THEN
            ISNULL(SUM(ob.PricedQuantityFOT)
                   OVER (PARTITION BY am.Article,
                                      CASE WHEN ISNULL(ob.PricedQuantityFOT,0) > 0 AND ob.LineTotalFlatValueFOTUSD IS NOT NULL THEN 1 ELSE 0 END
                         ORDER BY ob.OrderCreateTime DESC, ob.OrderID DESC
                         ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0)
       END AS LQ_RunQtyBeforeFOT
FROM ordersBase ob
LEFT JOIN LQ_ArticleMap am ON am.ItemKey = ob.ItemKey
GO
SET STATISTICS IO, TIME OFF;
GO
