----------25-05-14 Purchases

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

SMPE as (
select * 
--,ROW_NUMBER() OVER (PARTITION BY SMPE_ProductID,SMPE_Year,SMPE_Month ORDER BY SMPE_CreateDate DESC) as rownum
,ROW_NUMBER() OVER (
 PARTITION BY SMPE_ProductID, SMPE_Year, SMPE_Month
    ORDER BY 
        CASE 
            WHEN SMPE_TempPrice <> 0 THEN SMPE_CreateDate 
            ELSE NULL 
        END DESC
           ) as rownum
from [dbo].[StockMarketProductEOM]
),

CurrencyConvertion as (
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
),

Purchases as (

--------------------הזמנות רכש------------------
SELECT 

'1' as 'EntityID'
        ,CAST(HZ.MSPR_HZMNH AS VARCHAR) AS 'OrderID'
		,1 AS 'OrderLineNumber'
		,'Orders' AS 'DocName'
		,CAST(CONVERT(INT, CONVERT(VARCHAR,HZ.QOD_MQBL))as varchar) AS 'AccountKey' -- customer from invoices in case you need just invoices 
		,CAST(CONVERT(INT, CONVERT(VARCHAR,HZ.QOD_SHOLCH))as varchar) AS 'SupplierKey'
		,M.SHM_MOBIL AS 'Transport Type'
		,SUBSTRING(HZ.T_ASPQH,1,4) + '-' + SUBSTRING(HZ.T_ASPQH,5,2) + '-' + SUBSTRING(HZ.T_ASPQH,7,2) as 'Supply Date'
        ,SUBSTRING(HZ.T_HZMNH,1,4) + '-' + SUBSTRING(HZ.T_HZMNH,5,2) + '-' + SUBSTRING(HZ.T_HZMNH,7,2) as 'Order Date'
		,cast(CONVERT(INT, CONVERT(VARCHAR,HZ.AISH_MCIROT)) as varchar) AS 'AgentKey' 
		,cast(CONVERT(INT, CONVERT(VARCHAR,HZ.MOTSR_MOZMN)) as varchar)+ '-' + cast(CONVERT(INT, CONVERT(VARCHAR,HZ.MOTSR_MOZMN)) as varchar) AS 'ItemKey'
		,Case when HZ.OrderStatus=1 Then 1 else 0 end as 'IsOpen'
		,case when convert(smalldatetime, SUBSTRING(HZ.T_ASPQH,1,4) + '-' + SUBSTRING(HZ.T_ASPQH,5,2) + '-' + SUBSTRING(HZ.T_ASPQH,7,2)) > cast(getdate() as date) then 1 else 0 end as 'IsFuture'
		,case when  convert(smalldatetime, SUBSTRING(HZ.T_ASPQH,1,4) + '-' + SUBSTRING(HZ.T_ASPQH,5,2) + '-' + SUBSTRING(HZ.T_ASPQH,7,2)) < cast(getdate() as date) AND  HZ.OrderStatus=1 THEN 1 ELSE 0 END AS 'IsLate'
		--,CAST('1' as varchar) + CAST(CONVERT(INT, CONVERT(VARCHAR, i.BRANCH)) as varchar) AS 'BranchKey'
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
		--,HZ.CMOT_SHSOPQH AS 'Supplied Quantity'
		,HZ.CMOT_MOZMNT - CMOT_SHSOPQH AS 'Balance'
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
		     THEN CAST((HZ.MCHIR_ICH)*(HZ.CMOT_MOZMNT - CMOT_SHSOPQH) AS decimal (12,4))
	         ELSE CAST(HZ.MCHIR_ICH*(HZ.CMOT_MOZMNT - CMOT_SHSOPQH)*(1/SM.NEW_SHER) AS decimal (12,4))
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
		 ,CASE
	         WHEN HZ.MTBE = '$'
		     THEN CAST((HZ.MCHIR_ICH*SM.NEW_SHER)*(HZ.CMOT_MOZMNT - CMOT_SHSOPQH) AS decimal (12,4))
	         ELSE CAST(HZ.MCHIR_ICH*(HZ.CMOT_MOZMNT - CMOT_SHSOPQH) AS decimal (12,4))
         END AS 'LineTotalBalanceNIS'

		 
		 --,Op.CFFlat AS 'UnitPriceCF'
		 --,Op.CFFlat*OriginalQty AS 'LineTotalCF_USD'
		 ,OP.CFFlat*DollarRate*OriginalQty AS 'LineTotalCF_NIS'
		 --,OP.FotPrice AS 'UnitPriceFOT'
		 --,Op.FotPrice*OriginalQty AS 'LineTotalFOT_USD'
		 ,OP.FotPrice*DollarRate*OriginalQty AS 'LineTotalFOT_NIS'
		 --,CFPremium AS 'UnitPricePremium'
		 --,Op.CFPremium*OriginalQty AS 'LineTotalPremium_USD'
		 --,OP.CFPremium*DollarRate*OriginalQty AS 'LineTotalPremium_NIS'
		 --
		 ,Op.CFFlat*Op.Balance AS 'LineTotalBalanceCF_USD_'
		 ,OP.CFFlat*DollarRate*Op.Balance AS 'LineTotalBalanceCF_NIS'
		 ,Op.FotPrice*Balance AS 'LineTotalBalanceFOT_USD_'
		 ,OP.FotPrice*DollarRate*Op.Balance AS 'LineTotalBalanceFOT_NIS'
		 ,Op.CFPremium*Op.Balance AS 'LineTotalBalancePremium_USD'
		 ,OP.CFPremium*DollarRate*Op.Balance AS 'LineTotalBalancePremium_NIS'

		 ,Case
			WHEN CFPremium <>0
				THEN 1
			ELSE 0
		END AS 'PremiumFlag'
		--,null AS 'LineTotalNetVAT'
		--,null AS 'LineTotalNetVAT_USD'
		--,CONVERT(INT, LEFT(CONVERT(VARCHAR, CONVERT(DATETIME, (HZ.T_ASPQH + 46283040) / 1440.0), 112), 6)) AS YearMonth
		,CONVERT(INT, CONVERT(VARCHAR,SUBSTRING(HZ.T_ASPQH,1,4) + SUBSTRING(HZ.T_ASPQH,5,2))) AS YearMonth
		--,case when (i.DEBIT='C')  then round(ii.IVCOST  ,2)*-1 else round(ii.IVCOST  ,2) end  as 'LineTotalNet'  -- Without Exchange
		--,case when (i.DEBIT='C')  then round(ii.IVCOST  ,2)*-1*ii.IEXCHANGE else round(ii.IVCOST  ,2)*ii.IEXCHANGE end  as 'LineTotalNet' --always exchange
		--,case when (i.DEBIT='C')  then round(ii.QPRICE  ,2)*-1*ii.IEXCHANGE else round(ii.QPRICE  ,2)*ii.IEXCHANGE end  as 'LineTotalNet' -- the invoice items price without the total invoice discount
		----
		,CAST(HZ.OrderStatus AS varchar) AS 'סטטוס'
		,NULL AS 'OrderLineStatus'
		,OP.ActionType AS 'TransactionType'
		--,0 as toc
		--,0 as  tipc
		--,0 as tcc
		--,0 as tc
		,t.PO_Family
		,Null AS 'Basis'
		,1 AS 'Flat'
		,null as 'BasisQ'
		,null as 'FlatQ'
		,null as StockExchangeMonth
		,null as PricedContractsAvgPrice
		,null as PF_GeneralFactor
		,null as POL_PremiumPrice
		,null as POL_UnitPrice
		,null as POL_StockMarketTempPrice
		,null as POL_StockMarketPrice
		,null as POL_FinalPrice
		,null as SMP_Symbol
		,null as SMPE_TempPrice
		,null as CFFlat
		,null as CFFLAT_CC
		,null as CFFLAT_NCC
		,null as FotPrice
		,null as FOT_CC
		,null as FOT_NCC
		,null as OriginalQty
		,null as MarketTempPrice
		,null as FinalPriceClosedContract
		,null as ShipDesc
		,null as Loading_Port
		,null as Arrival_From
		,null as Arrival_To
		,null as OriginCountry
		,null as ActualETA
		,null as ActualETB
		,null as PurchaseOrderStatus
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
LEFT JOIN MOBILIM M ON HZ.QOD_MOBIL=M.MS_MOBIL
Left Join OrderPrices OP
	ON op.OrderID = Hz.MSPR_HZMNH
LEFT JOIN PurchaseOrderTypes t
	ON t.PO_TypeID = OP.ActionType
WHERE Cast(SUBSTRING(T_HZMNH,1,4) as int) >=2018 and Cast(SUBSTRING(T_HZMNH,1,4) as int) <= Year(Getdate())
--AND HZ.OrderStatus<>3
AND HZ.ActionType IN(2,10)



UNION ALL


--------------------הזמנות רכש - מערכת חדשה------------------
SELECT 

'1' as 'EntityID'
        ,PO.PO_OrderID  AS 'OrderID'
		,POL.POL_LineID AS 'OrderLineNumber'
		,'Orders-New' AS 'DocName'
		,CAST(CONVERT(INT, CONVERT(VARCHAR,PO.PO_CustomerID))as varchar) AS 'AccountKey' -- customer from invoices in case you need just invoices 
		,CAST(CONVERT(INT, CONVERT(VARCHAR,PO.PO_SuplierID))as varchar) AS 'SupplierKey'
		,NULL AS 'Transport Type'
		,CAST(POL.POL_SupplyDateFrom AS date) AS 'Supply Date'
        ,CAST(PO.PO_OrderCreateDate AS date) as 'Order Date'
		,cast(CONVERT(INT, CONVERT(VARCHAR,PO.PO_SalesID)) as varchar) AS 'AgentKey' 
		,cast(CONVERT(INT, CONVERT(VARCHAR,POL.POL_ProductID)) as varchar)+ '-' + cast(CONVERT(INT, CONVERT(VARCHAR,POL.POL_ProductID)) as varchar) AS 'ItemKey'
		,CASE
			WHEN pol.POL_OrderLineStatus >= 40 AND pol.POL_OrderLineStatus <= 60 
				THEN 0
			ELSE 1
		END AS 'IsOpen'
		,case when CAST(POL.POL_SupplyDateFrom AS date) > cast(getdate() as date) then 1 else 0 end as 'IsFuture'
		,case when CAST(POL.POL_SupplyDateUntil AS date) < cast(getdate() as date) AND  PO.PO_OrderStatus=1 THEN 1 ELSE 0 END AS 'IsLate'
		,null AS 'DiscountPercent'
		,NULL AS 'LineTotalDiscount'
		,CASE
	         WHEN POL_Currency = '$' THEN POL_UnitPrice
			 WHEN POL_Currency = 'Eur' THEN POL_UnitPrice *( SM.NEW_SHEREURO/SM.NEW_SHER)
		   --THEN CAST(HZ.MCHIR_ICH AS decimal (12,4))
	         ELSE POL_UnitPrice *(1/CASE WHEN SM.NEW_SHER <> 0 THEN SM.NEW_SHER ELSE 1 END)
         END AS 'UnitGrossPrice'
		,CASE
	         WHEN POL_Currency = '$' THEN POL_FinalPrice
			 WHEN POL_Currency = 'Eur' THEN POL_FinalPrice *( SM.NEW_SHEREURO/SM.NEW_SHER)
		   --THEN CAST(HZ.MCHIR_ICH AS decimal (12,4))
	         ELSE POL_FinalPrice *(1/CASE WHEN SM.NEW_SHER <> 0 THEN SM.NEW_SHER ELSE 1 END)
         END AS 'UnitNetPriceUSD'
		,CASE 
			WHEN POB.FinalWeightReceived = 0
				THEN Case 
					WHEN POB.OriginalWeightOrderd <> 0
						THEN POB.OriginalWeightOrderd
					ELSE POL.POL_QuantityOrdered
				END
			ELSE POL_FinalWeightReceived
		END AS 'Quantity'
		,POB.Balance AS 'Balance'
		,NULL AS 'LineTotalCost'
		,CASE
	         WHEN POL_Currency = '$' THEN POL_TotalPrice
			 WHEN POL_Currency = 'Eur' THEN POL_TotalPrice *( SM.NEW_SHEREURO/SM.NEW_SHER)
	         ELSE POL_TotalPrice*(1/CASE WHEN SM.NEW_SHER <> 0 THEN SM.NEW_SHER ELSE 1 END)
         END AS 'LineTotalNetUSD'
		,CASE
	         WHEN POL_Currency = '$' THEN POB.Balance * POL.POL_FinalPrice
		     --THEN (POL.POL_OriginalWeightOrderd - POB.TotalSupplied) * POL.POL_FinalPrice 
			 WHEN POL_Currency = 'Eur' THEN POB.Balance*(POL.POL_FinalPrice *( SM.NEW_SHEREURO/SM.NEW_SHER))
	         --ELSE (POL.POL_OriginalWeightOrderd - POB.TotalSupplied)*POL.POL_FinalPrice*(1/CASE WHEN SM.NEW_SHER <> 0 THEN SM.NEW_SHER ELSE 1 END)
			 ELSE POB.Balance*POL.POL_FinalPrice*(1/CASE WHEN SM.NEW_SHER <> 0 THEN SM.NEW_SHER ELSE 1 END)
         END AS 'LineTotalBalanceUSD'	
		--, (round(ii.IVCOST, 2)/ii.IEXCHANGE) *fnc.EXCHANGE2   *(CASE WHEN (i.DEBIT = 'C') THEN - 1 ELSE 1 END) AS 'LineTotalNet_USD'
		,CASE
	         WHEN POL_Currency = '$' THEN POL.POL_FinalPrice*SM.NEW_SHER
			 WHEN POL_Currency = 'Eur' THEN POL.POL_FinalPrice*SM.NEW_SHEREURO
	         ELSE POL.POL_FinalPrice
         END AS 'UnitNetPriceNIS'
	    ,CASE
	         WHEN POL_Currency = '$' THEN POL_TotalPrice*SM.NEW_SHER 
			 WHEN POL_Currency = 'Eur' THEN POL_TotalPrice*SM.NEW_SHEREURO
	         ELSE POL_TotalPrice
         END AS 'LineTotalNetNIS'
		 /*,CASE
	         WHEN POL_Currency = '$'
		     THEN POL.POL_FinalPrice*SM.NEW_SHER*(POL.POL_OriginalWeightOrderd - POL.POL_FinalWeightReceived)
	         ELSE POL.POL_FinalPrice*(POL.POL_OriginalWeightOrderd - POL.POL_FinalWeightReceived)
         END AS 'LineTotalBalanceNIS'*/
		 ,CASE
	         WHEN POL_Currency = '$' THEN POL.POL_FinalPrice*SM.NEW_SHER*POB.Balance--(POL.POL_OriginalWeightOrderd - POB.TotalSupplied)
			 WHEN POL_Currency = 'Eur' THEN POL.POL_FinalPrice*SM.NEW_SHEREURO*POB.Balance
	         ELSE POL.POL_FinalPrice*POB.Balance--(POL.POL_OriginalWeightOrderd - POB.TotalSupplied)
         END AS 'LineTotalBalanceNIS'
		 
		 --,Op.CFFlat AS 'UnitPriceCF'
		 --,Op.CFFlat*OriginalQty AS 'LineTotalCF_USD'
		 ,OP.CFFlat*DollarRate*OriginalQty AS 'LineTotalCF_NIS'
		 --,OP.FotPrice AS 'UnitPriceFOT'
		 --Op.FotPrice*OriginalQty AS 'LineTotalFOT_USD'
		 ,OP.FotPrice*DollarRate*OriginalQty AS 'LineTotalFOT_NIS'
		 --,CFPremium AS 'UnitPricePremium'
		 --,Op.CFPremium*OriginalQty AS 'LineTotalPremium_USD'
		 --,OP.CFPremium*DollarRate*OriginalQty AS 'LineTotalPremium_NIS'
		 
		 ,Op.CFFlat*Op.Balance AS 'LineTotalBalanceCF_USD_'		-----------------
		 ,OP.CFFlat*DollarRate*Op.Balance AS 'LineTotalBalanceCF_NIS'
		 ,Op.FotPrice*Op.Balance AS 'LineTotalBalanceFOT_USD_'   ---------------
		 ,OP.FotPrice*DollarRate*Op.Balance AS 'LineTotalBalanceFOT_NIS'
		 ,Op.CFPremium*Op.Balance AS 'LineTotalBalancePremium_USD'
		 ,OP.CFPremium*DollarRate*Op.Balance AS 'LineTotalBalancePremium_NIS'


		 ,Case
			WHEN CFPremium <>0
				THEN 1
			ELSE 0
		END AS 'PremiumFlag' 
		,CONCAT(YEAR(PO.PO_OrderCreateDate),FORMAT(PO.PO_OrderCreateDate,'MM')) AS YearMonth
		,POL.POL_OrderLineStatus as 'OrderStatus'
		,CASE WHEN POL.POL_OrderLineStatus BETWEEN 10 AND 29 THEN 'Open' 
		      --WHEN POL.POL_OrderLineStatus BETWEEN 30 AND 39 THEN 'Canceled'
			  ELSE 'Closed'
			  END AS 'OrderLineStatus'
		,OP.ActionType AS 'TransactionType'
		--,POL.POL_TotalOpenContract as toc
		--,POL.POL_TotalInProcessContract tipc
		--,POL.POL_TotalClosedContract tcc
		--,POL.POL_totalContracts tc
		,t.PO_Family
		,CASE 
			WHEN POL.POL_OrderLineStatus NOT IN (10,15)
				THEN NULL
			WHEN t.PO_Family = 2
				THEN CASE
						WHEN POL.POL_totalContracts <> 0 
							THEN CAST((CAST(POL.POL_TotalOpenContract as decimal(18,2))+CAST(POL.POL_TotalInProcessContract as decimal(18,2)))/CAST(POL.POL_totalContracts as decimal(18,2)) AS decimal(18,2))--*POL.POL_QuantityOrdered ---- כמות שלא תומחרה =כמות הזמנה כפול(סך הכל חוזים אפשריים - חוזים שנחתמו חלקי סך כל החוזים האפשריים
						ELSE 1--POL.POL_QuantityOrdered
					END
			WHEN t.PO_Family = 1
				Then Null
			ELSE NULL
		END AS 'Basis'
		,CASE 
			WHEN POL.POL_OrderLineStatus NOT IN (10,15)
				Then 1--POL.POL_QuantityOrdered
			WHEN t.PO_Family = 2
				THEN CASE
						WHEN POL.POL_totalContracts <> 0
							THEN CAST(CAST(POL.POL_TotalClosedContract as decimal(18,2))/CAST(POL.POL_totalContracts as decimal(18,2)) as decimal(18,2))---*POL.POL_QuantityOrdered ---- כמות שתומחרה =כמות הזמנה כפול חוזים שנחתמו חלקי סך כל החוזים האפשריים
						ELSE NULL
					END
			WHEN t.PO_Family = 1
				Then 1 ----POL.POL_QuantityOrdered
			ELSE 1 ----POL.POL_QuantityOrdered
		END AS 'Flat'
----------------------
		,CASE 
			WHEN POL.POL_OrderLineStatus NOT IN (10,15)
				THEN NULL
			WHEN t.PO_Family = 2
				THEN CASE
						WHEN POL.POL_totalContracts <> 0 
							THEN (POL.POL_TotalOpenContract)* pr.QtyTonePerContract--*POL.POL_QuantityOrdered ---- כמות שלא תומחרה =כמות הזמנה כפול(סך הכל חוזים אפשריים - חוזים שנחתמו חלקי סך כל החוזים האפשריים   THEN (POL.POL_TotalOpenContract+POL.POL_TotalInProcessContract)* pr.QtyTonePerContract
						ELSE POL.POL_TotalClosedContract * pr.QtyTonePerContract--POL.POL_QuantityOrdered
					END
			WHEN t.PO_Family = 1
				Then Null
			ELSE NULL
		END AS 'BasisQ'
		,CASE 
			WHEN POL.POL_OrderLineStatus NOT IN (10,15)
				Then POB.Balance--POL.POL_TotalOpenContract * pr.QtyTonePerContract
			WHEN t.PO_Family = 2
				THEN CASE
						WHEN POL.POL_totalContracts <> 0
							--THEN CAST(CAST(POL.POL_TotalClosedContract as decimal(18,2))/CAST(POL.POL_totalContracts as decimal(18,2)) as decimal(18,2))---*POL.POL_QuantityOrdered ---- כמות שתומחרה =כמות הזמנה כפול חוזים שנחתמו חלקי סך כל החוזים האפשריים
						THEN POL.POL_TotalClosedContract * pr.QtyTonePerContract
						ELSE NULL
					END
			WHEN t.PO_Family = 1
				Then POB.Balance --POL.POL_totalContracts * pr.QtyTonePerContract----POL.POL_QuantityOrdered --- פה לקוחת כמיות FLAT של כלל ההזמנה
			ELSE  POB.Balance--POL.POL_totalContracts ----POL.POL_QuantityOrdered
		END AS 'FlatQ'
-------------------

		,POL_StockExchangeDate											as StockExchangeMonth
		,(POL_StockMarketPrice+POL_PremiumPrice)*sf.PF_GeneralFactor	as PricedContractsAvgPrice
		,sf.PF_GeneralFactor
		,POL_PremiumPrice
		,POL_UnitPrice
		,POL_StockMarketTempPrice
		,POL_StockMarketPrice
		,POL_FinalPrice
		,smp.SMP_Symbol
		,smpe.SMPE_TempPrice
		,OP.CFFlat
		,OP.CFFLAT_CC
		,OP.CFFLAT_NCC
		,OP.FotPrice
		,OP.FOT_CC
		,OP.FOT_NCC
		,OP.OriginalQty 
		,POL.POL_StockMarketTempPriceForNoneClosed  as MarketTempPrice
		,POL_FinalPriceForClosedContract			as FinalPriceClosedContract
		,sl.ShipDesc
		,sa.SA_ShipLoadingPort						as Loading_Port
		,sa.SA_DateForLoadingFrom					as Arrival_From
		,sa.SA_DateForLoadingUntil					as Arrival_To
		,cl.CO_CountryNameENG						as OriginCountry
		,sa.SA_ActualETA							as ActualETA
		,sa.SA_ActualETB							as ActualETB
		,pos.PO_StatusDesc							as PurchaseOrderStatus
FROM PurchaseOrder PO
LEFT JOIN PurchaseOrderLines POL ON PO.PO_OrderID=POL.POL_OrderID
LEFT JOIN CurrencyConvertion SM 
	ON SM.TARIKH=CAST(PO.PO_OrderCreateDate AS date)
Left Join viewPurchaseOrderBalance POB
	ON POL.POL_OrderID = POB.POID AND POL.POL_LineID = POB.POLineID
Left Join OrderPrices OP
	ON op.OrderID = PO.PO_OrderID AND op.OrderIDLine = POL.POL_LineID
LEFT JOIN PurchaseOrderTypes t
	ON t.PO_TypeID = OP.ActionType
left join StockMarketProduct smp
	on POL.POL_ProductID = smp.SMP_ProductID AND MONTH(POL.POL_StockExchangeDate) = smp.SMP_MonthID
left join MOTSRIM PR
	on PR.QOD_MOTSR = POL.POL_ProductID
left join ProductStockMarketFactors sf
	on pol.POL_ProductID = sf.PF_ProductID and POL.POL_FactorCode = sf.PF_FactorID 
left join (select * from SMPE where  rownum = 1) smpe
	on POL.POL_ProductID = smpe.SMPE_ProductID AND YEAR(POL.POL_StockExchangeDate) = smpe.SMPE_Year AND MONTH(POL.POL_StockExchangeDate) = smpe.SMPE_Month

LEFT JOIN ShipsArrivals sa ON POL.POL_ShipArrivalID = sa.SA_ID
LEFT JOIN ShipList sl ON sa.SA_ShipID = sl.ShipID
LEFT JOIN tblCountryList cl ON sa.SA_CountryOfOrigin = cl.CO_CountryID
LEFT JOIN PurchaseOrderStatus pos ON POL.POL_ContractStage = pos.PO_StatusID

WHERE YEAR(PO.PO_OrderCreateDate)>=2018 and YEAR(PO.PO_OrderCreateDate) <= Year(Getdate())
--AND HZ.OrderStatus<>3
--AND HZ.ActionType IN(2,10)
and POL.POL_OrderLineStatus NOT BETWEEN 30 AND 39 --- לבטל לפני 

--and POL.POL_FinalWeightReceived <>0
 
--and PO.PO_OrderID = 2000360

)

SELECT	P.*
		--Basis*Balance AS BasisQTY,
		--Flat*Balance AS FlatQTY,
		,p.BasisQ																						AS BasisQTY
		,p.FlatQ																						AS FlatQTY
		--,(PF_GeneralFactor*(SMPE_TempPrice+POL_PremiumPrice))*p.BasisQ									AS LineTotalFlatValueBalanceUSD
		--,(PF_GeneralFactor*(POL_StockMarketPrice+POL_PremiumPrice))*p.FlatQ								AS LineTotalFlatBalanceUSD
		    -- New computed field: if PremiumFlag = 1 then multiply FlatQTY by FinalPriceClosedContract, otherwise use LineTotalNetUSD
    ,CASE 
        WHEN p.PremiumFlag = 1 THEN p.FlatQ * p.FinalPriceClosedContract
        ELSE p.LineTotalBalanceUSD
    END AS LineTotalFlatBalanceUSD,

	------
    CASE 
        WHEN p.PremiumFlag = 1 THEN p.FlatQ * p.FOT_CC
        ELSE LineTotalBalanceFOT_USD_
    END AS LineTotalBalanceFOT_USD,

	CASE 
        WHEN p.PremiumFlag = 1 THEN p.FlatQ  * p.CFFLAT_CC
        ELSE LineTotalBalanceCF_USD_
    END AS LineTotalBalanceCF_USD,
	---------
    -- New computed field: combine two calculations based on PremiumFlag
    (
      CASE 
          WHEN p.PremiumFlag = 1 THEN p.BasisQ * p.MarketTempPrice
          ELSE 0
      END
      +
      CASE 
          WHEN p.PremiumFlag = 1 THEN p.FlatQ * p.FinalPriceClosedContract 
          ELSE p.LineTotalNetUSD * (p.FlatQ/p.OriginalQty)					-------------!!!!!!!******
      END
    ) AS LineTotalFlatValueUSD,

    -- New computed field: choose CF unit price based on PremiumFlag
    CASE 
        WHEN p.PremiumFlag = 1 THEN p.CFFLAT_CC 
        ELSE p.CFFlat 
    END AS UnitPriceCF,

    -- New computed field: calculate CF total in USD
    CASE 
        WHEN p.PremiumFlag = 1 THEN p.CFFLAT_CC * p.FlatQ 
        ELSE p.CFFlat * p.OriginalQty 
    END AS LineTotalCF_USD,

    -- New computed field: choose FOT unit price based on PremiumFlag
    CASE 
        WHEN p.PremiumFlag = 1 THEN p.FOT_CC 
        ELSE p.FotPrice 
    END AS UnitPriceFOT,

    -- New computed field: calculate FOT total in USD
    CASE 
        WHEN p.PremiumFlag = 1 THEN p.FOT_CC * p.FlatQ 
        ELSE p.FotPrice * p.OriginalQty 
    END AS LineTotalFOT_USD,

    -- New computed field: use BasisQTY to compute flat value balance in USD
    p.MarketTempPrice * p.BasisQ AS LineTotalFlatValueBalanceUSD,

    -- New computed field: MarketTempPriceCFF_USD (only when PremiumFlag = 1)
    CASE 
        WHEN p.PremiumFlag = 1 THEN p.CFFLAT_NCC 
        ELSE NULL 
    END AS MarketTempPriceCFF_USD,

    -- New computed field: multiply MarketTempPriceCFF_USD by BasisQTY
    CASE 
        WHEN p.PremiumFlag = 1 THEN p.CFFLAT_NCC * p.BasisQ 
        ELSE NULL 
    END AS LineTotalFlatValueBalanceCFFUSD,

    -- New computed field: MarketTempPriceFOT_USD (only when PremiumFlag = 1)
    CASE 
        WHEN p.PremiumFlag = 1 THEN p.FOT_NCC 
        ELSE NULL 
    END AS MarketTempPriceFOT_USD,

    -- New computed field: multiply MarketTempPriceFOT_USD by BasisQTY
    CASE 
        WHEN p.PremiumFlag = 1 THEN p.FOT_NCC * p.BasisQ 
        ELSE NULL 
    END AS LineTotalFlatValueBalanceFOTUSD,

    -- New computed field: combined calculation for CFF total value in USD
    CASE 
        WHEN p.PremiumFlag = 1 THEN (p.CFFLAT_NCC * p.BasisQ) + (p.CFFLAT_CC * p.FlatQ)
        ELSE p.CFFlat * p.OriginalQty * (p.FlatQ/p.OriginalQty)					-------------!!!!!!!****** 
    END AS LineTotalFlatValueCFFUSD,

    -- New computed field: combined calculation for FOT total value in USD
    CASE 
        WHEN p.PremiumFlag = 1 THEN (p.FOT_NCC * p.BasisQ) + (p.FOT_CC * p.FlatQ)
        ELSE p.FotPrice * p.OriginalQty * (p.FlatQ/p.OriginalQty)					-------------!!!!!!!****** 
    END AS LineTotalFlatValueFOTUSD

		,GETDATE()													AS RowInsertDatetime
FROM Purchases p
--where OrderID IN ('2000416','2000430','2000369','2000274','2000354','2000344','2000343','2000367','2000360') --'2000344'--'2000274'
--where OrderID IN ('2000484')--,'2000482')

where (p.OriginalQty <> 0 OR p.OriginalQty IS NULL)
--and OrderID = 2000490