WITH CurrencyConvertion AS (
    SELECT *,
           CASE
               WHEN sher = 0
                    THEN FIRST_VALUE(sher) OVER (PARTITION BY value_partition ORDER BY Tarikh)
               ELSE sher
           END AS new_sher,
           CASE
               WHEN SHER_EURO = 0
                    THEN FIRST_VALUE(SHER_EURO) OVER (PARTITION BY value_partitionEuro ORDER BY Tarikh)
               ELSE SHER_EURO
           END AS new_sherEuro
    FROM (
        SELECT *,
               SUM(CASE WHEN sher = 0 THEN 0 ELSE 1 END) OVER (ORDER BY tarikh) AS value_partition,
               SUM(CASE WHEN SHER_EURO = 0 THEN 0 ELSE 1 END) OVER (ORDER BY tarikh) AS value_partitionEuro
        FROM SHERI_MTBE
    ) m
),
LastCreation AS (
    SELECT
        OrderId,
        MAX(CreateDate) AS LastCreation
    FROM tblOrderPriceS
    GROUP BY OrderID
),
LastVersion AS (
    SELECT
        o.OrderId,
        MAX(DayVersion) AS LastVersion
    FROM tblOrderPriceS o
    INNER JOIN LastCreation LC
        ON o.OrderID = LC.OrderID
       AND o.CreateDate = LC.LastCreation
    GROUP BY o.OrderID
),
OrderPrices AS (
    SELECT
        O.*
    FROM tblOrderPriceS o
    INNER JOIN LastCreation LC
        ON o.OrderID = LC.OrderID
       AND o.CreateDate = LC.LastCreation
    INNER JOIN LastVersion LV
        ON o.OrderID = LV.OrderID
       AND o.DayVersion = LV.LastVersion
    WHERE 1 = 1
),
totals AS (
    SELECT
        '1' AS EntityID,
        CAST(HS.NOMRTOR AS VARCHAR) AS ExpenseOrderID,
        -- CAST(HZ.MSPR_HZMNH AS VARCHAR) AS OrderID
        CAST(HS.MS_MSMKH_QSHOR AS VARCHAR) AS [OrderID], -- PurchaseOrderID
        -- CONCAT(CAST(POL.POL_OrderID AS VARCHAR(20)),CAST(POL.POL_LineID AS VARCHAR(10)))
        CONCAT(
            CAST(CONVERT(BIGINT, POL.POL_OrderID) AS VARCHAR(20)),
            CAST(POL.POL_LineID AS VARCHAR(10))
        ) AS PurchaseOrderID,
        HS.SHORH AS OrderLineNumber,
        'Order Expenses' AS DocName,
        -- CAST(CONVERT(INT, CONVERT(VARCHAR,HS.QOD_GORM_MQBL))as varchar) AS 'AccountKey'
        CAST(CONVERT(INT, CONVERT(VARCHAR, HC.QOD_SPQ)) AS VARCHAR) AS SupplierKey,
        CAST(
            MAX(
                CASE
                    WHEN HS.QOD_SHROT = 0
                         THEN CONVERT(INT, CONVERT(VARCHAR, HC.QOD_SPQ))
                END
            ) OVER (PARTITION BY HS.MS_MSMKH_QSHOR) AS VARCHAR
        ) AS OrderSupplierKey,
        CAST(CONVERT(INT, CONVERT(VARCHAR, hz.QOD_MQBL)) AS VARCHAR) AS SourceKey,
        -- NULL AS 'Transport Type'
        -- Supply / Order dates – commented out
        /*
        SUBSTRING(HZ.T_ASPQH,1,4) + '-' + SUBSTRING(HZ.T_ASPQH,5,2) + '-' + SUBSTRING(HZ.T_ASPQH,7,2) as 'Supply Date',
        SUBSTRING(HZ.T_HZMNH,1,4) + '-' + SUBSTRING(HZ.T_HZMNH,5,2) + '-' + SUBSTRING(HZ.T_HZMNH,7,2) as 'Order Date',
        SUBSTRING(HS.T_ERKH,1,4) + '-' + SUBSTRING(HS.T_ERKH,5,2) + '-' + SUBSTRING(HS.T_ERKH,7,2) as 'Value Date',
        SUBSTRING(HS.T_BITSOE,1,4) + '-' + SUBSTRING(HS.T_BITSOE,5,2) + '-' + SUBSTRING(HS.T_BITSOE,7,2) as 'Value Date',
        */
        -- grab the header-string once per order, convert to date, then expose on every row
        CASE
            WHEN HS.MS_MSMKH_QSHOR = 0
                 THEN CONVERT(DATE, HC.T_ERKH, 112)
            ELSE
                -- pull the header-row date per order (using corrected key)
                MAX(
                    CASE
                        WHEN HS.QOD_SHROT = 0
                             THEN CONVERT(DATE, HC.T_ERKH, 112)
                    END
                ) OVER (
                    PARTITION BY
                        CASE
                            WHEN HS.MS_MSMKH_QSHOR = 0
                                 THEN HZ.MSPR_HZMNH
                            ELSE HS.MS_MSMKH_QSHOR
                        END
                )
        END AS [Value Date],
        SUBSTRING(HS.T_HQLDH, 1, 4) + '-' + SUBSTRING(HS.T_HQLDH, 5, 2) + '-' + SUBSTRING(HS.T_HQLDH, 7, 2) AS EntryDate,
        CAST(CONVERT(INT, CONVERT(VARCHAR, HS.QOD_MOTSR)) AS VARCHAR) + '-' +
        CAST(CONVERT(INT, CONVERT(VARCHAR, HS.QOD_MOTSR)) AS VARCHAR) AS ItemKey_backup,
        CASE
            -- WHEN HST.QOD_SHROT IS NULL THEN cast(... )  -- לפני שינוי של העמסת עלויות
            WHEN HST.QOD_SHROT IS NULL
              OR HS.QOD_SHROT IN (5, 19, 30, 4)
                THEN CAST(CONVERT(INT, CONVERT(VARCHAR, HS.QOD_MOTSR)) AS VARCHAR) + '-' +
                     CAST(CONVERT(INT, CONVERT(VARCHAR, HS.QOD_MOTSR)) AS VARCHAR) -- העמסת עליות הובלה והפרש מחיר על המוצר
            ELSE CAST(CONVERT(INT, CONVERT(VARCHAR, HS.QOD_MOTSR)) AS VARCHAR) + '-' +
                 CAST(CONVERT(INT, CONVERT(VARCHAR, HST.QOD_SHROT)) AS VARCHAR) + 'S'
        END AS ItemKey,
        /*
        CASE
            WHEN HS.MTBE = '$' THEN HS.MCHIR_ICH
            -- THEN CAST(HZ.MCHIR_ICH AS decimal (12,4))
            ELSE CAST(
                     HS.MCHIR_ICH * (1 / CASE WHEN HC.SHER_MTBE = 0 THEN 1 ELSE HC.SHER_MTBE END)
                     AS DECIMAL(12,4)
                 )
        END AS UnitNetPriceUSD
        */
        CASE
            WHEN HST.QOD_SHROT IS NOT NULL THEN
                CASE
                    WHEN HS.MTBE = '$' THEN
                        CAST(HS.MCHIR_ICH / NULLIF(POL.POL_FinalWeightReceived, 0) AS DECIMAL(12,4))
                    WHEN HS.MTBE = 'Eur' THEN
                        CAST(
                            HS.MCHIR_ICH * (SM.new_sherEuro / SM.new_sher) /
                            NULLIF(POL.POL_FinalWeightReceived, 0)
                            AS DECIMAL(12,4)
                        )
                    ELSE
                        CAST(
                            HS.MCHIR_ICH * (1 / NULLIF(HC.SHER_MTBE, 0)) /
                            NULLIF(POL.POL_FinalWeightReceived, 0)
                            AS DECIMAL(12,4)
                        )
                END
            ELSE
                CASE
                    WHEN HS.MTBE = '$' THEN HS.MCHIR_ICH
                    WHEN HS.MTBE = 'Eur' THEN
                        CAST(
                            HS.MCHIR_ICH * (SM.new_sherEuro / SM.new_sher)
                            AS DECIMAL(12,4)
                        )
                    ELSE
                        CAST(
                            HS.MCHIR_ICH * (1 / NULLIF(HC.SHER_MTBE, 0))
                            AS DECIMAL(12,4)
                        )
                END
        END AS UnitNetPriceUSD,
        HS.MTBE,
        HS.CMOT AS Quantity,
        -- POL.POL_FinalWeightReceived as tst
        -- HZ.CMOT_MOZMNT AS 'OrderQuantity1'
        CASE
            WHEN HS.QOD_SHROT = 0 THEN HS.CMOT
            ELSE 0
        END AS OrderQuantity,
        -- ISNULL(HST.QOD_SHROT,999) AS 'PNLKey'
        CASE
            WHEN HST.QOD_SHROT IN (5, 19, 30, 4)
              OR HST.QOD_SHROT IS NULL
                THEN 999
            ELSE HST.QOD_SHROT
        END AS PNLKey,  -- העמסת עליות הובלה על המוצר והפרש מחיר
        -- HS.QOD_SHROT AS 'PNLKey'
        HS.CMOT AS Balance,
        NULL AS LineTotalCost,
        CASE
            WHEN HS.MTBE = '$' THEN HS.MCHIR_ICH * HS.CMOT
            WHEN HS.MTBE = 'Eur' THEN HS.MCHIR_ICH * HS.CMOT * (SM.new_sherEuro / SM.new_sher)
            ELSE HS.MCHIR_ICH * HS.CMOT * (1 / CASE WHEN HC.SHER_MTBE = 0 THEN 1 ELSE HC.SHER_MTBE END)
        END AS LineTotalNetUSD,
        /*
        CASE
            WHEN HZ.MTBE = '$'
                THEN CAST((HS.MCHIR_ICH)*(HS.CMOT - CMOT_SHSOPQH) AS decimal (12,4))
            ELSE CAST(HS.MCHIR_ICH*(HS.CMOT_MOZMNT - CMOT_SHSOPQH)*(1/SM.NEW_SHER) AS decimal (12,4))
        END
        */
        CASE
            WHEN HS.MTBE = '$' THEN HS.MCHIR_ICH * HS.CMOT
            WHEN HS.MTBE = 'Eur' THEN HS.MCHIR_ICH * HS.CMOT * (SM.new_sherEuro / SM.new_sher)
            ELSE HS.MCHIR_ICH * HS.CMOT * (1 / CASE WHEN HC.SHER_MTBE = 0 THEN 1 ELSE HC.SHER_MTBE END)
        END AS LineTotalBalanceUSD,
        -- LineTotalNet_USD comment
        CASE
            WHEN HS.MTBE = '$' THEN HS.MCHIR_ICH * HC.SHER_MTBE
            WHEN HS.MTBE = 'Eur' THEN HS.MCHIR_ICH * SM.new_sherEuro
            ELSE HS.MCHIR_ICH
        END AS UnitNetPriceNIS,
        CASE
            WHEN HS.MTBE = '$' THEN HS.MCHIR_ICH * HC.SHER_MTBE * HS.CMOT
            WHEN HS.MTBE = 'Eur' THEN HS.MCHIR_ICH * HS.CMOT * SM.new_sherEuro
            ELSE HS.MCHIR_ICH * HS.CMOT
        END AS LineTotalNetNIS,
        /*
        CASE
            WHEN HS.MTBE = '$'
                THEN CAST((HS.MCHIR_ICH*SM.NEW_SHER)*(HZ.CMOT_MOZMNT - CMOT_SHSOPQH) AS decimal (12,4))
            ELSE CAST(HS.MCHIR_ICH*(HZ.CMOT_MOZMNT - CMOT_SHSOPQH) AS decimal (12,4))
        END
        */
        CASE
            WHEN HS.MTBE = '$' THEN HS.MCHIR_ICH * HC.SHER_MTBE * HS.CMOT
            WHEN HS.MTBE = 'Eur' THEN HS.MCHIR_ICH * HS.CMOT * SM.new_sherEuro
            ELSE HS.MCHIR_ICH * HS.CMOT
        END AS LineTotalBalanceNIS,
        -- null AS 'LineTotalNetVAT'
        -- null AS 'LineTotalNetVAT_USD'
        -- CONVERT(INT, LEFT(CONVERT(VARCHAR, CONVERT(DATETIME, (HZ.T_ASPQH + 46283040) / 1440.0), 112), 6)) AS YearMonth
        CONVERT(
            INT,
            CONVERT(VARCHAR, SUBSTRING(HS.T_ERKH, 1, 4) + SUBSTRING(HS.T_ERKH, 5, 2))
        ) AS YearMonth,
        -- more commented legacy lines...
        HC.STTOS AS [סטטוס],
        HC.HEROT AS Details,
        HST.SHM_SHROT AS ServiceDetail,
        sl.ShipID,
        CAST(PO.PO_OrderCreateDate AS DATE) AS OrderDate,
        CAST(PO.PO_UpdatedDeliveryDateFrom AS DATE) AS SupplyDate,
        GETDATE() AS RowInsertDatetime,
        CASE
            WHEN POL.POL_OrderID IS NULL THEN 'Other'
            ELSE 'Import'
        END AS ExpenseSource,
        OP.ActionType AS TransactionType,
        CASE
            WHEN HS.QOD_SHROT IN (5, 19, 30) THEN N'סחורה'
            ELSE HC.SOG_MSMKH
        END AS DocType,
        SM.new_sher
		,case 
		when HST.QOD_SHROT IS NULL AND HS.QOD_SHROT = 0 and CAST(CONVERT(BIGINT, POL.POL_OrderID) AS VARCHAR(20)) <> ' '
		then row_number() over 
		(partition by HS.QOD_MOTSR ,CONCAT(CAST(CONVERT(BIGINT, POL.POL_OrderID) AS VARCHAR(20)),CAST(POL.POL_LineID AS VARCHAR(10))) order by CONVERT(INT,
            CONVERT(VARCHAR, SUBSTRING(HS.T_ERKH, 1, 4) + SUBSTRING(HS.T_ERKH, 5, 2))
        ) desc)  end as rn
        -- ,HC.SHER_MTBE
        -- ,HS.MCHIR_ICH
        -- ,(SM.NEW_SHEREURO/SM.NEW_SHER) as tst
    FROM HOTSAOT_COTROT HC
    LEFT JOIN HOTSAOT_SHOROT HS
        ON HC.NOMRTOR = HS.NOMRTOR
    LEFT JOIN PurchaseOrderLines POL
        ON CONCAT(
               CAST(CONVERT(BIGINT, POL.POL_OrderID) AS VARCHAR(20)),
               CAST(POL.POL_LineID AS VARCHAR(10))
           ) = CAST(HS.MS_MSMKH_QSHOR AS VARCHAR(30))
    LEFT JOIN HZMNOT HZ
        ON HS.MS_MSMKH_QSHOR = HZ.MSPR_HZMNH
    LEFT JOIN HOTSAOT_SHROTIM_New HST
        ON HS.QOD_SHROT = HST.QOD_SHROT
    LEFT JOIN ShipsArrivals sa
        ON POL.POL_ShipArrivalID = sa.SA_ID
    LEFT JOIN ShipList sl
        ON sa.SA_ShipID = sl.ShipID
    LEFT JOIN PurchaseOrder PO
        ON POL.POL_OrderID = PO.PO_OrderID
    LEFT JOIN CurrencyConvertion SM
        ON SM.TARIKH = HC.T_ERKH
    LEFT JOIN OrderPrices OP
        ON OP.OrderID = POL_OrderID
       AND OP.OrderIDLine = POL.POL_LineID
    WHERE 1 = 1
      AND CAST(SUBSTRING(HC.T_MSMKH, 1, 4) AS INT) >= 2018
      AND CAST(SUBSTRING(HC.T_MSMKH, 1, 4) AS INT) <= YEAR(GETDATE())
      AND HS.QOD_SHROT NOT IN (14)
    -- AND HZ.ActionType IN(2,10)
    -- AND HC.NOMRTOR = 10921
    -- AND HS.MS_MSMKH_QSHOR = 20005011--20004161
    -- AND HS.NOMRTOR = 10677
    -- AND PNLKey = '999'
)
,main AS (
    SELECT
        TM.QOD_SHOLCH AS DeliveredFrom,
        W.SHM_GORM AS DeliveredFromName,
        ISNULL(b.MS_HZMNH, bb.MS_HZMNH) AS PurchaseOrderID,
		case 
		when ISNULL(b.MS_HZMNH, bb.MS_HZMNH) like '2000%' then 'P'
		else 'E'
		end as Order_Type,
        CONVERT(VARCHAR, TM.QOD_MOTSR) + '-' + CONVERT(VARCHAR, TM.QOD_MOTSR) AS ItemKey,
        SUM(TM.MSHQL_NTO) AS qty_loaned,
        G.QOD_GORM AS DeliveredTo,
        G.SHM_GORM AS DeliveredToName,
        SUBSTRING(TARIKH_MSHLOCH, 1, 4) + '-' + SUBSTRING(TARIKH_MSHLOCH, 5, 2) AS [Date]

    FROM TEODOT_MSHLOCH TM
    LEFT JOIN GORMIM G
        ON TM.QOD_MQBL = G.QOD_GORM    -- customer
    LEFT JOIN GORMIM W
        ON TM.QOD_SHOLCH = W.QOD_GORM  -- supplier
    LEFT JOIN QISHOR_RCSH_LMCIRH a
        ON MS_TEODT_MCIRH = TM.MS_TEODH
    LEFT JOIN QISHOR_T_MSHLOCH_HZMNOT b
        ON a.MS_TEODT_RCSH = b.MS_T_MSHLOCH
    LEFT JOIN CurrencyConvertion SM
        ON SM.TARIKH = TM.TARIKH_MSHLOCH
    LEFT JOIN QISHOR_T_MSHLOCH_HZMNOT bb
        ON bb.MS_T_MSHLOCH = TM.MS_TEODH
    -- LEFT JOIN HOTSAOT_SHOROT HS ON HS.MS_MSMKH_QSHOR = b.MS_HZMNH
    -- LEFT JOIN PurchaseOrderLines POL ON ...
    WHERE 1 = 1
      -- AND TM.QOD_SHOLCH = 1280 -- השאלה ממילובר
      AND TM.MCHIR_ICH = 0
      AND G.AOPI_PEILOT NOT IN ('פחת','אחסון') -- exchange
	 --AND b.MS_HZMNH = --20005011--
      -- AND TM.MS_TEODH = 540996
      -- AND G.QOD_GORM = 146
      --AND TM.QOD_MOTSR = 40
    GROUP BY
        SUBSTRING(TARIKH_MSHLOCH, 1, 4) + '-' + SUBSTRING(TARIKH_MSHLOCH, 5, 2),
        TM.QOD_SHOLCH,
        W.SHM_GORM,
        b.MS_HZMNH,
        CONVERT(VARCHAR, TM.QOD_MOTSR) + '-' + CONVERT(VARCHAR, TM.QOD_MOTSR),
        -- TM.MSHQL_NTO
        G.QOD_GORM,
        G.SHM_GORM,
        -- b.MS_T_MSHLOCH,
        bb.MS_HZMNH
    -- ORDER BY 'Date' DESC, DeliveredFrom DESC
)
,base as (
SELECT DISTINCT
	a.*,
	b.UnitNetPriceUSD
FROM main a
LEFT JOIN (
    SELECT *
    FROM totals
    WHERE PNLKey = '999'
      AND OrderQuantity <> 0
) b
 ON a.PurchaseOrderID = b.PurchaseOrderID
where 1=1 
and Date >= '2024-01'
)
,purchase_orders as (
select 
	DeliveredFrom,
	DeliveredFromName,
	PurchaseOrderID,
	ItemKey,
	sum(qty_loaned) as quantity,
	DeliveredTo,
	DeliveredToName,
	Date as 'Purchase_Date',
	MAX(UnitNetPriceUSD) as max_unit_price
from base where Order_Type ='P'
group by 
	DeliveredFrom,
	DeliveredFromName,
	PurchaseOrderID,
	ItemKey,
	DeliveredTo,
	DeliveredToName,
	Date
)

, final as (
select 
	b.DeliveredFrom,
	b.DeliveredFromName,
	b.PurchaseOrderID as 'Exchange_order',
	b.Date as 'Exchange_Order_Date',
	b.ItemKey,
	b.qty_loaned as 'Qty_Sold',
	p.PurchaseOrderID as 'Purchase',
	p.Purchase_Date,
	ROW_NUMBER() over (partition by b.itemkey, b.PurchaseOrderID order by p.Purchase_Date desc) as rn,
	p.quantity,
	p.max_unit_price as 'Unit_Price'
from base b 
left join purchase_orders p on b.DeliveredFrom = p.DeliveredTo and b.ItemKey = p.ItemKey
where 1=1
and convert(int,SUBSTRING(b.Date,1,4)+SUBSTRING(b.Date,6,2)) - convert(int,SUBSTRING(p.Purchase_Date,1,4)+SUBSTRING(p.Purchase_Date,6,2)) between 0 and 3

)


select 
	DeliveredFrom,
	DeliveredFromName,
	Exchange_order,
	Exchange_Order_Date,
	ItemKey,
	Qty_Sold,
	Purchase,
	Purchase_Date,
	Unit_Price
from final
where 1=1 
	and rn = 1
	and Exchange_order is not null 
	and Unit_Price is not null
order by Exchange_Order_Date desc, Exchange_order
