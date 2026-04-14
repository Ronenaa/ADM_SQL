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

--------------------------------------------------------------- the below part should be a view is the database ----------------------------------------------------
,totals_raw AS (
    -- Step 1: per-row calculations
    SELECT
        CONCAT(
            CAST(CONVERT(BIGINT, POL.POL_OrderID) AS VARCHAR(20)),
            CAST(POL.POL_LineID AS VARCHAR(10))
        ) AS PurchaseOrderID,
        
        CONVERT(VARCHAR, HS.QOD_MOTSR) + '-' + CONVERT(VARCHAR, HS.QOD_MOTSR) AS ItemKey,

        -- PNL classification
        CASE
            WHEN HST.QOD_SHROT IN (5, 19, 30, 4)
              OR HST.QOD_SHROT IS NULL
                THEN 999
            ELSE HST.QOD_SHROT
        END AS PNLKey,

        -- Order quantity (only main item rows)
        CASE
            WHEN HS.QOD_SHROT = 0 THEN HS.CMOT
            ELSE 0
        END AS OrderQuantity,

        -- Line total in USD (exactly כמו הקטע ששלחת)
        CASE
            WHEN HS.MTBE = '$' THEN HS.MCHIR_ICH * HS.CMOT
            WHEN HS.MTBE = 'Eur' THEN HS.MCHIR_ICH * HS.CMOT * (SM.NEW_SHEREURO / NULLIF(SM.NEW_SHER, 0))
            ELSE HS.MCHIR_ICH * HS.CMOT * (
                    1 / CASE WHEN HC.SHER_MTBE = 0 THEN 1 ELSE HC.SHER_MTBE END
                 )
        END AS LineTotalNetUSD
    FROM HOTSAOT_COTROT HC
    LEFT JOIN HOTSAOT_SHOROT HS
        ON HC.NOMRTOR = HS.NOMRTOR
    LEFT JOIN PurchaseOrderLines POL
        ON CONCAT(
               CAST(CONVERT(BIGINT, POL.POL_OrderID) AS VARCHAR(20)),
               CAST(POL.POL_LineID AS VARCHAR(10))
           ) = CAST(HS.MS_MSMKH_QSHOR AS VARCHAR(30))
    LEFT JOIN HOTSAOT_SHROTIM_New HST
        ON HS.QOD_SHROT = HST.QOD_SHROT
    LEFT JOIN CurrencyConvertion SM
        ON SM.TARIKH = HC.T_ERKH
    WHERE 1 = 1
      AND CAST(SUBSTRING(HC.T_MSMKH, 1, 4) AS INT) >= 2018
      AND CAST(SUBSTRING(HC.T_MSMKH, 1, 4) AS INT) <= YEAR(GETDATE())
      AND HS.QOD_SHROT NOT IN (14)
),

totals AS (
    -- Step 2: aggregate like the DAX measure
    SELECT
        PurchaseOrderID,
        ItemKey,
        999 as PNLKey,
        SUM(LineTotalNetUSD) AS LineTotalNetUSD,       -- same as [Total Expenses USD]
        SUM(OrderQuantity)   AS OrderQuantity,     -- same as [Total Order Quantity (Hzmnot)]
        CAST(ROUND(SUM(LineTotalNetUSD) / NULLIF(SUM(OrderQuantity), 0), 2) AS FLOAT) AS UnitNetPriceUSD  -- Avg Price USD (Expenses)
    FROM totals_raw
    GROUP BY
        PurchaseOrderID,
        ItemKey
),

-- exchange_movements: merges former 'main' + 'base' — raw free shipments enriched with purchase cost
exchange_movements AS (
    SELECT
        TM.QOD_SHOLCH  AS DeliveredFrom,
        W.SHM_GORM     AS DeliveredFromName,
        ISNULL(b.MS_HZMNH, bb.MS_HZMNH) AS PurchaseOrderID,
        CASE
            WHEN ISNULL(b.MS_HZMNH, bb.MS_HZMNH) LIKE '2000%' THEN 'P' -- Purchase order
            ELSE 'E'                                                     -- Expense / other
        END AS Order_Type,
        CONVERT(VARCHAR, TM.QOD_MOTSR) + '-' + CONVERT(VARCHAR, TM.QOD_MOTSR) AS ItemKey,
        SUM(TM.MSHQL_NTO) AS qty_loaned,
        G.QOD_GORM     AS DeliveredTo,
        G.SHM_GORM     AS DeliveredToName,
        SUBSTRING(TARIKH_MSHLOCH, 1, 4) + '-' + SUBSTRING(TARIKH_MSHLOCH, 5, 2) AS [Date],
        -- price columns from totals (previously joined in the separate 'base' CTE)
        t.UnitNetPriceUSD,
        t.LineTotalNetUSD,
        t.OrderQuantity
    FROM TEODOT_MSHLOCH TM
    LEFT JOIN GORMIM G                   ON TM.QOD_MQBL        = G.QOD_GORM
    LEFT JOIN GORMIM W                   ON TM.QOD_SHOLCH       = W.QOD_GORM
    LEFT JOIN QISHOR_RCSH_LMCIRH a       ON a.MS_TEODT_MCIRH   = TM.MS_TEODH
    LEFT JOIN QISHOR_T_MSHLOCH_HZMNOT b  ON a.MS_TEODT_RCSH    = b.MS_T_MSHLOCH
    LEFT JOIN QISHOR_T_MSHLOCH_HZMNOT bb ON bb.MS_T_MSHLOCH    = TM.MS_TEODH
    LEFT JOIN (
        SELECT PurchaseOrderID, ItemKey, UnitNetPriceUSD, LineTotalNetUSD, OrderQuantity
        FROM totals
        WHERE PNLKey = 999
          AND OrderQuantity <> 0
    ) t ON ISNULL(b.MS_HZMNH, bb.MS_HZMNH) = t.PurchaseOrderID
    WHERE TM.MCHIR_ICH = 0                         -- Only free/loaned shipments
      AND G.AOPI_PEILOT NOT IN (N'פחת', N'אחסון') -- Exclude waste/storage
      AND SUBSTRING(TARIKH_MSHLOCH, 1, 4) + '-' + SUBSTRING(TARIKH_MSHLOCH, 5, 2) >= '2024-01'
    GROUP BY
        SUBSTRING(TARIKH_MSHLOCH, 1, 4) + '-' + SUBSTRING(TARIKH_MSHLOCH, 5, 2),
        TM.QOD_SHOLCH, W.SHM_GORM,
        b.MS_HZMNH, bb.MS_HZMNH,
        CONVERT(VARCHAR, TM.QOD_MOTSR) + '-' + CONVERT(VARCHAR, TM.QOD_MOTSR),
        G.QOD_GORM, G.SHM_GORM,
        t.UnitNetPriceUSD, t.LineTotalNetUSD, t.OrderQuantity
),

purchase_orders AS (
    -- P-type movements only: the original purchases that exchanges are matched against
    SELECT
        DeliveredFrom,
        DeliveredFromName,
        PurchaseOrderID,
        ItemKey,
        SUM(qty_loaned)        AS quantity,
        DeliveredTo,
        DeliveredToName,
        [Date]                 AS Purchase_Date,
        MAX(UnitNetPriceUSD)   AS max_unit_price,
        MAX(LineTotalNetUSD)   AS max_LineTotalNetUSD,
        MAX(OrderQuantity)     AS max_OrderQuantity
    FROM exchange_movements
    WHERE Order_Type = 'P'
    GROUP BY
        DeliveredFrom, DeliveredFromName,
        PurchaseOrderID, ItemKey,
        DeliveredTo, DeliveredToName,
        [Date]
),

-- exchange_priced: merges former 'final' + 'final2' — ranks and keeps only the best purchase match
exchange_priced AS (
    SELECT
        DeliveredFrom, DeliveredFromName,
        Exchange_order, Exchange_Order_Date,
        ItemKey, Qty_Sold,
        Purchase, Purchase_Date,
        UnitNetPriceUSD, LineTotalNetUSD, OrderQuantity
    FROM (
        SELECT
            b.DeliveredFrom,
            b.DeliveredFromName,
            b.PurchaseOrderID  AS Exchange_order,
            b.[Date]           AS Exchange_Order_Date,
            b.ItemKey,
            b.qty_loaned       AS Qty_Sold,
            p.PurchaseOrderID  AS Purchase,
            p.Purchase_Date,
            p.max_unit_price   AS UnitNetPriceUSD,
            p.max_LineTotalNetUSD AS LineTotalNetUSD,
            p.max_OrderQuantity   AS OrderQuantity,
            ROW_NUMBER() OVER (
                PARTITION BY b.ItemKey, b.PurchaseOrderID
                ORDER BY p.Purchase_Date DESC
            ) AS rn
        FROM exchange_movements b
        LEFT JOIN purchase_orders p
            ON  b.DeliveredFrom = p.DeliveredTo
            AND b.ItemKey       = p.ItemKey
        WHERE CONVERT(INT, SUBSTRING(b.[Date], 1, 4) + SUBSTRING(b.[Date], 6, 2))
            - CONVERT(INT, SUBSTRING(p.Purchase_Date, 1, 4) + SUBSTRING(p.Purchase_Date, 6, 2))
            BETWEEN 0 AND 3
    ) ranked
    WHERE rn = 1
      AND Exchange_order  IS NOT NULL
      AND UnitNetPriceUSD IS NOT NULL
)

-----------------------------------------------------------------------------------------------------------------------------------------------
,Purchase_Exchange as (

----Invoice--
SELECT
	 CAST(CCS.HZMNT_RCSH AS VARCHAR)			AS PurchaseOrderID,
     'Invoice'									AS DocName,
     NULL										AS SupplierKey,
     CAST(SUBSTRING(CCS.T_CHSHBONIT,1,4) + '-' + SUBSTRING(CCS.T_CHSHBONIT,5,2) + '-' + SUBSTRING(CCS.T_CHSHBONIT,7,2) AS DATE) AS [Value Date],
     CAST(CONVERT(VARCHAR, POL_ProductID) AS VARCHAR) + '-' + CAST(CONVERT(VARCHAR, M.ServiceCode) AS VARCHAR) + 'S' AS ItemKey,
     CAST(ROUND(CCS.MCHIR_ICH_LLA_ME_M / NULLIF(CASE WHEN SHER_LCHISHOB <> 0 THEN SHER_LCHISHOB ELSE CC2.new_sher END, 0), 2) AS FLOAT) * -1  AS UnitNetPriceUSD,
     CCS.MSHQL_NTO                   AS Quantity,
     CASE WHEN CCS.QOD_MOTSR = 0 THEN CCS.MSHQL_NTO ELSE 0 END AS OrderQuantity,
     HST.QOD_SHROT                           AS PNLKey,
	 HST.PNL as [PNL Code],
     CAST(ROUND(CCS.CMOT * (CCS.MCHIR_ICH_LLA_ME_M / NULLIF(CASE WHEN SHER_LCHISHOB <> 0 THEN SHER_LCHISHOB ELSE CC2.new_sher END, 0)), 2) AS FLOAT) * -1 AS LineTotalNetUSD,
     CONVERT(INT, CONVERT(VARCHAR, SUBSTRING(CCS.T_CHSHBONIT,1,4) + SUBSTRING(CCS.T_CHSHBONIT,5,2))) AS YearMonth,
	 null as ShipID,
    'Invoice'                                AS DocType,
	CASE WHEN SHER_LCHISHOB <> 0 THEN SHER_LCHISHOB ELSE CC2.new_sher END as [NEW_SHER]
FROM [dbo].[CHIOBI_CHOTS_COTROT] CC
LEFT JOIN [dbo].[CHIOBI_CHOTS_SHOROT] CCS
       ON CC.MS_CHSHBONIT = CCS.MS_CHSHBONIT
LEFT JOIN CurrencyConvertion CC2
       ON CCS.[TARIKH_MSHLOCH] = CC2.Tarikh
LEFT JOIN (select DISTINCT
			POL_SQL_POID, POL_ProductID
			from PurchaseOrderLines POL	) POL
		ON CCS.HZMNT_RCSH = POL.POL_SQL_POID
LEFT JOIN MOTSRIM M
	ON CCS.QOD_MOTSR = M.QOD_MOTSR
LEFT JOIN HOTSAOT_SHROTIM_New HST 
	ON M.ServiceCode = HST.QOD_SHROT
	where CAST(SUBSTRING(CCS.T_CHSHBONIT,1,4) + '-' + SUBSTRING(CCS.T_CHSHBONIT,5,2) + '-' + SUBSTRING(CCS.T_CHSHBONIT,7,2) AS DATE) >= '2024-01-01'

UNION ALL
	------Import--------
SELECT

		CONCAT(
			CAST(CONVERT(BIGINT, POL.POL_OrderID) AS VARCHAR(20)),
			CAST(POL.POL_LineID AS VARCHAR(10))
		 )	as 'PurchaseOrderID'
		,'Import' AS 'DocName'  -- was: 'Order Expenses'
		,CAST(
		  MAX(
			CASE WHEN HS.QOD_SHROT = 0
				 THEN CONVERT(INT, CONVERT(VARCHAR, HC.QOD_SPQ))
			END
		  ) OVER (PARTITION BY HS.MS_MSMKH_QSHOR)
			AS VARCHAR)	 AS 'SupplierKey'
		,CASE 
			WHEN HS.MS_MSMKH_QSHOR = 0 
			  THEN CONVERT(date, HC.T_ERKH, 112)
			ELSE
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
		,CASE
			WHEN  HST.QOD_SHROT IS NULL OR HS.QOD_SHROT IN (5, 19, 30, 4) THEN cast(CONVERT(INT, CONVERT(VARCHAR,HS.QOD_MOTSR)) as varchar)+ '-' + cast(CONVERT(INT, CONVERT(VARCHAR,HS.QOD_MOTSR)) as varchar) -- העמסת עליות הובלה והפרש מחיר על המוצר
			ELSE cast(CONVERT(INT, CONVERT(VARCHAR,HS.QOD_MOTSR)) as varchar)+ '-' + cast(CONVERT(INT, CONVERT(VARCHAR,HST.QOD_SHROT)) as varchar)+'S' 
			END		AS 'ItemKey'

		 ,CASE
			WHEN HST.QOD_SHROT IS NOT NULL THEN
				CASE
					WHEN HS.MTBE = '$' THEN
						CAST(ROUND(HS.MCHIR_ICH / NULLIF(POL.POL_FinalWeightReceived, 0), 2) AS FLOAT)
					WHEN HS.MTBE = 'Eur' THEN
						CAST(ROUND(
							HS.MCHIR_ICH * (SM.NEW_SHEREURO / NULLIF(SM.NEW_SHER, 0)) / NULLIF(POL.POL_FinalWeightReceived, 0)
						, 2) AS FLOAT)
					ELSE
						CAST(ROUND(
							HS.MCHIR_ICH * (1 / NULLIF(HC.SHER_MTBE, 0)) / NULLIF(POL.POL_FinalWeightReceived, 0)
						, 2) AS FLOAT)
				END
			ELSE
				CASE
					WHEN HS.MTBE = '$' THEN CAST(ROUND(HS.MCHIR_ICH, 2) AS FLOAT)
					WHEN HS.MTBE = 'Eur' THEN
						CAST(ROUND(
							HS.MCHIR_ICH * (SM.NEW_SHEREURO / NULLIF(SM.NEW_SHER, 0))
						, 2) AS FLOAT)
					ELSE
						CAST(ROUND(
							HS.MCHIR_ICH * (1 / NULLIF(HC.SHER_MTBE, 0))
						, 2) AS FLOAT)
				END
		END AS 'UnitNetPriceUSD'
		,HS.CMOT AS 'Quantity'
		,CASE WHEN HS.QOD_SHROT = 0 then HS.CMOT
				ELSE 0
				END									AS 'OrderQuantity'
		,CASE 
			WHEN HST.QOD_SHROT IN (5, 19, 30, 4,18) OR HST.QOD_SHROT IS NULL 
			THEN 999
			ELSE HST.QOD_SHROT
		END											AS 'PNLKey' ----------  העמסת עליות הובלה על המוצר והפרש מחיר
			,case when HST.QOD_SHROT IN (5, 19, 30, 4,18) OR HST.QOD_SHROT IS NULL  then 1010 
			else HST.PNL end as [PNL Code]
			,CAST(ROUND(CASE
	         WHEN HS.MTBE = '$' THEN HS.MCHIR_ICH * HS.CMOT
			 WHEN HS.MTBE = 'Eur' THEN HS.MCHIR_ICH * HS.CMOT * (SM.NEW_SHEREURO / NULLIF(SM.NEW_SHER, 0))
	         ELSE HS.MCHIR_ICH * HS.CMOT * (1 / CASE WHEN HC.SHER_MTBE = 0 THEN 1 ELSE HC.SHER_MTBE END)
         END, 2) AS FLOAT) AS 'LineTotalNetUSD'
		,CONVERT(INT, CONVERT(VARCHAR,SUBSTRING(HS.T_ERKH,1,4) + SUBSTRING(HS.T_ERKH,5,2))) AS YearMonth
		,sl.ShipID
		,CASE 
		    WHEN HS.QOD_SHROT IN (5, 19, 30, 20, 4) THEN N'סחורה'
		    ELSE HC.SOG_MSMKH
		END												as DocType
		,SM.NEW_SHER
FROM HOTSAOT_COTROT HC
LEFT JOIN HOTSAOT_SHOROT HS ON HC.NOMRTOR = HS.NOMRTOR
LEFT JOIN PurchaseOrderLines POL ON CONCAT(
										CAST(CONVERT(BIGINT, POL.POL_OrderID) AS VARCHAR(20)),
										CAST(POL.POL_LineID AS VARCHAR(10))
									)	
										= CAST(HS.MS_MSMKH_QSHOR AS VARCHAR(30))
LEFT JOIN HZMNOT HZ ON (HS.MS_MSMKH_QSHOR = HZ.MSPR_HZMNH)
LEFT JOIN HOTSAOT_SHROTIM_New HST ON HS.QOD_SHROT = HST.QOD_SHROT
LEFT JOIN ShipsArrivals sa ON POL.POL_ShipArrivalID = sa.SA_ID
LEFT JOIN ShipList sl ON sa.SA_ShipID = sl.ShipID
LEFT JOIN PurchaseOrder PO ON POL.POL_OrderID = PO.PO_OrderID
LEFT JOIN CurrencyConvertion SM  ON SM.TARIKH=HC.T_ERKH

WHERE 1=1
and Cast(SUBSTRING(HC.T_MSMKH,1,4) as int) >=2018 and Cast(SUBSTRING(HC.T_MSMKH,1,4) as int) <= YEAR(GETDATE())
and HS.QOD_SHROT NOT IN (14)
and CONCAT(CAST(CONVERT(BIGINT, POL.POL_OrderID) AS VARCHAR(20)),CAST(POL.POL_LineID AS VARCHAR(10))) <> ' '
and  cast( HC.T_ERKH as date) >= '2024-01-01'

UNION ALL

-----Exchange----
SELECT
    HZ.MSPR_HZMNH                                                       AS PurchaseOrderID,
    'Exchange'                                                          AS DocName,  -- was: 'Orders'
    NULL		                                                        AS SupplierKey,
    CAST(SUBSTRING(HZ.T_ASPQH,1,4) + '-' + SUBSTRING(HZ.T_ASPQH,5,2) + '-' + SUBSTRING(HZ.T_ASPQH,7,2)
         AS DATE)                                                       AS [Value Date],
    CAST(CONVERT(INT, CONVERT(VARCHAR,HZ.MOTSR_MOZMN)) AS VARCHAR) + '-' +
    CAST(CONVERT(INT, CONVERT(VARCHAR,HZ.MOTSR_MOZMN)) AS VARCHAR)      AS ItemKey,
    t.UnitNetPriceUSD                                                   AS UnitNetPriceUSD,
    HZ.CMOT_MOZMNT                                                      AS Quantity,
    t.OrderQuantity                                                     AS OrderQuantity,
    999                                                                 AS PNLKey,
	1010  as [PNL Code],
    t.LineTotalNetUSD                                                  AS LineTotalNetUSD,
    CONVERT(INT, CONVERT(VARCHAR, SUBSTRING(HZ.T_ASPQH,1,4) + SUBSTRING(HZ.T_ASPQH,5,2))) AS YearMonth,
    NULL                                                                AS ShipID,
    'Order'                                                             AS DocType,
    SM.NEW_SHER
FROM HZMNOT HZ
LEFT JOIN CurrencyConvertion SM
    ON SM.TARIKH = HZ.T_HZMNH
LEFT JOIN exchange_priced t
    ON t.Exchange_order = HZ.MSPR_HZMNH
   AND t.ItemKey =
        CAST(CONVERT(INT, CONVERT(VARCHAR,HZ.MOTSR_MOZMN)) AS VARCHAR) + '-' +
        CAST(CONVERT(INT, CONVERT(VARCHAR,HZ.MOTSR_MOZMN)) AS VARCHAR)
WHERE CAST(SUBSTRING(HZ.T_HZMNH,1,4) AS INT) BETWEEN 2018 AND YEAR(GETDATE())
  AND HZ.OrderStatus <> 3
  AND HZ.ActionType IN (6,7)
  AND YEAR(CAST(SUBSTRING(HZ.T_ASPQH,1,4) + '-' + SUBSTRING(HZ.T_ASPQH,5,2) + '-' + SUBSTRING(HZ.T_ASPQH,7,2) AS DATE))  >= 2024
  )

,inv as (
		SELECT
			CONVERT(VARCHAR, FINAL.ItemKey) + '-' + CONVERT(VARCHAR, FINAL.ItemKey) as ItemKey,
			FINAL.SupplierKey,
			CONVERT(char(7), FINAL.Date, 126) AS YearMonth,

			SUM(FINAL.Quantity) AS TotalQuantity,

			CAST(ROUND(MAX(CASE WHEN FINAL.rn = 1 THEN FINAL.UnitPrice END), 2) AS FLOAT) AS LastUnitPrice,
			CAST(ROUND(MAX(CASE WHEN FINAL.rn = 1 THEN FINAL.FOTprice END), 2) AS FLOAT) AS LastFOTPrice,
			CAST(ROUND(MAX(CASE WHEN FINAL.rn = 1 THEN FINAL.CFPrice END), 2) AS FLOAT) AS LastCFPrice,
			CAST(ROUND(MAX(CASE WHEN FINAL.rn = 1 THEN FINAL.WeightedExpenses END), 2) AS FLOAT) AS WeightedExpenses

		FROM (
			SELECT
				Inv.*,
				ROW_NUMBER() OVER (
					PARTITION BY 
						Inv.ItemKey,
						Inv.SupplierKey,
						CONVERT(char(7), Inv.Date, 126)
					ORDER BY 
						Inv.Date DESC,
						Inv.Version DESC
				) AS rn
		FROM (

        /* ===== STG_1 ===== */
        SELECT 
            ProductID AS ItemKey,
            inv.DueDate AS Date,
            SupplierID AS SupplierKey,
            AvgPrice AS UnitPrice,
            FotFlat AS FOTprice,
            CFFlat AS CFPrice,
            INV.TotalInventory AS Quantity,
            PCOST.WeightedExpenses,
            Inv.Version,
            ROW_NUMBER() OVER (
                PARTITION BY inv.DueDate, inv.SupplierID, inv.ProductID 
                ORDER BY inv.Version DESC
            ) AS MaxVersion
        FROM tblInventory Inv
        LEFT JOIN tblProductsCost PCost
            ON INV.DueDate = PCost.DueDate
            AND INV.ProductID = PCost.ProductCode
            AND INV.Version = PCost.Version
        UNION ALL
        /* ===== STG_3 (including STG_2 inline) ===== */
        SELECT
            TM.ItemKey,
            PC.DueDate AS Date,
            1144 AS SupplierKey,
            PC.AvgPrice AS UnitPrice,
            PC.FotFlat AS FOTprice,
            PC.CFFlat AS CFPrice,
            SUM(TM.Quantity) AS Quantity,
            PC.WeightedExpenses,
            MV.MAXVERSION AS Version,
            ROW_NUMBER() OVER (
                PARTITION BY PC.DueDate, TM.ItemKey
                ORDER BY MV.MAXVERSION DESC
            ) AS MaxVersion

        FROM (
            /* STG_2 inline */
            SELECT 
                TM.QOD_PRIT AS ItemKey,
                CAST(SUBSTRING(T_TNOEH,1,4) + '-' + SUBSTRING(T_TNOEH,5,2) + '-' + SUBSTRING(T_TNOEH,7,2) AS DATE) AS [Date],
                SUM(TM.CMOT_LOGOS) AS Quantity
            FROM BT.dbo.TNOEOT_MLAI_CLLI TM
            WHERE QOD_GORM <> 1
              AND CAST(SUBSTRING(TM.T_TNOEH,1,4) AS INT) BETWEEN 2018 AND YEAR(GETDATE())
              AND SOG_TNOEH_CN_ITS_SP_HE LIKE N'%כ%'
            GROUP BY TM.QOD_PRIT, TM.T_TNOEH
            UNION ALL
            SELECT 
                TM.QOD_PRIT,
                CAST(SUBSTRING(T_TNOEH,1,4) + '-' + SUBSTRING(T_TNOEH,5,2) + '-' + SUBSTRING(T_TNOEH,7,2) AS DATE) AS [Date],
                SUM(TM.CMOT_LOGOS) * -1
            FROM BT.dbo.TNOEOT_MLAI_CLLI TM
            WHERE QOD_GORM <> 1
              AND CAST(SUBSTRING(TM.T_TNOEH,1,4) AS INT) BETWEEN 2018 AND YEAR(GETDATE())
              AND SOG_TNOEH_CN_ITS_SP_HE LIKE N'%י%'
            GROUP BY TM.QOD_PRIT, TM.T_TNOEH
        ) TM

        LEFT JOIN (
            SELECT 
                DueDate,
                ProductCode,
                MAX(Version) AS MAXVERSION
            FROM tblProductsCost
            GROUP BY DueDate, ProductCode
        ) MV
            ON MV.DueDate = TM.Date
            AND MV.ProductCode = TM.ItemKey

        LEFT JOIN tblProductsCost PC
            ON PC.Version = MV.MAXVERSION
            AND PC.DueDate = MV.DueDate
            AND PC.ProductCode = MV.ProductCode

        GROUP BY 
            TM.ItemKey,
            PC.DueDate,
            PC.AvgPrice,
            PC.CFFlat,
            PC.FotFlat,
            PC.WeightedExpenses,
            MV.MAXVERSION

    ) Inv
    WHERE Inv.MaxVersion = 1

) FINAL

GROUP BY
    FINAL.ItemKey,
    FINAL.SupplierKey,
    CONVERT(char(7), FINAL.Date, 126)
	)

-- Resolves the primary DocName per PurchaseOrderID before aggregation.
-- A PO can have rows of multiple types (e.g. Import + Invoice).
-- Priority: Import > Exchange > Invoice.
,po_doctype AS (
    SELECT DISTINCT
        PurchaseOrderID,
        CASE
            WHEN EXISTS (
                SELECT 1 FROM Purchase_Exchange pe2
                WHERE pe2.PurchaseOrderID = pe.PurchaseOrderID
                  AND pe2.DocName = 'Import'
            ) THEN 'Import'
            WHEN EXISTS (
                SELECT 1 FROM Purchase_Exchange pe2
                WHERE pe2.PurchaseOrderID = pe.PurchaseOrderID
                  AND pe2.DocName = 'Exchange'
            ) THEN 'Exchange'
            ELSE 'Invoice'
        END AS DocName
    FROM Purchase_Exchange pe
)

,P_costs as (
    select
	pe.PurchaseOrderID,
	dt.DocName,
	min([Value Date]) as ValueDate,
	max(case when [PNLKey] = 999 then SupplierKey else null end) as SupplierKey,
	max(ShipID) as boat,
	SUM(CASE WHEN [PNL Code] = 2270 THEN LineTotalNetUSD ELSE 0 END) as DischargeCosts,
	CAST(ROUND(SUM(CASE WHEN [PNL Code] = 1201 THEN LineTotalNetUSD ELSE 0 END) / NULLIF(SUM(orderquantity), 0), 2) AS FLOAT) as [demurrage / Despatch],
	CAST(ROUND(SUM(CASE WHEN [PNL Code] = 1111 THEN LineTotalNetUSD ELSE 0 END) / NULLIF(SUM(orderquantity), 0), 2) AS FLOAT) as [Shortage],
	CAST(ROUND(SUM(CASE WHEN [PNL Code] not in (1010,2270,1201,1111) THEN LineTotalNetUSD ELSE 0 END) / NULLIF(SUM(orderquantity), 0), 2) AS FLOAT) as [Other_Expenses],
	sum (orderquantity) as orderquantity,
	sum(LineTotalNetUSD) as LineTotalNetUSD,
	CAST(ROUND(
		SUM(CASE WHEN PNLKey = 999 THEN LineTotalNetUSD ELSE 0 END)
		/ NULLIF(SUM(CASE WHEN PNLKey = 999 THEN OrderQuantity ELSE 0 END), 0)
	, 2) AS FLOAT) AS Cif_price,
	MAX(pe.NEW_SHER) AS NEW_SHER
  from Purchase_Exchange pe
  LEFT JOIN po_doctype dt ON dt.PurchaseOrderID = pe.PurchaseOrderID
  group by pe.PurchaseOrderID, dt.DocName
  )
,sales as (
  SELECT 
	'Invoice' AS 'DocName'
		,Case
			WHEN CS.QOD_MOTSR = TM.QOD_MOTSR
				THEN 'Item'
			ELSE 'Additional Expense'
		END AS 'LineType'
		,CONVERT(VARCHAR,CS.QOD_LQOCH) 'AccountKey' -- customer from invoices in case you need just invoices 
		,SUBSTRING(CS.T_CHSHBONIT,1,4) + '-' + SUBSTRING(CS.T_CHSHBONIT,5,2) + '-' + SUBSTRING(CS.T_CHSHBONIT,7,2) AS 'Date' -- Invoice date  
		,CAST(CONVERT(INT, CONVERT(VARCHAR,AM.QOD))as varchar) 'AgentKey' -- Agent from Customer method
		,CAST( CONVERT(VARCHAR,CS.QOD_MOTSR) as varchar) +'-' + CAST( CONVERT(VARCHAR,CS.QOD_MOTSR) as varchar) AS 'ItemKey'
		,CAST(ROUND(CS.MCHIR_ICH_LLA_ME_M / NULLIF(CASE WHEN SHER_LCHISHOB <> 0 THEN SHER_LCHISHOB ELSE 1 END, 0), 2) AS FLOAT) AS 'UnitNetPriceUSD'
		,cast(ROUND(CS.MSHQL_NTO,2)AS FLOAT)  AS 'Quantity'
		,case
		when TM.ActionType = 11 and CS.QOD_MOTSR = TM.QOD_MOTSR then TM.MSHQL_NTO
		else 0 end
	as qty_cif
	,case
		when TM.ActionType = 1 and CS.QOD_MOTSR = TM.QOD_MOTSR then TM.MSHQL_NTO
		else 0 end
	as qty_fot
	,case
		when TM.ActionType = 1 then 'FOT'
		WHEN TM.ActionType = 11 then 'CIF'
		WHEN TM.ActionType = 12 then 'FOT Premium'
	else null 
	end as SalesType
		,CASE
			WHEN CS.QOD_MOTSR=TM.QOD_MOTSR
				THEN 0
			ELSE Cs.CMOT
		END as AdditionalQuantity
		,CAST(ROUND(
			CASE
				WHEN CS.QOD_MOTSR <> 0
					THEN (CS.MCHIR_ICH_LLA_ME_M * CS.MSHQL_NTO) / NULLIF(CASE WHEN SHER_LCHISHOB <> 0 THEN SHER_LCHISHOB ELSE CC.new_sher END, 0)
				ELSE CS.MCHIR_ICH_LLA_ME_M / NULLIF(CASE WHEN SHER_LCHISHOB <> 0 THEN SHER_LCHISHOB ELSE CC.new_sher END, 0)
			END
			+
			CASE
				WHEN CS.QOD_MOTSR = TM.QOD_MOTSR THEN 0
				WHEN TM.QOD_MOTSR IS NULL         THEN 0
				ELSE Cs.CMOT * (CS.MCHIR_ICH_LLA_ME_M / NULLIF(CASE WHEN SHER_LCHISHOB <> 0 THEN SHER_LCHISHOB ELSE CC.new_sher END, 0))
			END
		, 2) AS FLOAT) AS 'LineTotalNet_USD' --Invoices are always in NIS
		,CONVERT(INT, CONVERT(VARCHAR,SUBSTRING(CS.T_CHSHBONIT,1,4) + SUBSTRING(CS.T_CHSHBONIT,5,2))) AS YearMonth
		, TM.MS_TEODH as 'DeliveryNote'
		,CASE
			WHEN SUBSTRING(TM.TARIKH_MSHLOCH,1,4) = '0000'
				THEN CAST(SUBSTRING(CS.T_CHSHBONIT,1,4) + '-' + SUBSTRING(CS.T_CHSHBONIT,5,2) + '-' + SUBSTRING(CS.T_CHSHBONIT,7,2) as date)
			ELSE CAST(ISNULL(SUBSTRING(TM.TARIKH_MSHLOCH,1,4) + '-' + SUBSTRING(TM.TARIKH_MSHLOCH,5,2) + '-' + SUBSTRING(TM.TARIKH_MSHLOCH,7,2),
		ISNULL(SUBSTRING(FTM.FirstDeliveryDateTM,1,4) + '-' + SUBSTRING(FTM.FirstDeliveryDateTM,5,2) + '-' + SUBSTRING(FTM.FirstDeliveryDateTM,7,2),
		SUBSTRING(CS.T_CHSHBONIT,1,4) + '-' + SUBSTRING(CS.T_CHSHBONIT,5,2) + '-' + SUBSTRING(CS.T_CHSHBONIT,7,2)))
		AS date) 
		END as 'DeliveryDate'
		,HZ.MSPR_HZMNH as 'OrderID'
		,TM.QOD_SHOLCH as 'SupplierWarehouse'
		,TM.ActionType							as 'ActionType'
		,CASE 
			WHEN TM.ActionType = 6 THEN G.AOPI_PEILOT
			ELSE act.ActionType
		END										as 'ActionTypeDesc'										
		,'0'									as 'AdjustmentFlag'
		,'Sales'								as  'QuantityCategory'
		,NULL									as  'TransactionType'  -- invoices are always direct sales
FROM CHSHBONIOT_COTROT CH
Left Join GORMIM G
	on CH.QOD_LQOCH = G.QOD_GORM
Left Join TBLT_ANSHI_MCIROT AM 
	on AM.SHM_AISH_MCIROT = G.AISH_MCIROT_MTPL
LEFT JOIN CHSHBONIOT_SHOROT CS 
    ON CH.MS_CHSHBONIT = CS.MS_CHSHBONIT
LEFT JOIN TEODOT_MSHLOCH TM 
	ON CS.MS_T_MSHLOCH = TM.MS_TEODH
LEFT JOIN (SELECT MS_CHSHBONIT , Min (TM.TARIKH_MSHLOCH) as 'FirstDeliveryDateTM',Min (CHS.TARIKH_MSHLOCH) as 'FirstDeliveryDateCHS'
			FROM CHSHBONIOT_SHOROT CHS
			Left Join TEODOT_MSHLOCH TM
				ON TM.MS_TEODH = CHS.MS_T_MSHLOCH
			Where 1=1
			AND TM.TARIKH_MSHLOCH <>00000000
			AND CHS.TARIKH_MSHLOCH <>00000000
			Group By CHS.MS_CHSHBONIT) FTM
	ON FTM.MS_CHSHBONIT = CH.MS_CHSHBONIT
Left Join (SELECT MS_HZMNH,MS_T_MSHLOCH
			FROM QISHOR_T_MSHLOCH_HZMNOT
			) HZT
	on TM.MS_TEODH = HZT.MS_T_MSHLOCH
Left Join HZMNOT HZ
	on HZ.MSPR_HZMNH = HZT.MS_HZMNH
left join TBLT_PEOLOT_HZMNH_T_MSHLOCH act
	on TM.ActionType = act.MS_AOPTSIH
left join CurrencyConvertion CC 
	ON cs.[TARIKH_MSHLOCH] = cc.Tarikh
WHERE 1=1  
AND Cast(SUBSTRING(cs.T_CHSHBONIT,1,4) as int) >= 2025
AND TM.QOD_SHOLCH <> TM.QOD_MQBL 

-------OPEN Orders----------------------------------------------------------------------------------
UNION ALL

 SELECT 
	'Delivery Note' as  'DocName'
	,'Item' AS 'LineType'
	,CONVERT(INT, CONVERT(VARCHAR, TM.QOD_MQBL)) AS 'AccountKey'
	,SUBSTRING(TARIKH_MSHLOCH,1,4) + '-' + SUBSTRING(TARIKH_MSHLOCH,5,2) + '-' + SUBSTRING(TARIKH_MSHLOCH,7,2) as 'Date'
	,CONVERT(INT, CONVERT(VARCHAR, TM.AISH_MCIROT)) AS 'AgentKey'
	,CONVERT(VARCHAR, TM.QOD_MOTSR)+'-'+CONVERT(VARCHAR, TM.QOD_MOTSR) AS 'ItemKey'
	,CAST(ROUND(CASE
	         WHEN TM.MTBE_SH = '$' THEN TM.MCHIR_ICH
	         ELSE TM.MCHIR_ICH * (1 / NULLIF(SM.NEW_SHER, 0))
         END, 2) AS FLOAT) AS 'UnitNetPriceUSD'
	,cast(ROUND(TM.MSHQL_NTO,2)AS FLOAT) as 'Quantity'
	,case
		when TM.ActionType = 11 then TM.MSHQL_NTO
		else 0 end
	as qty_cif
	,case
		when TM.ActionType = 1 then TM.MSHQL_NTO
		else 0 end
	as qty_fot
	,case
		when TM.ActionType = 1 then 'FOT'
		WHEN TM.ActionType = 11 then 'CIF'
		WHEN TM.ActionType = 12 then 'FOT Premium'
	else null 
	end as SalesType
	,0 AS 'AdditionalQuantity'
	,CAST(ROUND(CASE
	         WHEN TM.MTBE_SH = '$'
		     THEN TM.MCHIR_ICH * TM.MSHQL_NTO /*TM.CMOT_SHSOPQH*/
	         ELSE TM.MCHIR_ICH * TM.MSHQL_NTO /*TM.CMOT_SHSOPQH*/ * (1 / NULLIF(SM.NEW_SHER, 0))
         END, 2) AS FLOAT) AS 'LineTotalNet_USD'
	,CONVERT(INT, CONVERT(VARCHAR,SUBSTRING(T_ASPQH,1,4) + SUBSTRING(T_ASPQH,5,2))) AS YearMonth
	,TM.MS_TEODH as 'DeliveryNote'
	,CAST(SUBSTRING(TARIKH_MSHLOCH,1,4) + '-' + SUBSTRING(TARIKH_MSHLOCH,5,2) + '-' + SUBSTRING(TARIKH_MSHLOCH,7,2) as date) as 'DeliveryDate'
	,HZ.MSPR_HZMNH as 'OrderID'
	,TM.QOD_SHOLCH as 'SupplierWarehouse'
	,TM.ActionType							as 'ActionType'
	,CASE 
		WHEN TM.ActionType = 6 THEN G.AOPI_PEILOT
		ELSE act.ActionType
	END as 'ActionTypeDesc'
	,CASE
	WHEN TM.MCHIR_ICH = 0
		THEN 1
	ELSE 0
	END AS 'AdjustmentFlag'
	,CASE
		WHEN TM.MCHIR_ICH <> 0 THEN 'Sales'
		WHEN TM.MCHIR_ICH = 0 and G.AOPI_PEILOT = 'פחת' THEN 'Shortage'
		WHEN TM.MCHIR_ICH = 0 and G.AOPI_PEILOT = 'אחסון' THEN 'Storage'
		WHEN TM.MCHIR_ICH = 0 and G.AOPI_PEILOT NOT IN ('פחת','אחסון') then 'Exchange'
	END AS 'QuantityCategory'
	,CASE
		WHEN TM.MCHIR_ICH = 0 AND W.QOD_GORM IS NOT NULL THEN G.AOPI_PEILOT
		WHEN TM.MCHIR_ICH = 0 AND W.QOD_GORM IS NULL     THEN 'החלפה'
		ELSE NULL
	END AS 'TransactionType'
FROM TEODOT_MSHLOCH TM
Left Join (SELECT distinct MS_T_MSHLOCH
			FROM CHSHBONIOT_SHOROT
		) CH
	on TM.MS_TEODH = CH.MS_T_MSHLOCH
Left Join (SELECT MS_HZMNH,MS_T_MSHLOCH
			FROM QISHOR_T_MSHLOCH_HZMNOT
		) HZT
	on TM.MS_TEODH = HZT.MS_T_MSHLOCH
Left Join HZMNOT HZ
	on HZ.MSPR_HZMNH = HZT.MS_HZMNH
LEFT JOIN CurrencyConvertion SM  -- reuse top-level CTE instead of repeating SHERI_MTBE inline
	ON SM.TARIKH = HZ.T_HZMNH
Left Join GORMIM G
	on TM.QOD_MQBL = G.QOD_GORM
Left Join (
			Select *
			From GORMIM
			Where EntityType Like N'%מקום אספקה%') W
	On W.QOD_GORM = TM.QOD_SHOLCH
left join TBLT_PEOLOT_HZMNH_T_MSHLOCH act
	on TM.ActionType = act.MS_AOPTSIH
WHERE
CH.MS_T_MSHLOCH is null
AND Cast(SUBSTRING(TARIKH_MSHLOCH,1,4) as int) >= 2025
AND STTOS in (0,1)
AND TM.PurchaseOrderType = 0
AND TM.QOD_SHOLCH <> TM.QOD_MQBL   -- exclude internal docs (same source and destination)
  )




,base_link as (
  SELECT distinct
        a.MS_TEODT_MCIRH AS DeliveryNote,
        b.MS_HZMNH AS PurchaseOrderID
    FROM QISHOR_RCSH_LMCIRH a
    LEFT JOIN QISHOR_T_MSHLOCH_HZMNOT b
        ON a.MS_TEODT_RCSH = b.MS_T_MSHLOCH

		)

-- Warehouse sales: delivery notes that have no entry in base_link are not linked to any
-- purchase order, meaning they originate from a warehouse (not import or exchange).
,WH_sales AS (
    SELECT
        s.DeliveryNote,
        s.DeliveryDate,
        FORMAT(s.DeliveryDate, 'yyyy-MM')                                           AS [Year-Month],
        s.AccountKey,
        s.AgentKey,
        s.ActionType,
        s.ActionTypeDesc,
        s.SupplierWarehouse,
        s.AdjustmentFlag,
        s.QuantityCategory,
        MAX(s.TransactionType)                                                      AS TransactionType,
        -- ItemKey, sale price, and SalesType come from the 'Item' line only
        MAX(CASE WHEN s.LineType = 'Item' THEN s.ItemKey        ELSE NULL END)      AS ItemKey,
        MAX(CASE WHEN s.LineType = 'Item' THEN s.SalesType      ELSE NULL END)      AS SalesType,
        MAX(CASE WHEN s.LineType = 'Item' THEN s.UnitNetPriceUSD ELSE NULL END)     AS UnitNetPriceUSD,
        -- Quantity = item qty only (excludes storage/additional-expense qty)
        SUM(CASE WHEN s.LineType = 'Item' THEN s.Quantity       ELSE 0   END)       AS Quantity,
        -- Total revenue = all lines combined (item + storage fee)
        SUM(s.LineTotalNet_USD)                                                     AS LineTotalNet_USD,
        -- 1 = delivery note had multiple lines (e.g. item + warehouse storage fee), 0 = single line
        CASE WHEN COUNT(*) > 1 THEN 1 ELSE 0 END                                   AS MultiLineFlag
    FROM sales s
    LEFT OUTER JOIN base_link bl ON bl.DeliveryNote = s.DeliveryNote
	WHERE s.SupplierWarehouse in (1144,1411,1367,1366,1289,1101,943)
    GROUP BY
        s.DeliveryNote, s.DeliveryDate,
        s.AccountKey, s.AgentKey,
        s.ActionType, s.ActionTypeDesc,
        s.SupplierWarehouse, 
        s.AdjustmentFlag, s.QuantityCategory
)

,gain as(
-- ============================================================
-- Branch 1: Import / Exchange orders — cost from P_costs
-- ============================================================
SELECT
    cast(s.DeliveryNote as varchar)                                         AS DeliveryNote,
    case when row_number() over (partition by bl.PurchaseOrderID order by s.DeliveryNote asc) = 1
         then '1' else '0' end                                              AS Qty_flag,
    cast(bl.PurchaseOrderID as varchar)                                     AS PurchaseOrderID,
    cast(PC.SupplierKey as varchar)                                         AS SupplierKey,
    cast(PC.boat as varchar)                                                AS ShipID,
    PC.DocName                                                              AS Purchase_DocName,
    case when PC.DocName = 'Exchange' then sum(s.Quantity) over (partition by bl.PurchaseOrderID)
         else PC.orderquantity end                                          AS [Purchase Quantity],
    PC.ValueDate,
    s.LineType,
    s.DeliveryDate,
    FORMAT(PC.ValueDate, 'yyyy-MM')                                       AS [Year-Month],
    s.AdjustmentFlag,
    cast(s.AccountKey as varchar)                                           AS AccountKey,
    cast(s.AgentKey as varchar)                                             AS AgentKey,
    cast(s.ItemKey as varchar)                                              AS ItemKey,
    case when s.SalesType is null then s.QuantityCategory
         else s.SalesType end                                               AS [Price Term],
    s.QuantityCategory,
    s.ActionTypeDesc,
    s.Quantity,
    s.LineTotalNet_USD,
    CASE WHEN s.AdjustmentFlag <> 1 AND s.LineType = 'Item'
         THEN CAST(ROUND(s.LineTotalNet_USD / NULLIF(s.Quantity, 0), 2) AS FLOAT)
         ELSE NULL END                                                      AS price_usd,
    s.UnitNetPriceUSD,
    PC.Cif_price                                                            AS CIF_Purchase,
    PC.[demurrage / Despatch],
    PC.[Other_Expenses],
    PC.Shortage,
    CAST(ROUND(PC.DischargeCosts /
        NULLIF((PC.orderquantity - SUM(CASE WHEN s.SalesType = 'CIF' THEN s.Quantity ELSE 0 END)
                OVER (PARTITION BY bl.PurchaseOrderID)), 0), 2) AS FLOAT)  AS DischargeCost,
    CAST(ROUND(PC.Cif_price + PC.[demurrage / Despatch] + (PC.DischargeCosts /
        NULLIF((PC.orderquantity - SUM(CASE WHEN s.SalesType = 'CIF' THEN s.Quantity ELSE 0 END)
                OVER (PARTITION BY bl.PurchaseOrderID)), 0)), 2) AS FLOAT) AS FOT_Purchase,
    CAST(ROUND(CASE
        WHEN s.SalesType = 'CIF'                   AND s.LineType = 'Item'
            THEN (s.LineTotalNet_USD / NULLIF(s.Quantity, 0)) - PC.Cif_price
        WHEN s.SalesType IN ('FOT', 'FOT Premium') AND s.LineType = 'Item'
            THEN (s.LineTotalNet_USD / NULLIF(s.Quantity, 0)) - (PC.Cif_price + PC.[demurrage / Despatch] + (PC.DischargeCosts /
                 NULLIF((PC.orderquantity - SUM(CASE WHEN s.SalesType = 'CIF' THEN s.Quantity ELSE 0 END)
                         OVER (PARTITION BY bl.PurchaseOrderID)), 0)))
        ELSE 0
    END, 2) AS FLOAT)                                                       AS Gain,
    CAST(ROUND(CASE
        WHEN s.SalesType = 'CIF'                   AND s.LineType = 'Item'
            THEN ((s.LineTotalNet_USD / NULLIF(s.Quantity, 0)) - PC.Cif_price) * s.Quantity
        WHEN s.SalesType IN ('FOT', 'FOT Premium') AND s.LineType = 'Item'
            THEN ((s.LineTotalNet_USD / NULLIF(s.Quantity, 0)) - (PC.Cif_price + PC.[demurrage / Despatch] + (PC.DischargeCosts /
                 NULLIF((PC.orderquantity - SUM(CASE WHEN s.SalesType = 'CIF' THEN s.Quantity ELSE 0 END)
                         OVER (PARTITION BY bl.PurchaseOrderID)), 0)))) * s.Quantity
        ELSE 0
    END, 2) AS FLOAT)                                                       AS TotalGain,
    0                                                                       AS MultiLineFlag,  -- individual lines, not aggregated
    s.TransactionType,
    NULL                                                                    AS WeightedExpenses,
    0                                        AS [16_Fee]
FROM sales s
INNER JOIN base_link bl ON bl.DeliveryNote = s.DeliveryNote
LEFT JOIN  P_costs PC   ON PC.PurchaseOrderID = bl.PurchaseOrderID
WHERE PC.ValueDate IS NOT NULL

UNION ALL

-- ============================================================
-- Branch 2: Warehouse orders — cost from inv (LastCFPrice)
-- ============================================================
SELECT
    CAST(s.DeliveryNote AS VARCHAR)                                         AS DeliveryNote,
    case when row_number() over (partition by s.SupplierWarehouse,s.ItemKey,inv.YearMonth order by inv.YearMonth desc) = 1
         then '1' else '0' end                                              AS Qty_flag, 
    NULL                                                                    AS PurchaseOrderID,
    CAST(s.SupplierWarehouse AS VARCHAR)                                    AS SupplierKey,
    NULL                                                                    AS ShipID,
    'Warehouse'                                                             AS Purchase_DocName,
	s.Quantity AS [Purchase Quantity],
    CAST(inv.YearMonth + '-01' AS DATE)                                     AS ValueDate,  -- first day of inv month (NULL if no inv match)
    'Item'                                                                  AS LineType,
    s.DeliveryDate,
    inv.YearMonth                                  AS [Year-Month],
    s.AdjustmentFlag,
    CAST(s.AccountKey AS VARCHAR)                                           AS AccountKey,
    CAST(s.AgentKey AS VARCHAR)                                             AS AgentKey,
    CAST(s.ItemKey AS VARCHAR)                                              AS ItemKey,
    CASE WHEN s.SalesType IS NULL THEN s.QuantityCategory
         ELSE s.SalesType END                                               AS [Price Term],
    s.QuantityCategory,
    s.ActionTypeDesc,
    s.Quantity,
    s.LineTotalNet_USD,
    CASE WHEN s.AdjustmentFlag <> 1
         THEN cast(ROUND(s.LineTotalNet_USD / NULLIF(s.Quantity, 0),2) AS FLOAT)
         ELSE NULL END                                                      AS price_usd,
    s.UnitNetPriceUSD,
    NULL                                                                    AS CIF_Purchase,  -- no CIF concept for warehouse
    NULL                                                                    AS [demurrage / Despatch],
    NULL                                                                    AS [Other_Expenses],
    NULL                                                                    AS Shortage,
    NULL                                                                    AS DischargeCost,  -- fixed warehouse discharge cost
    inv.LastFOTPrice                                                        AS FOT_Purchase,
    CASE WHEN s.AdjustmentFlag <> 1
         THEN cast(ROUND((s.LineTotalNet_USD / NULLIF(s.Quantity, 0)) - (16 + COALESCE(NULLIF(inv.WeightedExpenses, 0), inv.LastFOTPrice)), 2) AS FLOAT)
         ELSE 0 END                                                         AS Gain,
    CASE WHEN s.AdjustmentFlag <> 1
         THEN cast(ROUND((s.LineTotalNet_USD / NULLIF(s.Quantity, 0)) - (16 + COALESCE(NULLIF(inv.WeightedExpenses, 0), inv.LastFOTPrice)), 2) AS FLOAT) * s.Quantity
         ELSE 0 END                                                         AS TotalGain,
    s.MultiLineFlag,
    s.TransactionType,
    inv.WeightedExpenses                                                    AS WeightedExpenses,
    16																		AS [16_Fee]
FROM WH_sales s
LEFT JOIN inv
    ON  inv.ItemKey     = s.ItemKey
    AND inv.SupplierKey = s.SupplierWarehouse
    AND inv.YearMonth   = s.[Year-Month]
LEFT JOIN CurrencyConvertion CC ON CC.TARIKH = s.DeliveryDate

	)

	select * from gain 
	--where AccountKey = 146 and LineType = 'Item' and [Year-Month] = '2026-04' 

	--select 
	--AccountKey,
	--[Year-Month],
	--ItemKey,
	--SUM(LineTotalNet_USD),
	--sum(quantity),
	--SUM(LineTotalNet_USD)/sum(quantity)
	--from gain
	--where AccountKey = 146 and LineType = 'Item' and [Year-Month] = '2026-04' 
	--group by AccountKey,[Year-Month],ItemKey 
