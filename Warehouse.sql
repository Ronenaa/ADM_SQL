--Inventory--
--with inv as (....)

SELECT
    FINAL.ItemKey,
    FINAL.SupplierKey,
    CONVERT(char(7), FINAL.Date, 126) AS YearMonth,

    SUM(FINAL.Quantity) AS TotalQuantity,

    CAST(ROUND(MAX(CASE WHEN FINAL.rn = 1 THEN FINAL.UnitPrice END), 2) AS FLOAT) AS LastUnitPrice,
    CAST(ROUND(MAX(CASE WHEN FINAL.rn = 1 THEN FINAL.FOTprice END), 2) AS FLOAT) AS LastFOTPrice,
    CAST(ROUND(MAX(CASE WHEN FINAL.rn = 1 THEN FINAL.CFPrice END), 2) AS FLOAT) AS LastCFPrice

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
                CAST(SUBSTRING(T_TNOEH,1,4) + '-' + SUBSTRING(T_TNOEH,5,2) + '-' + SUBSTRING(T_TNOEH,7,2) AS DATE) AS Date,
                SUM(TM.CMOT_LOGOS) AS Quantity
            FROM BT.dbo.TNOEOT_MLAI_CLLI TM
            WHERE QOD_GORM <> 1
              AND CAST(SUBSTRING(TM.T_TNOEH,1,4) AS INT) BETWEEN 2018 AND YEAR(GETDATE())
              AND SOG_TNOEH_CN_ITS_SP_HE LIKE N'%כ%'
            GROUP BY TM.QOD_PRIT, TM.T_TNOEH

            UNION ALL

            SELECT 
                TM.QOD_PRIT,
                CAST(SUBSTRING(T_TNOEH,1,4) + '-' + SUBSTRING(T_TNOEH,5,2) + '-' + SUBSTRING(T_TNOEH,7,2) AS DATE),
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

ORDER BY
    FINAL.SupplierKey,
    FINAL.ItemKey,
    YearMonth;