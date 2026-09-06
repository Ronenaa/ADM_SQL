-- dimPNL
SELECT
    QOD_SHROT          AS PNLKey,
    PNL                AS [PNL Code],
    SHM_SHROT          AS [PNL Description],
    p.PNL_Desc         AS [PNL Description Purchase],
    CASE
        WHEN PNL = 2270 THEN 'Discharge cost'
        WHEN PNL = 1111 THEN 'Shortage'
        WHEN PNL = 1201 THEN 'Demurrage / Despatch'
        ELSE 'Other Expenses'
    END                AS [Expense Group],
    CASE
        WHEN PNL = 2270 THEN 2
        WHEN PNL = 1111 THEN 3
        WHEN PNL = 1201 THEN 4
        ELSE 5
    END                AS [PNL Sort]
FROM HOTSAOT_SHROTIM_New h
LEFT JOIN (
    SELECT *
    FROM tblPnlList
    WHERE PNL_Type = 'OUT'
) p
  ON h.PNL = p.PNL_ID

UNION ALL

-- the extra "Purchase of inventory" row
SELECT
    999                AS PNLKey,
    '1010'             AS [PNL Code],
    ''                 AS [PNL Description],
    'Purchase of inventory' AS [PNL Description Purchase],
    'Purchase of inventory' AS [Expense Group],
    1                  AS [PNL Sort]
