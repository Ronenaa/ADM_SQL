
WITH Products_STG1 AS(
  SELECT  CAST( CONVERT(BIGINT, CONVERT(VARCHAR, M.QOD_MOTSR)) as varchar) AS 'ItemKey'
  		  ,M.QOD_MOTSR AS 'מקט'
		  ,CASE
			  WHEN CHARINDEX('=',M.TAOR_MOTSR,0) >0
			   THEN RIGHT(M.TAOR_MOTSR,Len(M.TAOR_MOTSR)-1) 
			   ELSE M.TAOR_MOTSR 
		   END AS 'שם מוצר'
  		 -- ,CASE WHEN LEFT(M.TAOR_MOTSR,1) = '='  THEN SUBSTRING(M.TAOR_MOTSR,2,20) ELSE M.TAOR_MOTSR END AS 'שם מוצר'
  		  ,CASE WHEN CHARINDEX('=',M.TAOR_MOTSR,0) >0 THEN RIGHT(M.TAOR_MOTSR,Len(M.TAOR_MOTSR)-1) ELSE M.TAOR_MOTSR  END + ' | ' + CAST( CONVERT(BIGINT, CONVERT(VARCHAR, M.QOD_MOTSR)) as varchar) AS 'מזהה מוצר'
		  ,M.I_M AS 'יחידת מידה'
		  ,M.MCHIR_MCIRH 'מחיר מכירה'
		  ,M.MTBE_MCHIR_MCIRH 'מטבע'
  		  ,M.IsActive AS 'סטטוס'
  		  ,M.SIOOG AS 'סיווג מוצר'
		  ,M.MasterProductInPurchase AS 'מוצר על'
		  ,Case 
				WHEN p.TAOR_QBOTSH is NULL OR p.TAOR_QBOTSH = '' OR p.TAOR_QBOTSH = 'OTHERS' or T.TARO_ANGLIT_1 = 'Other'
					THEN 'FEED STUFF'
				ELSE p.TAOR_QBOTSH
			END AS 'קבוצת פריט'
		  ,T.TARO_ANGLIT_1 AS 'Product'
		  ,T.TARO_ANGLIT_3_PNL_HCNSH as 'PNL'
		  ,T.TARO_ANGLIT_2_ART as 'Article'
		  ,Case 
				WHEN A.ART_DESC is NULL OR A.ART_DESC = ''
					THEN 'Other'
				ELSE A.ART_DESC
			END AS 'Article_Description'
  FROM MOTSRIM M
  LEFT JOIN ADM_QBOTSOT_PRIT P on M.GroupCode = P.QOD_QBOTSH
  LEFT JOIN (SELECT *
			FROM [dbo].[TRGOM_ANGLIT]
			WHERE SOG_PRIT_SOCN_BNQ_TM_SH_CHBRH = N'פ') T
			ON M.QOD_MOTSR = T.QOD
  LEFT JOIN (SELECT distinct [TARO_ANGLIT_2_ART],[ART_DESC]
			FROM [dbo].[TRGOM_ANGLIT]
			WHERE SOG_PRIT_SOCN_BNQ_TM_SH_CHBRH = N'פ' and ART_DESC<>'' and TARO_ANGLIT_3_PNL_HCNSH = 8052) A
			ON A.TARO_ANGLIT_2_ART = T.TARO_ANGLIT_2_ART
)
,Products_STG_TopMaterial AS (
  SELECT DISTINCT stg1.[מוצר על], stg1c.[שם מוצר],T.TARO_ANGLIT_1 as 'MainItem'
  FROM Products_STG1 stg1
  LEFT JOIN Products_STG1 stg1c on stg1.[מוצר על] = stg1c.מקט
    LEFT JOIN (SELECT *
			FROM [dbo].[TRGOM_ANGLIT]
			WHERE SOG_PRIT_SOCN_BNQ_TM_SH_CHBRH = N'פ') T
			ON stg1c.ItemKey = T.QOD
  WHERE STG1.[מוצר על]<>0
)

,STG2 AS(
	SELECT  
	'1' as 'EntityID'
	    ,STG1.ItemKey AS 'MainItemKey'
	    ,CASE 
			WHEN TM.[שם מוצר] is NULL
				THEN STG1.[Product]
			ELSE TM.[MainItem]
		END AS  'MainItemName'
		,STG1.ItemKey + '-' + STG1.ItemKey AS ItemKey
		,STG1.[שם מוצר] AS 'שם מוצר'
		,STG1.[מקט]
		,STG1.[מזהה מוצר]
		,CASE 
			WHEN TM.[מוצר על] is NULL
				THEN STG1.ItemKey
			ELSE TM.[מוצר על]
		END AS 'מוצר על'
	    ,CASE 
			WHEN TM.[שם מוצר] is NULL
				THEN STG1.[שם מוצר]
			ELSE TM.[שם מוצר]
		END AS 'שם מוצר על'
		,STG1.[יחידת מידה]
		,STG1.[מחיר מכירה]
		,STG1.[סיווג מוצר]
		,STG1.[סיווג מוצר] AS 'קטגוריה' -- Product Category Name
		,STG1.[סטטוס] AS 'סטטוס'
		,[קבוצת פריט]
		,Product
		,PNL
		,Article
		,Article_Description
		,GETDATE() AS RowInsertDatetime
FROM Products_STG1 STG1
Left Join Products_STG_TopMaterial TM ON STG1.[מוצר על]= TM.[מוצר על]


	)
	
	,STG2_1 AS (


SELECT *,
		Case
			When STG2.Article_Description = 'Other'
				THEN 100000000
			When STG2.Article_Description ='Corn'
				THEN 1
			When STG2.Article_Description ='FW'
				THEN 2
			When STG2.Article_Description ='MW'
				THEN 3
			When STG2.Article_Description ='FB'
				THEN 4
			When STG2.Article_Description ='CGFP'
				THEN 5
			When STG2.Article_Description ='DDGS'
				THEN 6
			When STG2.Article_Description ='RSM'
				THEN 7
			When STG2.Article_Description ='SFM'
				THEN 8
			When STG2.Article_Description ='BPP'
				THEN 9
			When STG2.Article_Description ='SBM'
				THEN 10
			When STG2.Article_Description ='Soybeans'
				THEN 11
			ELSE 100000000
		END AS ProductArticleOrder
		,Case
			When STG2.[קבוצת פריט] = 'Other'
				THEN 100000000
			When STG2.[קבוצת פריט] ='Grain'
				THEN 1
			When STG2.[קבוצת פריט] ='FEED STUFF'
				THEN 2
			When STG2.[קבוצת פריט] ='Soya'
				THEN 3
			ELSE 100000000
		END AS ProductGroupOrder
FROM STG2



UNION ALL

	SELECT  
	'1' as 'EntityID'
	    ,'0S' AS 'MainItemKey'
		,'כללי' AS 'MainItemName'
        ,'0-' +CAST(SIM.QOD_SHROT AS VARCHAR)+ 'S'  AS 'ItemKey'
		,SIM.SHM_SHROT  AS 'שם מוצר'
		,SIM.QOD_SHROT AS 'מקט'
		,CAST(SIM.QOD_SHROT AS VARCHAR) +' | ' + SIM.SHM_SHROT AS 'מזהה מוצר'
		,NULL AS 'מוצר על'
	    ,'כללי' AS 'שם מוצר על'
		,NULL AS 'יחידת מידה'
		,NULL AS 'מחיר מכירה'
		,'כללי' AS 'סיווג מוצר'
		,'כללי' AS 'קטגוריה'
		,NULL AS 'סטטוס'
		,'כללי' AS 'קבוצת פריט'
		,'' as Product
		,PNL as PNL
		,'' as 'Article'
		 ,'Other' AS 'Article_Description'
		,GETDATE() AS RowInsertDatetime
		,100000000 AS ProductArticleOrder
		,100000000 AS ProductGroupOrder
FROM HOTSAOT_SHROTIM_New SIM
  LEFT JOIN (SELECT *
			FROM [dbo].[TRGOM_ANGLIT]
			WHERE SOG_PRIT_SOCN_BNQ_TM_SH_CHBRH = N'פ') T
			ON SIM.QOD_SHROT = T.QOD
  LEFT JOIN (SELECT distinct [TARO_ANGLIT_2_ART],[ART_DESC]
			FROM [dbo].[TRGOM_ANGLIT]
			WHERE SOG_PRIT_SOCN_BNQ_TM_SH_CHBRH = N'פ' and ART_DESC<>'' and TARO_ANGLIT_3_PNL_HCNSH = 8052) A
			ON A.TARO_ANGLIT_2_ART = T.TARO_ANGLIT_2_ART
)
	SELECT STG2_1.*
		  ,CASE
			WHEN STG2_1.Product is NULL OR STG2_1.Product = ''
				THEN 0  
			ELSE Dense_Rank () Over (Order by ProductArticleOrder,Product)  
		END AS ProductOrder
	      ,INV.Balance AS 'Balance'
		  ,Inv.StockValue

		 
	FROM STG2_1
	
	
	----2.1
	----INVENTORY----
LEFT JOIN (
		SELECT cast(Inv.ProductID as varchar) +'-'+ cast(Inv.ProductID as varchar) as 'ItemKey'
			,SUM(TotalInventory) AS Balance
			,SUM(ROUND(CAST(PCOST.FotFlat AS decimal),2) * INV.TotalInventory) as 'StockValue'
		FROM  tblInventory Inv
        INNER join (
        SELECT MAX(DueDate) AS MD
        from tblInventory
        )DD
        ON INV.DueDate = DD.MD
        LEFT JOIN tblProductsCost PCost
        ON (INV.DueDate = PCost.DueDate
        AND INV.ProductID = PCost.ProductCode)
		GROUP BY Inv.ProductID
			
		) Inv
		ON STG2_1.ItemKey = Inv.ItemKey