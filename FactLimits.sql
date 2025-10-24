With InitalTable as(
Select QOD_QBOTSH as GroupID, 
TAOR_QBOTSH AS 'ProductGroup'
  FROM ADM_QBOTSOT_PRIT
)


Select
GroupID,
ProductGroup,
Case
	When GroupID = 1
		Then 30000
	When GroupID = 2
		Then 65000
	When GroupID = 5
		Then 5000
	Else 10000
End as 'MaxLimit',
Case
	When GroupID = 1
		Then -30000
	When GroupID = 2
		Then -65000
	When GroupID = 5
		Then -5000
	Else -10000
End as 'MinLimit'
From InitalTable
