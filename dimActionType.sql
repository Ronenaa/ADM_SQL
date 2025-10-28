
with Actiontypes as (
select distinct
	TM.ActionType as  ActionType1,
	G.AOPI_PEILOT,
	act.ActionType as  ActionType2
from TEODOT_MSHLOCH TM 
	Left Join GORMIM G
on TM.QOD_MQBL = G.QOD_GORM
	Left join TBLT_PEOLOT_HZMNH_T_MSHLOCH act
on TM.ActionType = act.MS_AOPTSIH
)

select 
distinct
ActionType1 as 'ActionId',
CASE 
	WHEN ActionType1 = 6 THEN AOPI_PEILOT
	WHEN ActionType1 = 1 THEN 'FOT'
	WHEN ActionType1 = 11 THEN 'CIF'
	ELSE ActionType2
	END		as 'ActionTypeDesc'
from Actiontypes
order by 1