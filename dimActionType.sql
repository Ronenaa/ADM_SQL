with actiontype as(
select distinct
	TM.ActionType as Id,
	case
		when TM.ActionType = 1 then 'FOT'
		when TM.ActionType = 11 then 'CIF'
		else act.ActionType
		end as 'ActionType'
from TEODOT_MSHLOCH TM 
	Left Join GORMIM G
on TM.QOD_MQBL = G.QOD_GORM
	Left join TBLT_PEOLOT_HZMNH_T_MSHLOCH act
on TM.ActionType = act.MS_AOPTSIH
where act.ActionType is not null
)

select *
,
RANK() over (order by id asc) as Sort
from actiontype