drop view if exists analysis.proximity_index;

create view analysis.proximity_index as 
with 
	proximity_index_step1 as (
    	
    	select block_group20, proximity_index,
    	
    	case when proximity_index < (select proximity_index_thresholds from _resources.thresholds where level_code = 2) then (select levels from _resources.thresholds where level_code = 1) else null end as lc_1,
    	case when proximity_index >= (select proximity_index_thresholds from _resources.thresholds where level_code = 2) and proximity_index < (select proximity_index_thresholds from _resources.thresholds where level_code = 3) then (select levels from _resources.thresholds where level_code = 2) else null end as lc_2,
    	case when proximity_index >= (select proximity_index_thresholds from _resources.thresholds where level_code = 3) and proximity_index < (select proximity_index_thresholds from _resources.thresholds where level_code = 4) then (select levels from _resources.thresholds where level_code = 3) else null end as lc_3,
    	case when proximity_index >= (select proximity_index_thresholds from _resources.thresholds where level_code = 4) and proximity_index < (select proximity_index_thresholds from _resources.thresholds where level_code = 5) then (select levels from _resources.thresholds where level_code = 4) else null end as lc_4,
    	case when proximity_index >= (select proximity_index_thresholds from _resources.thresholds where level_code = 5) and proximity_index < (select proximity_index_thresholds from _resources.thresholds where level_code = 6) then (select levels from _resources.thresholds where level_code = 5) else null end as lc_5,
    	case when proximity_index >= (select proximity_index_thresholds from _resources.thresholds where level_code = 6) then (select levels from _resources.thresholds where level_code = 6) else null end as lc_6,
    	case when proximity_index is null then 'null' else null end as "null"
    	
    	from analysis.proximity_index_step1
    	
    	),
    proximity_index_step2 as (
    	
    	select block_group20, proximity_index, concat(lc_1,lc_2,lc_3,lc_4,lc_5,lc_6,"null") as proximity_index_level_step1 from proximity_index_step1
    	
    	)

    	
	select block_group20, proximity_index, 
	case when proximity_index_level_step1='null' then null else proximity_index_level_step1 end as proximity_index_level
	from proximity_index_step2