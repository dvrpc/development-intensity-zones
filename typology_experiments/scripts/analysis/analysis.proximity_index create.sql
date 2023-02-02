drop view if exists analysis.proximity_index;

create view analysis.proximity_index as 
with 
	proximity_index_step1 as (
    	
    	select block_group20, proximity_index,
    	
    	case when proximity_index < (select proximity_index_thresholds from _resources.thresholds where levels = 'low') then 'very low' else null end as very_low,
    	case when proximity_index >= (select proximity_index_thresholds from _resources.thresholds where levels = 'low') and proximity_index < (select proximity_index_thresholds from _resources.thresholds where levels = 'moderate') then 'low' else null end as low,
    	case when proximity_index >= (select proximity_index_thresholds from _resources.thresholds where levels = 'moderate') and proximity_index < (select proximity_index_thresholds from _resources.thresholds where levels = 'high') then 'moderate' else null end as moderate,
    	case when proximity_index >= (select proximity_index_thresholds from _resources.thresholds where levels = 'high') and proximity_index < (select proximity_index_thresholds from _resources.thresholds where levels = 'very high') then 'high' else null end as high,
    	case when proximity_index >= (select proximity_index_thresholds from _resources.thresholds where levels = 'very high') and proximity_index < (select proximity_index_thresholds from _resources.thresholds where levels = 'extreme') then 'very high' else null end as very_high,
    	case when proximity_index >= (select proximity_index_thresholds from _resources.thresholds where levels = 'extreme') then 'extreme' else null end as extreme,
    	case when proximity_index is null then 'null' else null end as "null"
    	
    	from analysis.proximity_index_step1
    	
    	),
    proximity_index_step2 as (
    	
    	select block_group20, proximity_index, concat(very_low,low,moderate,high,very_high,extreme,"null") as proximity_index_level_step1 from proximity_index_step1
    	
    	)

    	
	select block_group20, proximity_index, 
	case when proximity_index_level_step1='null' then null else proximity_index_level_step1 end as proximity_index_level
	from proximity_index_step2