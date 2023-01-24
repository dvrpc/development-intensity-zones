drop view if exists analysis.proximity_index;

create view analysis.proximity_index as 
with 
	proximity_index_step1 as (select * from analysis.proximity_index_step1),
	thresholds as (select levels, proximity_index_thresholds from _resources.thresholds),
	proximity_index_level_column as (
	
		select block_group20, 'very low' as proximity_index_level from proximity_index_step1 where proximity_index < (select proximity_index_thresholds from thresholds where levels = 'low') union
		select block_group20, 'low' as proximity_index_level from proximity_index_step1 where proximity_index >= (select proximity_index_thresholds from thresholds where levels = 'low') and proximity_index < (select proximity_index_thresholds from thresholds where levels = 'moderate') union
		select block_group20, 'moderate' as proximity_index_level from proximity_index_step1 where proximity_index >= (select proximity_index_thresholds from thresholds where levels = 'moderate') and proximity_index < (select proximity_index_thresholds from thresholds where levels = 'high') union
		select block_group20, 'high' as proximity_index_level from proximity_index_step1 where proximity_index >= (select proximity_index_thresholds from thresholds where levels = 'high') and proximity_index < (select proximity_index_thresholds from thresholds where levels = 'very high') union
		select block_group20, 'very high' as proximity_index_level from proximity_index_step1 where proximity_index >= (select proximity_index_thresholds from thresholds where levels = 'very high') and proximity_index < (select proximity_index_thresholds from thresholds where levels = 'extreme') union
		select block_group20, 'extreme' as proximity_index_level from proximity_index_step1 where proximity_index >= (select proximity_index_thresholds from thresholds where levels = 'extreme') union
		select block_group20, 'low' as proximity_index_level from proximity_index_step1 where proximity_index is null
		
		), /*Basically figured out how to retrieve a cell value using SQL from https://stackoverflow.com/a/56322459 (in turn found on https://stackoverflow.com/questions/56322358/postgresql-how-to-copy-the-value-of-a-cell-in-a-row-and-paste-it-into-another-c )*/
	proximity_index as (
        select
            b.block_group20,
            b.proximity_index,
            d.proximity_index_level
        from proximity_index_step1 b
        	left join proximity_index_level_column d
            on b.block_group20 = d.block_group20
    	)
    
    
    select * from proximity_index