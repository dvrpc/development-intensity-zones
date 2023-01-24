drop view if exists analysis.bones_accessibility;

create view analysis.bones_accessibility as 
with 
	bones_accessibility_step1 as (select * from analysis.bones_accessibility_step1),
	thresholds as (select levels, accessibility_thresholds from _resources.bones_thresholds),
	bones_accessibility_accessibility_level_column as (
	
		select block_group20, 'very low' as accessibility_level from bones_accessibility_step1 where accessibility_bones < (select accessibility_thresholds from thresholds where levels = 'low') union
		select block_group20, 'low' as accessibility_level from bones_accessibility_step1 where accessibility_bones >= (select accessibility_thresholds from thresholds where levels = 'low') and accessibility_bones < (select accessibility_thresholds from thresholds where levels = 'moderate') union
		select block_group20, 'moderate' as accessibility_level from bones_accessibility_step1 where accessibility_bones >= (select accessibility_thresholds from thresholds where levels = 'moderate') and accessibility_bones < (select accessibility_thresholds from thresholds where levels = 'high') union
		select block_group20, 'high' as accessibility_level from bones_accessibility_step1 where accessibility_bones >= (select accessibility_thresholds from thresholds where levels = 'high') and accessibility_bones < (select accessibility_thresholds from thresholds where levels = 'very high') union
		select block_group20, 'very high' as accessibility_level from bones_accessibility_step1 where accessibility_bones >= (select accessibility_thresholds from thresholds where levels = 'very high') and accessibility_bones < (select accessibility_thresholds from thresholds where levels = 'extreme') union
		select block_group20, 'extreme' as accessibility_level from bones_accessibility_step1 where accessibility_bones >= (select accessibility_thresholds from thresholds where levels = 'extreme') union
		select block_group20, 'low' as accessibility_level from bones_accessibility_step1 where accessibility_bones is null
		
		), /*Basically figured out how to retrieve a cell value using SQL from https://stackoverflow.com/a/56322459 (in turn found on https://stackoverflow.com/questions/56322358/postgresql-how-to-copy-the-value-of-a-cell-in-a-row-and-paste-it-into-another-c )*/
	bones_accessibility as (
        select
            b.block_group20,
            b.accessibility_bones,
            d.accessibility_level
        from bones_accessibility_step1 b
        	left join bones_accessibility_accessibility_level_column d
            on b.block_group20 = d.block_group20
    	)
    
    
    select * from bones_accessibility