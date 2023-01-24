drop view if exists analysis.bones_density;

create view analysis.bones_density as 
with 
	bones_density_step1 as (select * from analysis.bones_density_step1),
	thresholds as (select levels, density_thresholds from _resources.bones_thresholds),
	bones_density_density_level_column as (
	
		select block_group20, 'very low' as density_level from bones_density_step1 where density_bones < (select density_thresholds from thresholds where levels = 'low') union
		select block_group20, 'low' as density_level from bones_density_step1 where density_bones >= (select density_thresholds from thresholds where levels = 'low') and density_bones < (select density_thresholds from thresholds where levels = 'moderate') union
		select block_group20, 'moderate' as density_level from bones_density_step1 where density_bones >= (select density_thresholds from thresholds where levels = 'moderate') and density_bones < (select density_thresholds from thresholds where levels = 'high') union
		select block_group20, 'high' as density_level from bones_density_step1 where density_bones >= (select density_thresholds from thresholds where levels = 'high') and density_bones < (select density_thresholds from thresholds where levels = 'very high') union
		select block_group20, 'very high' as density_level from bones_density_step1 where density_bones >= (select density_thresholds from thresholds where levels = 'very high') and density_bones < (select density_thresholds from thresholds where levels = 'extreme') union
		select block_group20, 'extreme' as density_level from bones_density_step1 where density_bones >= (select density_thresholds from thresholds where levels = 'extreme') union
		select block_group20, 'low' as density_level from bones_density_step1 where density_bones is null
		
		), /*Basically figured out how to retrieve a cell value using SQL from https://stackoverflow.com/a/56322459 (in turn found on https://stackoverflow.com/questions/56322358/postgresql-how-to-copy-the-value-of-a-cell-in-a-row-and-paste-it-into-another-c )*/
	bones_density as (
        select
            b.block_group20,
            b.density_bones,
            d.density_level
        from bones_density_step1 b
        	left join bones_density_density_level_column d
            on b.block_group20 = d.block_group20
    	)
    
    
    select * from bones_density