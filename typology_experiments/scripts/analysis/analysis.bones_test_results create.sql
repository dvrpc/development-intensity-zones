drop view if exists analysis.bones_test_results;

create view analysis.bones_test_results as 
with 
	density_and_accessibility_table as (select * from analysis.bones_density_and_accessibility),
	thresholds as (select * from _resources.bones_thresholds),
	data_for_area_type_column as (select accessibility_levels as accessibility_level, density_levels as density_level, area_type from _resources.test_classifications),
	data_for_area_type_2050_column as (
		
		select accessibility_levels as accessibility_level_2050, density_levels as density_level_2050, area_type as area_type_2050 from _resources.test_classifications
		
		),
	block_groups_dvrpc_2020_shp as (select "GEOID" as block_group20, geom from analysis.block_groups_dvrpc_2020),
	bones_test_results_density_level_column as (
	
		select block_group20, density, accessibility, density_2050, accessibility_2050, 'very low' as density_level from density_and_accessibility_table where density < (select density_thresholds from thresholds where levels = 'low') union
		select block_group20, density, accessibility, density_2050, accessibility_2050, 'low' as density_level from density_and_accessibility_table where density >= (select density_thresholds from thresholds where levels = 'low') and density < (select density_thresholds from thresholds where levels = 'moderate') union
		select block_group20, density, accessibility, density_2050, accessibility_2050, 'moderate' as density_level from density_and_accessibility_table where density >= (select density_thresholds from thresholds where levels = 'moderate') and density < (select density_thresholds from thresholds where levels = 'high') union
		select block_group20, density, accessibility, density_2050, accessibility_2050, 'high' as density_level from density_and_accessibility_table where density >= (select density_thresholds from thresholds where levels = 'high') and density < (select density_thresholds from thresholds where levels = 'very high') union
		select block_group20, density, accessibility, density_2050, accessibility_2050, 'very high' as density_level from density_and_accessibility_table where density >= (select density_thresholds from thresholds where levels = 'very high') and density < (select density_thresholds from thresholds where levels = 'extreme') union
		select block_group20, density, accessibility, density_2050, accessibility_2050, 'extreme' as density_level from density_and_accessibility_table where density >= (select density_thresholds from thresholds where levels = 'extreme') union
		select block_group20, density, accessibility, density_2050, accessibility_2050, 'N/A' as density_level from density_and_accessibility_table where density is null
		
		), /*Note that the density_2050 and accessibility_2050 columns are brought up here only so that they can be carried over throughout the rest of the script, no operations are done on or related to those columns here. Also, basically figured out how to retrieve a cell value using SQL from https://stackoverflow.com/a/56322459 (in turn found on https://stackoverflow.com/questions/56322358/postgresql-how-to-copy-the-value-of-a-cell-in-a-row-and-paste-it-into-another-c )*/
	block_groups_dvrpc_2020_with_density_level_column as (
        select
            b.block_group20,
            d.density,
            d.accessibility,
            d.density_level,
            d.density_2050,
            d.accessibility_2050,
            b.geom
        from block_groups_dvrpc_2020_shp b
        	left join bones_test_results_density_level_column d
            on b.block_group20 = d.block_group20
    	),
	bones_test_results_accessibility_level_column as (
	
		select block_group20, 'very low' as accessibility_level from density_and_accessibility_table where accessibility < (select accessibility_thresholds from thresholds where levels = 'low') union
		select block_group20, 'low' as accessibility_level from density_and_accessibility_table where accessibility >= (select accessibility_thresholds from thresholds where levels = 'low') and accessibility < (select accessibility_thresholds from thresholds where levels = 'moderate') union
		select block_group20, 'moderate' as accessibility_level from density_and_accessibility_table where accessibility >= (select accessibility_thresholds from thresholds where levels = 'moderate') and accessibility < (select accessibility_thresholds from thresholds where levels = 'high') union
		select block_group20, 'high' as accessibility_level from density_and_accessibility_table where accessibility >= (select accessibility_thresholds from thresholds where levels = 'high') and accessibility < (select accessibility_thresholds from thresholds where levels = 'very high') union
		select block_group20, 'very high' as accessibility_level from density_and_accessibility_table where accessibility >= (select accessibility_thresholds from thresholds where levels = 'very high') and accessibility < (select accessibility_thresholds from thresholds where levels = 'extreme') union
		select block_group20, 'extreme' as accessibility_level from density_and_accessibility_table where accessibility >= (select accessibility_thresholds from thresholds where levels = 'extreme') union
		select block_group20, 'N/A' as accessibility_level from density_and_accessibility_table where accessibility is null
		
		),
	block_groups_dvrpc_2020_with_accessibility_level_column_too as (
        select
            b.block_group20,
            b.density,
            b.accessibility,
            b.density_level,
            d.accessibility_level,
            b.density_2050,
            b.accessibility_2050,
            b.geom
        from block_groups_dvrpc_2020_with_density_level_column b
        	left join bones_test_results_accessibility_level_column d
            on b.block_group20 = d.block_group20
    	),
    bones_test_results_density_level_2050_column as (
	
		select block_group20, 'very low' as density_level_2050 from density_and_accessibility_table where density_2050 < (select density_thresholds from thresholds where levels = 'low') union
		select block_group20, 'low' as density_level_2050 from density_and_accessibility_table where density_2050 >= (select density_thresholds from thresholds where levels = 'low') and density_2050 < (select density_thresholds from thresholds where levels = 'moderate') union
		select block_group20, 'moderate' as density_level_2050 from density_and_accessibility_table where density_2050 >= (select density_thresholds from thresholds where levels = 'moderate') and density_2050 < (select density_thresholds from thresholds where levels = 'high') union
		select block_group20, 'high' as density_level_2050 from density_and_accessibility_table where density_2050 >= (select density_thresholds from thresholds where levels = 'high') and density_2050 < (select density_thresholds from thresholds where levels = 'very high') union
		select block_group20, 'very high' as density_level_2050 from density_and_accessibility_table where density_2050 >= (select density_thresholds from thresholds where levels = 'very high') and density_2050 < (select density_thresholds from thresholds where levels = 'extreme') union
		select block_group20, 'extreme' as density_level_2050 from density_and_accessibility_table where density_2050 >= (select density_thresholds from thresholds where levels = 'extreme') union
		select block_group20, 'N/A' as density_level_2050 from density_and_accessibility_table where density_2050 is null
		
		),
    block_groups_dvrpc_2020_with_density_level_2050_column_too as (
        select
            b.block_group20,
            b.density,
            b.accessibility,
            b.density_level,
            b.accessibility_level,
            b.density_2050,
            b.accessibility_2050,
            d.density_level_2050,
            b.geom
        from block_groups_dvrpc_2020_with_accessibility_level_column_too b
        	left join bones_test_results_density_level_2050_column d
            on b.block_group20 = d.block_group20
    	),
    bones_test_results_accessibility_level_2050_column as (
	
		select block_group20, 'very low' as accessibility_level_2050 from density_and_accessibility_table where accessibility_2050 < (select accessibility_thresholds from thresholds where levels = 'low') union
		select block_group20, 'low' as accessibility_level_2050 from density_and_accessibility_table where accessibility_2050 >= (select accessibility_thresholds from thresholds where levels = 'low') and accessibility_2050 < (select accessibility_thresholds from thresholds where levels = 'moderate') union
		select block_group20, 'moderate' as accessibility_level_2050 from density_and_accessibility_table where accessibility_2050 >= (select accessibility_thresholds from thresholds where levels = 'moderate') and accessibility_2050 < (select accessibility_thresholds from thresholds where levels = 'high') union
		select block_group20, 'high' as accessibility_level_2050 from density_and_accessibility_table where accessibility_2050 >= (select accessibility_thresholds from thresholds where levels = 'high') and accessibility_2050 < (select accessibility_thresholds from thresholds where levels = 'very high') union
		select block_group20, 'very high' as accessibility_level_2050 from density_and_accessibility_table where accessibility_2050 >= (select accessibility_thresholds from thresholds where levels = 'very high') and accessibility_2050 < (select accessibility_thresholds from thresholds where levels = 'extreme') union
		select block_group20, 'extreme' as accessibility_level_2050 from density_and_accessibility_table where accessibility_2050 >= (select accessibility_thresholds from thresholds where levels = 'extreme') union
		select block_group20, 'N/A' as accessibility_level_2050 from density_and_accessibility_table where accessibility_2050 is null
		
		),
    block_groups_dvrpc_2020_with_accessibility_level_2050_column_too as (
        select
            b.block_group20,
            b.density,
            b.accessibility,
            b.density_level,
            b.accessibility_level,
            b.density_2050,
            b.accessibility_2050,
            b.density_level_2050,
            d.accessibility_level_2050,
            b.geom
        from block_groups_dvrpc_2020_with_density_level_2050_column_too b
        	left join bones_test_results_accessibility_level_2050_column d
            on b.block_group20 = d.block_group20
    	),
    block_groups_dvrpc_2020_with_area_type_column_too as (
        select
            b.block_group20,
            b.density,
            b.accessibility,
            b.density_level,
            b.accessibility_level,
            d.area_type,
            b.density_2050,
            b.accessibility_2050,
            b.density_level_2050,
            b.accessibility_level_2050,
            b.geom
        from block_groups_dvrpc_2020_with_accessibility_level_2050_column_too b
        	left join data_for_area_type_column d
            on b.accessibility_level = d.accessibility_level
            and b.density_level = d.density_level
    	),
    bones_test_results_before_unique_id as (
        select
            b.block_group20,
            b.density,
            b.accessibility,
            b.density_level,
            b.accessibility_level,
            b.area_type,
            b.density_2050,
            b.accessibility_2050,
            b.density_level_2050,
            b.accessibility_level_2050,
            d.area_type_2050,
            b.geom
        from block_groups_dvrpc_2020_with_area_type_column_too b
        	left join data_for_area_type_2050_column d
            on b.accessibility_level_2050 = d.accessibility_level_2050
            and b.density_level_2050 = d.density_level_2050
    	)
    
    
    select row_number() over() as row_number, block_group20, density, accessibility, density_level, accessibility_level, area_type, density_2050, accessibility_2050, density_level_2050, accessibility_level_2050, area_type_2050, geom from bones_test_results_before_unique_id