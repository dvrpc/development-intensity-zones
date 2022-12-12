drop view if exists analysis.bones_test_results;

create view analysis.bones_test_results as 
with 
	density_and_accessibility_table_without_crosswalk_density_columns as (
		
		select block_group20, density_bones, accessibility_bones from analysis.bones_density_and_accessibility
		
		),
	crosswalk_density_columns as (
		
		select "GEOID" as block_group20, crosswalk_aland_density, crosswalk_non_pos_water_density from analysis.crosswalks_density_block_groups_dvrpc_2020
		
		),
	density_and_accessibility_table as (
        select
            b.block_group20,
            b.density_bones,
            b.accessibility_bones,
            d.crosswalk_aland_density, 
            d.crosswalk_non_pos_water_density
        from density_and_accessibility_table_without_crosswalk_density_columns b
        	left join crosswalk_density_columns d
            on b.block_group20 = d.block_group20
    	),
	thresholds as (select * from _resources.bones_thresholds),
	data_for_area_type_column as (select accessibility_levels as accessibility_level, density_levels as density_level, area_type from _resources.bones_classifications),
	block_groups_dvrpc_2020_shp as (select "GEOID" as block_group20, geom from analysis.block_groups_dvrpc_2020),
	bones_test_results_density_level_column as (
	
		select block_group20, density_bones, accessibility_bones, crosswalk_aland_density, crosswalk_non_pos_water_density, 'very low' as density_level from density_and_accessibility_table where density_bones < (select density_thresholds from thresholds where levels = 'low') union
		select block_group20, density_bones, accessibility_bones, crosswalk_aland_density, crosswalk_non_pos_water_density, 'low' as density_level from density_and_accessibility_table where density_bones >= (select density_thresholds from thresholds where levels = 'low') and density_bones < (select density_thresholds from thresholds where levels = 'moderate') union
		select block_group20, density_bones, accessibility_bones, crosswalk_aland_density, crosswalk_non_pos_water_density, 'moderate' as density_level from density_and_accessibility_table where density_bones >= (select density_thresholds from thresholds where levels = 'moderate') and density_bones < (select density_thresholds from thresholds where levels = 'high') union
		select block_group20, density_bones, accessibility_bones, crosswalk_aland_density, crosswalk_non_pos_water_density, 'high' as density_level from density_and_accessibility_table where density_bones >= (select density_thresholds from thresholds where levels = 'high') and density_bones < (select density_thresholds from thresholds where levels = 'very high') union
		select block_group20, density_bones, accessibility_bones, crosswalk_aland_density, crosswalk_non_pos_water_density, 'very high' as density_level from density_and_accessibility_table where density_bones >= (select density_thresholds from thresholds where levels = 'very high') and density_bones < (select density_thresholds from thresholds where levels = 'extreme') union
		select block_group20, density_bones, accessibility_bones, crosswalk_aland_density, crosswalk_non_pos_water_density, 'extreme' as density_level from density_and_accessibility_table where density_bones >= (select density_thresholds from thresholds where levels = 'extreme') union
		select block_group20, density_bones, accessibility_bones, crosswalk_aland_density, crosswalk_non_pos_water_density, 'low' as density_level from density_and_accessibility_table where density_bones is null
		
		), /*Note that the density_bones, accessibility_bones, crosswalk_aland_density, and crosswalk_non_pos_water_density columns are brought up here only so that they can be carried over throughout the rest of the script, no operations are done on or related to those columns here. Also, basically figured out how to retrieve a cell value using SQL from https://stackoverflow.com/a/56322459 (in turn found on https://stackoverflow.com/questions/56322358/postgresql-how-to-copy-the-value-of-a-cell-in-a-row-and-paste-it-into-another-c )*/
	block_groups_dvrpc_2020_with_density_level_column as (
        select
            b.block_group20,
            d.density_bones,
            d.accessibility_bones,
            d.density_level,
            d.crosswalk_aland_density, 
            d.crosswalk_non_pos_water_density,
            b.geom
        from block_groups_dvrpc_2020_shp b
        	left join bones_test_results_density_level_column d
            on b.block_group20 = d.block_group20
    	),
	bones_test_results_accessibility_level_column as (
	
		select block_group20, 'very low' as accessibility_level from density_and_accessibility_table where accessibility_bones < (select accessibility_thresholds from thresholds where levels = 'low') union
		select block_group20, 'low' as accessibility_level from density_and_accessibility_table where accessibility_bones >= (select accessibility_thresholds from thresholds where levels = 'low') and accessibility_bones < (select accessibility_thresholds from thresholds where levels = 'moderate') union
		select block_group20, 'moderate' as accessibility_level from density_and_accessibility_table where accessibility_bones >= (select accessibility_thresholds from thresholds where levels = 'moderate') and accessibility_bones < (select accessibility_thresholds from thresholds where levels = 'high') union
		select block_group20, 'high' as accessibility_level from density_and_accessibility_table where accessibility_bones >= (select accessibility_thresholds from thresholds where levels = 'high') and accessibility_bones < (select accessibility_thresholds from thresholds where levels = 'very high') union
		select block_group20, 'very high' as accessibility_level from density_and_accessibility_table where accessibility_bones >= (select accessibility_thresholds from thresholds where levels = 'very high') and accessibility_bones < (select accessibility_thresholds from thresholds where levels = 'extreme') union
		select block_group20, 'extreme' as accessibility_level from density_and_accessibility_table where accessibility_bones >= (select accessibility_thresholds from thresholds where levels = 'extreme') union
		select block_group20, 'low' as accessibility_level from density_and_accessibility_table where accessibility_bones is null
		
		),
	block_groups_dvrpc_2020_with_accessibility_level_column_too as (
        select
            b.block_group20,
            b.density_bones,
            b.accessibility_bones,
            b.density_level,
            d.accessibility_level,
            b.crosswalk_aland_density, 
            b.crosswalk_non_pos_water_density,
            b.geom
        from block_groups_dvrpc_2020_with_density_level_column b
        	left join bones_test_results_accessibility_level_column d
            on b.block_group20 = d.block_group20
    	),
    block_groups_dvrpc_2020_with_area_type_column_too as (
        select
            b.block_group20,
            b.density_bones,
            b.accessibility_bones,
            b.density_level,
            b.accessibility_level,
            d.area_type,
            b.crosswalk_aland_density, 
            b.crosswalk_non_pos_water_density,
            b.geom
        from block_groups_dvrpc_2020_with_accessibility_level_column_too b
        	left join data_for_area_type_column d
            on b.accessibility_level = d.accessibility_level
            and b.density_level = d.density_level
    	)
    
    
    select row_number() over() as row_number, block_group20, density_bones, accessibility_bones, density_level, accessibility_level, area_type, crosswalk_aland_density, crosswalk_non_pos_water_density, geom from block_groups_dvrpc_2020_with_area_type_column_too