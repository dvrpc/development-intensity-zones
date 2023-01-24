drop view if exists analysis.bones_test_results_step1;

create view analysis.bones_test_results_step1 as 
with 
	bones_density_and_accessibility as (
        select
            b.block_group20,
            b.density_bones,
            d.accessibility_bones, /*Put the "_bones" columns next to each other and the "_level" columns next to each other*/
            b.density_level,
            d.accessibility_level
        from analysis.bones_density b
        	left join analysis.bones_accessibility d
            on b.block_group20 = d.block_group20
    	),
    data_for_area_type_column as (
    	
    	select accessibility_levels as accessibility_level, density_levels as density_level, area_type from _resources.bones_classifications
    	
    	),
    bones_density_and_accessibility_with_area_type_column as (
        select
            b.block_group20,
            b.density_bones,
            b.accessibility_bones,
            b.density_level,
            b.accessibility_level,
            d.area_type
        from bones_density_and_accessibility b
        	left join data_for_area_type_column d
            on b.accessibility_level = d.accessibility_level
            and b.density_level = d.density_level
    	),
    bones_density_and_accessibility_crosswalk_density_column as (
		
		select "GEOID" as block_group20, crosswalk_non_pos_water_density from analysis.crosswalks_density_block_groups_dvrpc_2020
		
		),
	bones_density_and_accessibility_with_crosswalk_density_column_too as (
        select
            b.block_group20,
            b.density_bones,
            b.accessibility_bones,
            b.density_level,
            b.accessibility_level,
            b.area_type,
            d.crosswalk_non_pos_water_density
        from bones_density_and_accessibility_with_area_type_column b
        	left join bones_density_and_accessibility_crosswalk_density_column d
            on b.block_group20 = d.block_group20
    	),
    bones_density_and_accessibility_geometries as (select "GEOID" as block_group20, geom from analysis.block_groups_24co_2020),
	bones_density_and_accessibility_with_geometries_too as (
        select
            b.block_group20,
            b.density_bones,
            b.accessibility_bones,
            b.density_level,
            b.accessibility_level,
            b.area_type,
            b.crosswalk_non_pos_water_density,
            d.geom
        from bones_density_and_accessibility_with_crosswalk_density_column_too b
        	left join bones_density_and_accessibility_geometries d
            on b.block_group20 = d.block_group20
    	)
    
    
    select row_number() over() as row_number, block_group20, density_bones, accessibility_bones, density_level, accessibility_level, area_type, crosswalk_non_pos_water_density, geom from bones_density_and_accessibility_with_geometries_too