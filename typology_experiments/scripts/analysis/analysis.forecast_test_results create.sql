drop view if exists analysis.forecast_test_results;

create view analysis.forecast_test_results as 
with 
	density_and_accessibility_table as (select * from analysis.forecast_density_and_accessibility),
	thresholds as (select * from _resources.test_thresholds),
	data_for_area_type_column as (select accessibility_levels as accessibility_level, density_levels as density_level, area_type from _resources.test_classifications),
	block_groups_dvrpc_2020_shp as (select "GEOID" as block_group20, geom from analysis.block_groups_dvrpc_2020),
	forecast_test_results_density_level_column as (
	
		select block_group20, density, accessibility, 'very low' as density_level from density_and_accessibility_table where density < (select density_thresholds from thresholds where levels = 'low') union
		select block_group20, density, accessibility, 'low' as density_level from density_and_accessibility_table where density >= (select density_thresholds from thresholds where levels = 'low') and density < (select density_thresholds from thresholds where levels = 'moderate') union
		select block_group20, density, accessibility, 'moderate' as density_level from density_and_accessibility_table where density >= (select density_thresholds from thresholds where levels = 'moderate') and density < (select density_thresholds from thresholds where levels = 'high') union
		select block_group20, density, accessibility, 'high' as density_level from density_and_accessibility_table where density >= (select density_thresholds from thresholds where levels = 'high') and density < (select density_thresholds from thresholds where levels = 'very high') union
		select block_group20, density, accessibility, 'very high' as density_level from density_and_accessibility_table where density >= (select density_thresholds from thresholds where levels = 'very high') union
		select block_group20, density, accessibility, 'N/A' as density_level from density_and_accessibility_table where density is null
		
		), /*Basically figured out how to retrieve a cell value using SQL from https://stackoverflow.com/a/56322459 (in turn found on https://stackoverflow.com/questions/56322358/postgresql-how-to-copy-the-value-of-a-cell-in-a-row-and-paste-it-into-another-c )*/
	block_groups_dvrpc_2020_with_density_level_column as (
        select
            b.block_group20,
            d.density,
            d.accessibility,
            d.density_level,
            b.geom
        from block_groups_dvrpc_2020_shp b
        	left join forecast_test_results_density_level_column d
            on b.block_group20 = d.block_group20
    	),
	forecast_test_results_accessibility_level_column as (
	
		select block_group20, 'very low' as accessibility_level from density_and_accessibility_table where accessibility < (select accessibility_thresholds from thresholds where levels = 'low') union
		select block_group20, 'low' as accessibility_level from density_and_accessibility_table where accessibility >= (select accessibility_thresholds from thresholds where levels = 'low') and accessibility < (select accessibility_thresholds from thresholds where levels = 'moderate') union
		select block_group20, 'moderate' as accessibility_level from density_and_accessibility_table where accessibility >= (select accessibility_thresholds from thresholds where levels = 'moderate') and accessibility < (select accessibility_thresholds from thresholds where levels = 'high') union
		select block_group20, 'high' as accessibility_level from density_and_accessibility_table where accessibility >= (select accessibility_thresholds from thresholds where levels = 'high') and accessibility < (select accessibility_thresholds from thresholds where levels = 'very high') union
		select block_group20, 'very high' as accessibility_level from density_and_accessibility_table where accessibility >= (select accessibility_thresholds from thresholds where levels = 'very high') union
		select block_group20, 'N/A' as accessibility_level from density_and_accessibility_table where accessibility is null
		
		),
	block_groups_dvrpc_2020_with_accessibility_level_column_too as (
        select
            b.block_group20,
            b.density,
            b.accessibility,
            b.density_level,
            d.accessibility_level,
            b.geom
        from block_groups_dvrpc_2020_with_density_level_column b
        	left join forecast_test_results_accessibility_level_column d
            on b.block_group20 = d.block_group20
    	),
    forecast_test_results_final as (
        select
            b.block_group20,
            b.density,
            b.accessibility,
            b.density_level,
            b.accessibility_level,
            d.area_type,
            b.geom
        from block_groups_dvrpc_2020_with_accessibility_level_column_too b
        	left join data_for_area_type_column d
            on b.accessibility_level = d.accessibility_level
            and b.density_level = d.density_level
    	)
    
    
    select * from forecast_test_results_final