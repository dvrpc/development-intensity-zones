drop view if exists analysis.transect_step1;

create view analysis.transect_step1 as 
with 
	density_and_proximity_indices as (
        select
            b.block_group20,
            b.density_index,
            d.proximity_index, /*Put the "_index" columns next to each other and the "_index_level" columns next to each other*/
            b.density_index_level,
            d.proximity_index_level
        from analysis.density_index b
        	left join analysis.proximity_index d
            on b.block_group20 = d.block_group20
    	),
    data_for_prelim_transect_zone_column as (
    	
    	select proximity_index_levels as proximity_index_level, density_index_levels as density_index_level, prelim_transect_zone from _resources.classifications
    	
    	),
    density_and_proximity_indices_with_prelim_transect_zone_column as (
        select
            b.block_group20,
            b.density_index,
            b.proximity_index,
            b.density_index_level,
            b.proximity_index_level,
            d.prelim_transect_zone
        from density_and_proximity_indices b
        	left join data_for_prelim_transect_zone_column d
            on b.proximity_index_level = d.proximity_index_level
            and b.density_index_level = d.density_index_level
    	),
    density_and_proximity_indices_crosswalk_density_column as (
		
		select "GEOID" as block_group20, crosswalk_density from analysis.crosswalks_density_block_groups_dvrpc_2020
		
		),
	density_and_proximity_indices_with_crosswalk_density_column_too as (
        select
            b.block_group20,
            b.density_index,
            b.proximity_index,
            b.density_index_level,
            b.proximity_index_level,
            b.prelim_transect_zone,
            d.crosswalk_density
        from density_and_proximity_indices_with_prelim_transect_zone_column b
        	left join density_and_proximity_indices_crosswalk_density_column d
            on b.block_group20 = d.block_group20
    	),
    density_and_proximity_indices_geometries as (select "GEOID" as block_group20, geom from analysis.block_groups_24co_2020),
	density_and_proximity_indices_with_geometries_too as (
        select
            b.block_group20,
            b.density_index,
            b.proximity_index,
            b.density_index_level,
            b.proximity_index_level,
            b.prelim_transect_zone,
            b.crosswalk_density,
            d.geom
        from density_and_proximity_indices_with_crosswalk_density_column_too b
        	left join density_and_proximity_indices_geometries d
            on b.block_group20 = d.block_group20
    	)
    
    
    select row_number() over() as row_number, block_group20, density_index, proximity_index, density_index_level, proximity_index_level, prelim_transect_zone, crosswalk_density, geom from density_and_proximity_indices_with_geometries_too