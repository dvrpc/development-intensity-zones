drop view if exists analysis.diz_step1 cascade;

create view analysis.diz_step1 as 
with 
	indices as (
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
    data_for_prelim_diz_zone_step1 as (
    	
    	select proximity_index_levels as proximity_index_level, density_index_levels as density_index_level, prelim_diz_zone as prelim_diz_zone_step1 from _resources.classifications
    	
    	),
    indices_with_prelim_diz_zone_step1 as (
        select
            b.block_group20,
            b.density_index,
            b.proximity_index,
            b.density_index_level,
            b.proximity_index_level,
            d.prelim_diz_zone_step1
        from indices b
        	left join data_for_prelim_diz_zone_step1 d
            on b.proximity_index_level = d.proximity_index_level
            and b.density_index_level = d.density_index_level
    	),
    indices_with_prelim_diz_zone as (
        
    	select block_group20, density_index, proximity_index, density_index_level, proximity_index_level,
    	case when block_group20 in (select "GEOID" from analysis.block_groups_24co_2020_area_calcs where percent_lu_h2o_pos >= 95) then 0 else prelim_diz_zone_step1 end as prelim_diz_zone
    	from indices_with_prelim_diz_zone_step1
        
        ),
    indices_crosswalk_density as (
		
		select "GEOID" as block_group20, sum(crosswalk_density) as crosswalk_density from analysis.crosswalks_density_block_groups_dvrpc_2020
		
		group by "GEOID"
		
		), /*There's at least 1 but not much more than 1 block group in analysis.crosswalks_density_block_groups_dvrpc_2020 that has more than 1 polygon, so I'll sum the crosswalk density for those*/
	indices_with_crosswalk_density as (
        select
            b.block_group20,
            b.density_index,
            b.proximity_index,
            b.density_index_level,
            b.proximity_index_level,
            b.prelim_diz_zone,
            d.crosswalk_density
        from indices_with_prelim_diz_zone b
        	left join indices_crosswalk_density d
            on b.block_group20 = d.block_group20
    	),
    indices_geometries as (select "GEOID" as block_group20, geom from analysis.block_groups_24co_2020),
	indices_with_geometries as (
        select
            b.block_group20,
            b.density_index,
            b.proximity_index,
            b.density_index_level,
            b.proximity_index_level,
            b.prelim_diz_zone,
            b.crosswalk_density,
            d.geom
        from indices_with_crosswalk_density b
        	left join indices_geometries d
            on b.block_group20 = d.block_group20
    	)
    
    
    select row_number() over() as row_number, block_group20, density_index, proximity_index, density_index_level, proximity_index_level, prelim_diz_zone, crosswalk_density, geom from indices_with_geometries