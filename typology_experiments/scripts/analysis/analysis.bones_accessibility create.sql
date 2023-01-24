drop view if exists analysis.bones_accessibility;

create view analysis.bones_accessibility as 
with 
	block_groups_24co_2020 as (select "GEOID" as block_group20, aland_acres, geom from analysis.block_groups_24co_2020),
	gq_hu_table as (
	
		select concat(state,county,tract,block) as block_group20, p5_001n+h1_001n as gq_hu from _raw.tot_pops_and_hhs_2020_block
		
		),
	block_centroids_2020_geometries as (
	
		select "GEOID20" as block_group20, geom from analysis.block_centroids_2020_with_2020_decennial_pop_and_hhs
		
		),
	gq_hu_table_with_geometries as (
        select
            b.block_group20,
            b.gq_hu,
            d.geom
        from gq_hu_table b
        	left join block_centroids_2020_geometries d
            on b.block_group20 = d.block_group20
    	),
	costar_property_locations as (
	
		select rentable_building_area/1000 as comm_sqft_thou, geom from analysis.costarproperties_region_plus_surrounding
		
		),
	buffers_2_mile as (
	
		select "GEOID" as block_group20_2mibuff, buff_mi, geom from analysis.incorp_del_river_bg_centroids_24co_2020_buffers where buff_mi = 2
		
		),
	costar_property_locations_2mibuffers_spatial_join as (
		select
			b.comm_sqft_thou,
			d.block_group20_2mibuff,
			b.geom
		from
			costar_property_locations b,
			buffers_2_mile d
		where
			st_intersects(b.geom, d.geom) /*Basically found out how to spatial join in SQL from Sean Lawrence*/ 
		),
	data_for_tot_comm_sqft_thou_2mi_column as (
	
		select block_group20_2mibuff as block_group20, sum(comm_sqft_thou) as tot_comm_sqft_thou_2mi from costar_property_locations_2mibuffers_spatial_join
		
		group by block_group20_2mibuff
		
		),
	buffers_5_mile as (
	
		select "GEOID" as block_group20_5mibuff, buff_mi, geom from analysis.incorp_del_river_bg_centroids_24co_2020_buffers where buff_mi = 5
		
		),
	gq_hu_table_with_geometries_5mibuffers_spatial_join as (
		select
			b.block_group20,
            b.gq_hu,
			d.block_group20_5mibuff,
            b.geom
		from
			gq_hu_table_with_geometries b,
			buffers_5_mile d
		where
			st_intersects(b.geom, d.geom) 
		),
	data_for_tot_gq_hu_5mi_column as (
	
		select block_group20_5mibuff as block_group20, sum(gq_hu) as tot_gq_hu_5mi from gq_hu_table_with_geometries_5mibuffers_spatial_join
		
		group by block_group20_5mibuff
		
		),
	data_for_accessibility_bones_column as (
        select
            b.block_group20,
            b.tot_comm_sqft_thou_2mi,
            d.tot_gq_hu_5mi
        from data_for_tot_comm_sqft_thou_2mi_column b
        	left join data_for_tot_gq_hu_5mi_column d
            on b.block_group20 = d.block_group20
    	),
    bones_accessibility_step1 as (
    	
    	select block_group20, ((tot_comm_sqft_thou_2mi*tot_gq_hu_5mi)*2)/(tot_comm_sqft_thou_2mi+tot_gq_hu_5mi) as accessibility_bones from data_for_accessibility_bones_column
    	
    	),
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