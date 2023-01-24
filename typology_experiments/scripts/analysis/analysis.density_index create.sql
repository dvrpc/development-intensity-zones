drop view if exists analysis.density_index;

create view analysis.density_index as 
with 
	block_groups_24co_2020 as (select "GEOID" as block_group20, aland_acres, geom from analysis.block_groups_24co_2020),
	tot_hus_2020_bg as (
		
		select concat(state,county,tract,block_group) as block_group20, p5_001n+h1_001n as housing_units_d20 from _raw.tot_pops_and_hhs_2020_bg
		
		),
	block_groups_24co_2020_with_tot_hus_2020_bg as (
        select
            b.block_group20,
            b.aland_acres,
            d.housing_units_d20,
            b.geom
        from block_groups_24co_2020 b
        	left join tot_hus_2020_bg d
            on b.block_group20 = d.block_group20
    	),
	costarproperties_rentable_area_bg as (
	
		select "GEOID" as block_group20, commercial_sqft as comm_sqft_thou from analysis.costarproperties_rentable_area_bg
		
		),
	block_groups_24co_2020_with_costarproperties_rentable_area_bg_too as (
        select
            b.block_group20,
            b.aland_acres,
            b.housing_units_d20,
            d.comm_sqft_thou,
            b.geom
        from block_groups_24co_2020_with_tot_hus_2020_bg b
        	left join costarproperties_rentable_area_bg d
            on b.block_group20 = d.block_group20
    	),
    numerators_for_density_index_calculations_step1 as (
	
		select block_group20, sum(comm_sqft_thou) as total_comm_sqft_thou, sum(housing_units_d20) as total_housing_units from block_groups_24co_2020_with_costarproperties_rentable_area_bg_too
		
		group by block_group20
		
		),
	numerators_for_density_index_calculations as (
	
		select block_group20, total_comm_sqft_thou+total_housing_units as density_index_numerator from numerators_for_density_index_calculations_step1
		
		),
	data_for_aland_acres_column as (
	
		select block_group20, sum(aland_acres) as total_aland_acres from block_groups_24co_2020
		
		group by block_group20
		
		),
	numerators_for_density_index_calculations_with_total_aland_acres as (
        select
            b.block_group20,
            b.density_index_numerator,
            d.total_aland_acres
        from numerators_for_density_index_calculations b
        	left join data_for_aland_acres_column d
            on b.block_group20 = d.block_group20
    	),
    density_index_step1 as (
    	
    	select block_group20, density_index_numerator/total_aland_acres as density_index from numerators_for_density_index_calculations_with_total_aland_acres
    	
    	),
	thresholds as (select levels, density_index_thresholds from _resources.thresholds),
	density_index_level_column as (
	
		select block_group20, 'very low' as density_index_level from density_index_step1 where density_index < (select density_index_thresholds from thresholds where levels = 'low') union
		select block_group20, 'low' as density_index_level from density_index_step1 where density_index >= (select density_index_thresholds from thresholds where levels = 'low') and density_index < (select density_index_thresholds from thresholds where levels = 'moderate') union
		select block_group20, 'moderate' as density_index_level from density_index_step1 where density_index >= (select density_index_thresholds from thresholds where levels = 'moderate') and density_index < (select density_index_thresholds from thresholds where levels = 'high') union
		select block_group20, 'high' as density_index_level from density_index_step1 where density_index >= (select density_index_thresholds from thresholds where levels = 'high') and density_index < (select density_index_thresholds from thresholds where levels = 'very high') union
		select block_group20, 'very high' as density_index_level from density_index_step1 where density_index >= (select density_index_thresholds from thresholds where levels = 'very high') and density_index < (select density_index_thresholds from thresholds where levels = 'extreme') union
		select block_group20, 'extreme' as density_index_level from density_index_step1 where density_index >= (select density_index_thresholds from thresholds where levels = 'extreme') union
		select block_group20, 'low' as density_index_level from density_index_step1 where density_index is null
		
		), /*Basically figured out how to retrieve a cell value using SQL from https://stackoverflow.com/a/56322459 (in turn found on https://stackoverflow.com/questions/56322358/postgresql-how-to-copy-the-value-of-a-cell-in-a-row-and-paste-it-into-another-c )*/
	density_index as (
        select
            b.block_group20,
            b.density_index,
            d.density_index_level
        from density_index_step1 b
        	left join density_index_level_column d
            on b.block_group20 = d.block_group20
    	)
    
    
    select * from density_index