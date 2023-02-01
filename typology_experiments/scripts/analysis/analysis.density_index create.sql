drop view if exists analysis.density_index;

create view analysis.density_index as
with 
	block_groups_24co_2020 as (select "GEOID" as block_group20, aland_acres from analysis.block_groups_24co_2020),
	tot_hus_2020_bg as (
		
		select concat(state,county,tract,block_group) as block_group20, p5_001n+h1_001n as housing_units_d20 from _raw.tot_pops_and_hhs_2020_bg
		
		),
	block_groups_24co_2020_with_tot_hus_2020_bg as (
        select
            b.block_group20,
            b.aland_acres,
            d.housing_units_d20
        from block_groups_24co_2020 b
        	left join tot_hus_2020_bg d
            on b.block_group20 = d.block_group20
    	),
	costarproperties_rentable_area_bg as (
	
		select "GEOID" as block_group20, commercial_sqft as comm_sqft_thou_step1 from analysis.costarproperties_rentable_area_bg
		
		),
	block_groups_24co_2020_with_costarproperties_rentable_area_bg_too as (
        select
            b.block_group20,
            b.aland_acres,
            b.housing_units_d20,
            d.comm_sqft_thou_step1
        from block_groups_24co_2020_with_tot_hus_2020_bg b
        	left join costarproperties_rentable_area_bg d
            on b.block_group20 = d.block_group20
    	),
    density_index_step1 as (
    	
    	select block_group20, aland_acres, housing_units_d20, 
    	case when comm_sqft_thou_step1 is null then 0 else comm_sqft_thou_step1 end as comm_sqft_thou 
    	from block_groups_24co_2020_with_costarproperties_rentable_area_bg_too
    
    	),
    density_index_step2 as (
    	
    	select block_group20, 
    	case when aland_acres <> 0 then round(cast((housing_units_d20+comm_sqft_thou)/aland_acres as numeric), 3) else null end as density_index 
    	from density_index_step1
    	
    	),
	density_index_level_column as (
	
		select block_group20, 'very low' as density_index_level from density_index_step2 where density_index < (select density_index_thresholds from _resources.thresholds where levels = 'low') union
		select block_group20, 'low' as density_index_level from density_index_step2 where density_index >= (select density_index_thresholds from _resources.thresholds where levels = 'low') and density_index < (select density_index_thresholds from _resources.thresholds where levels = 'moderate') union
		select block_group20, 'moderate' as density_index_level from density_index_step2 where density_index >= (select density_index_thresholds from _resources.thresholds where levels = 'moderate') and density_index < (select density_index_thresholds from _resources.thresholds where levels = 'high') union
		select block_group20, 'high' as density_index_level from density_index_step2 where density_index >= (select density_index_thresholds from _resources.thresholds where levels = 'high') and density_index < (select density_index_thresholds from _resources.thresholds where levels = 'very high') union
		select block_group20, 'very high' as density_index_level from density_index_step2 where density_index >= (select density_index_thresholds from _resources.thresholds where levels = 'very high') and density_index < (select density_index_thresholds from _resources.thresholds where levels = 'extreme') union
		select block_group20, 'extreme' as density_index_level from density_index_step2 where density_index >= (select density_index_thresholds from _resources.thresholds where levels = 'extreme') union
		select block_group20, null as density_index_level from density_index_step2 where density_index is null
		
		) /*Basically figured out how to retrieve a cell value using SQL from https://stackoverflow.com/a/56322459 (in turn found on https://stackoverflow.com/questions/56322358/postgresql-how-to-copy-the-value-of-a-cell-in-a-row-and-paste-it-into-another-c )*/
	
	
    select
        b.block_group20,
        b.density_index,
        d.density_index_level
    from density_index_step2 b
    	left join density_index_level_column d
        on b.block_group20 = d.block_group20