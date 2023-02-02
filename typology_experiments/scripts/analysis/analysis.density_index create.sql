drop view if exists analysis.density_index;

create view analysis.density_index as
with 
	block_groups_24co_2020 as (select "GEOID" as block_group20, "ALAND"/4046.856 as aland_acres from analysis.block_groups_24co_2020),
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
	density_index_step3 as (
    	
    	select block_group20, density_index,
    	
    	case when density_index < (select density_index_thresholds from _resources.thresholds where levels = 'low') then 'very low' else null end as very_low,
    	case when density_index >= (select density_index_thresholds from _resources.thresholds where levels = 'low') and density_index < (select density_index_thresholds from _resources.thresholds where levels = 'moderate') then 'low' else null end as low,
    	case when density_index >= (select density_index_thresholds from _resources.thresholds where levels = 'moderate') and density_index < (select density_index_thresholds from _resources.thresholds where levels = 'high') then 'moderate' else null end as moderate,
    	case when density_index >= (select density_index_thresholds from _resources.thresholds where levels = 'high') and density_index < (select density_index_thresholds from _resources.thresholds where levels = 'very high') then 'high' else null end as high,
    	case when density_index >= (select density_index_thresholds from _resources.thresholds where levels = 'very high') and density_index < (select density_index_thresholds from _resources.thresholds where levels = 'extreme') then 'very high' else null end as very_high,
    	case when density_index >= (select density_index_thresholds from _resources.thresholds where levels = 'extreme') then 'extreme' else null end as extreme,
    	case when density_index is null then 'null' else null end as "null"
    	
    	from density_index_step2
    	
    	),
    density_index_step4 as (
    	
    	select block_group20, density_index, concat(very_low,low,moderate,high,very_high,extreme,"null") as density_index_level_step1 from density_index_step3
    	
    	)

    	
	select block_group20, density_index, 
	case when density_index_level_step1='null' then null else density_index_level_step1 end as density_index_level
	from density_index_step4