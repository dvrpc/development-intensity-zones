drop view if exists analysis.density_index;

create view analysis.density_index as 
with 
	block_groups_24co_2020_land as (
		
		select "GEOID" as block_group20, "ALAND"/4046.856 as aland_acres from analysis.block_groups_24co_2020 where "ALAND" <> 0
		
		),
	tot_hus_2020_bg as (
		
		select concat(state,county,tract,block_group) as block_group20, p5_001n+h1_001n as housing_units_d20 from _raw.tot_pops_and_hhs_2020_bg
		
		),
	block_groups_24co_2020_land_with_tot_hus_2020_bg as (
        select
            b.block_group20,
            b.aland_acres,
            d.housing_units_d20
        from block_groups_24co_2020_land b
        	left join tot_hus_2020_bg d
            on b.block_group20 = d.block_group20
    	),
	costarproperties_rentable_area_bg as (
	
		select "GEOID" as block_group20, commercial_sqft as comm_sqft_thou_step1 from analysis.costarproperties_rentable_area_bg
		
		),
	block_groups_24co_2020_land_with_costarproperties_rentable_area_bg_too as (
        select
            b.block_group20,
            b.aland_acres,
            b.housing_units_d20,
            d.comm_sqft_thou_step1,
            case when comm_sqft_thou_step1 is null then 0 else comm_sqft_thou_step1 end as comm_sqft_thou
        from block_groups_24co_2020_land_with_tot_hus_2020_bg b
        	left join costarproperties_rentable_area_bg d
            on b.block_group20 = d.block_group20
    	),
    density_index_land_step1 as (
    	
    	select block_group20, 
    	round(cast((housing_units_d20+comm_sqft_thou)/aland_acres as numeric), 3) as density_index 
    	from block_groups_24co_2020_land_with_costarproperties_rentable_area_bg_too
    
    	),
	density_index_land_step2 as (
    	
    	select block_group20, density_index,
    	
    	case when density_index < (select density_index_thresholds from _resources.thresholds where level_code = 2) then (select levels from _resources.thresholds where level_code = 1) else null end as lc_1,
    	case when density_index >= (select density_index_thresholds from _resources.thresholds where level_code = 2) and density_index < (select density_index_thresholds from _resources.thresholds where level_code = 3) then (select levels from _resources.thresholds where level_code = 2) else null end as lc_2,
    	case when density_index >= (select density_index_thresholds from _resources.thresholds where level_code = 3) and density_index < (select density_index_thresholds from _resources.thresholds where level_code = 4) then (select levels from _resources.thresholds where level_code = 3) else null end as lc_3,
    	case when density_index >= (select density_index_thresholds from _resources.thresholds where level_code = 4) and density_index < (select density_index_thresholds from _resources.thresholds where level_code = 5) then (select levels from _resources.thresholds where level_code = 4) else null end as lc_4,
    	case when density_index >= (select density_index_thresholds from _resources.thresholds where level_code = 5) and density_index < (select density_index_thresholds from _resources.thresholds where level_code = 6) then (select levels from _resources.thresholds where level_code = 5) else null end as lc_5,
    	case when density_index >= (select density_index_thresholds from _resources.thresholds where level_code = 6) then 'level_code 6' else null end as lc_6
    	
    	from density_index_land_step1
    	
    	)

    	
    select block_group20, density_index, 
    concat(lc_1,lc_2,lc_3,lc_4,lc_5,lc_6) as density_index_level 
    from density_index_land_step2 union
    
    select "GEOID" as block_group20, null as density_index, null as density_index_level 
    from analysis.block_groups_24co_2020 
    where "ALAND" = 0