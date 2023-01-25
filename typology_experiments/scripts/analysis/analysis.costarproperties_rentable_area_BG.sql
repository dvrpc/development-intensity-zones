drop table if exists analysis.costarproperties_rentable_area_bg;

create table analysis.costarproperties_rentable_area_bg as
with
	costarproperties_rentable_area_bg_step1 as (
		select 
		    bgd."GEOID",
		    sum(crps.rentable_building_area)/1000 as commercial_sqft
		from analysis.costarproperties_region_plus_surrounding crps
			join analysis.block_groups_24co_2020 bgd
		    on st_intersects(crps.geom, bgd.geom)
			group by bgd."GEOID"
	),
	costarproperties_rentable_area_bg as (
		
		select * from costarproperties_rentable_area_bg_step1 where "GEOID" <> '421010369021' union
		select "GEOID", 1901812/1000 as commercial_sqft from costarproperties_rentable_area_bg_step1 where "GEOID" = '421010369021'
		
	) /*Manually updates the commercial_sqft value for that University City block group*/
    
    
    select * from costarproperties_rentable_area_bg
	