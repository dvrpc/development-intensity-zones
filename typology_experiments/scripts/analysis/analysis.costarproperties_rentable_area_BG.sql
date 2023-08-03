drop table if exists analysis.costarproperties_rentable_area_bg;

create table analysis.costarproperties_rentable_area_bg as
with
	select 
	    bgd."GEOID",
	    sum(crps.rentable_building_area)/1000 as commercial_sqft
	from (
		select * from analysis.costarproperties_region_plus_surrounding crps union
		select * from _raw.not_in_costar -- AS EXAMPLES, this contains those 2 really big Costar properties, that one in University City and the GSK campus in Montco (so more similar properties can get added to this in the future if need be). _raw.not_in_costar was originally manually made by Ben Gruswitz 
		)
		join analysis.block_groups_24co_2020 bgd
	    on st_intersects(crps.geom, bgd.geom)
		group by bgd."GEOID"  /*The script was written by Brian Carney, I just changed line indents, line spacing in one instance, replaced "costarproperties_rentable_area_BG" with "costarproperties_rentable_area_bg", and added _raw.not_in_costar into this*/
