drop table if exists analysis.unprotected_land_area;

create table analysis.unprotected_land_area as 
with 
	developable_block_group_fragments as (
		
		select "GEOID", round(cast(st_area(geom)/4046.856 as numeric), 0) as acres 
		from analysis.block_group_land_by_developability 
		where developability=1
		
		), /*Found out how to get the area of each polygon in a shapefile from https://gis.stackexchange.com/q/193564 (in turn found on https://gis.stackexchange.com/questions/193564/postgres-postgis-calculate-area-of-polygon-in-square-miles )*/
	data_for_non_pos_water_acres_column as (
		
		select "GEOID", sum(acres) as non_pos_water_acres 
		from developable_block_group_fragments
		group by "GEOID"
		
		)

		
	select
        b."GEOID",
        b."ALAND",
        d.non_pos_water_acres,
        round(cast("ALAND"/4046.856 as numeric), 0) as aland_acres,
        b.geom
    from analysis.block_groups_24co_2020 b
    	left join data_for_non_pos_water_acres_column d
        on b."GEOID" = d."GEOID"