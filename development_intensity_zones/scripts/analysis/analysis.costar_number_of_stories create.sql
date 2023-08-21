drop table if exists analysis.costar_number_of_stories;
create table analysis.costar_number_of_stories as

with
	costar_number_of_stories_without_bg_2020_geoids as (
		
		select 
			propertyid, 
			number_of_stories, 
			building_status, 
			year_built, 
			shape as geom 
			from _raw.costarproperties 
			where number_of_stories is not null 
			and building_status in ('Converted', 'Existing', 'Under Construction', 'Under Renovation') 
			and year_built <= 2020 union
		
		select 
			"PropertyID" as propertyid, 
			"Number_Of_Stories" as number_of_stories, 
			"Building_Status" as building_status, 
			"Year_Built" as year_built, 
			geom 
			from _raw."CoStarProperties_Surrounding_Co_2023_01" 
			where "Number_Of_Stories" is not null 
			and "Building_Status" in ('Converted', 'Existing', 'Under Construction', 'Under Renovation') 
			and "Year_Built" <= 2020
		
		)

	
	select
			b.propertyid,
			d."GEOID",
			b.number_of_stories,
			b.building_status, 
			b.year_built,
			b.geom
		from
			costar_number_of_stories_without_bg_2020_geoids b,
			analysis.block_groups_24co_2020 d
		where
			st_intersects(b.geom, d.geom) /*Basically found out how to spatial join in SQL from Sean Lawrence*/
		order by
			propertyid