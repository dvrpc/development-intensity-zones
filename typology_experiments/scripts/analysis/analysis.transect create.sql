drop materialized view if exists analysis.transect;

create materialized view analysis.transect as /*Found out how to create a materialized view, and what it is from https://www.postgresql.org/docs/current/rules-materializedviews.html */
with 
	transect_step1 as (
		
		select block_group20, density_index, proximity_index, density_index_level, proximity_index_level, prelim_transect_zone, crosswalk_density, 
		
		case when prelim_transect_zone < 6 and prelim_transect_zone > 0 then prelim_transect_zone + 1 else 7 end as prelim_transect_zone_plus_1, 
		
		geom from analysis.transect_step1
		
		), /*Found out how to conditionally create a column from https://stackoverflow.com/a/19029960 (in turn found on https://stackoverflow.com/questions/19029842/if-then-else-statements-in-postgresql )*/
	transect_average_comm_stories_column as (
		
		select "GEOID" as block_group20, avg(number_of_stories) as average_comm_stories from analysis.costar_number_of_stories
		
		group by "GEOID"
		
		),
	transect_with_average_comm_stories_column as (
        select
            b.block_group20, 
            b.density_index, 
            b.proximity_index, 
            b.density_index_level, 
            b.proximity_index_level, 
            b.prelim_transect_zone, 
            b.prelim_transect_zone_plus_1,
            b.crosswalk_density,
            d.average_comm_stories,
			b.geom
        from transect_step1 b
        	left join transect_average_comm_stories_column d
            on b.block_group20 = d.block_group20
    	),
	crosswalk_density_summary as (
		
		select prelim_transect_zone as prelim_transect_zone_plus_1, percentile_40 from analysis.crosswalk_density_summary
	
		),
	transect_with_crosswalk_bonus_and_stories_bonus_columns_too_step1 as (
        select
            b.block_group20, 
            b.density_index, 
            b.proximity_index, 
            b.density_index_level, 
            b.proximity_index_level, 
            b.prelim_transect_zone, 
            b.prelim_transect_zone_plus_1,
            b.crosswalk_density,
            b.average_comm_stories,
            d.percentile_40,
            case when crosswalk_density > percentile_40 then 1 else 0 end as crosswalk_bonus_step1,
            case when average_comm_stories >= 3 and prelim_transect_zone = 5 and density_index_level = 'very high' and proximity_index_level = 'extreme' then 1 else 0 end as stories_bonus, /*Also creates stories_bonus here*/
			b.geom
        from transect_with_average_comm_stories_column b
        	left join crosswalk_density_summary d
            on b.prelim_transect_zone_plus_1 = d.prelim_transect_zone_plus_1
    	),
	transect_with_crosswalk_bonus_and_stories_bonus_columns_too as (
		
		select block_group20, density_index, proximity_index, density_index_level, proximity_index_level, prelim_transect_zone, crosswalk_density, average_comm_stories,
		case when prelim_transect_zone in (1,5) then 0 else crosswalk_bonus_step1 end as crosswalk_bonus,
		stories_bonus, geom
		
		from transect_with_crosswalk_bonus_and_stories_bonus_columns_too_step1
	
		),
	transect_with_transect_zone_column_too as (
		
		select block_group20, density_index, proximity_index, density_index_level, proximity_index_level, prelim_transect_zone, crosswalk_density, average_comm_stories, crosswalk_bonus, stories_bonus, 
		case when prelim_transect_zone <> 0 then prelim_transect_zone + crosswalk_bonus + stories_bonus else 0 end as transect_zone,
		geom
		
		from transect_with_crosswalk_bonus_and_stories_bonus_columns_too
	
		),
	transect as (
        select
            b.block_group20, 
            b.density_index, 
            b.proximity_index, 
            b.density_index_level, 
            b.proximity_index_level, 
            b.prelim_transect_zone, 
            b.crosswalk_density,
            b.average_comm_stories, 
            b.crosswalk_bonus,
            b.stories_bonus,
            b.transect_zone,
            d.transect_zone_name,
			b.geom
        from transect_with_transect_zone_column_too b
        	left join _resources.transect_zone_names d
            on b.transect_zone = d.transect_zone
            order by block_group20
    	)
    
    
    select row_number() over() as row_number, block_group20, density_index, proximity_index, density_index_level, proximity_index_level, prelim_transect_zone, crosswalk_density, average_comm_stories, crosswalk_bonus, stories_bonus, transect_zone, transect_zone_name, geom from transect