drop view if exists analysis.transect;

create view analysis.transect as
with 
	transect_step1 as (
		
		select block_group20, density_index, proximity_index, density_index_level, proximity_index_level, prelim_transect_zone, crosswalk_density, 
		
		case when prelim_transect_zone < 6 then prelim_transect_zone + 1 else 7 end as prelim_transect_zone_plus_1, 
		
		geom from analysis.transect_step1
		
		), /*Found out how to conditionally create a column from https://stackoverflow.com/a/19029960 (in turn found on https://stackoverflow.com/questions/19029842/if-then-else-statements-in-postgresql )*/
	crosswalk_density_summary as (select * from analysis.crosswalk_density_summary),
	transect_additional_columns_step1 as (
		
		select block_group20, density_index_level, proximity_index_level, prelim_transect_zone, prelim_transect_zone_plus_1, crosswalk_density,
		
		case when crosswalk_density > (select percentile_40 from crosswalk_density_summary where prelim_transect_zone = prelim_transect_zone_plus_1) then 1 else 0 end as crosswalk_bonus_step1
		
		from transect_step1
	
		),
	transect_additional_columns_step2 as (
		
		select block_group20, density_index_level, proximity_index_level, prelim_transect_zone, prelim_transect_zone_plus_1, crosswalk_density, crosswalk_bonus_step1,
		
		case when prelim_transect_zone in (1,5) then 0 else crosswalk_bonus_step1 end as crosswalk_bonus
		
		from transect_additional_columns_step1
	
		),	
	costar_number_of_stories as (select "GEOID" as block_group20, number_of_stories from analysis.costar_number_of_stories),
	average_stories_for_each_block_group as (
		
		select block_group20, avg(number_of_stories) as average_comm_stories from costar_number_of_stories
		
		group by block_group20
		
		),
	transect_additional_columns_step3 as (
        select
            b.block_group20,
            b.density_index_level, 
            b.proximity_index_level,
            b.prelim_transect_zone, 
            b.prelim_transect_zone_plus_1,
            b.crosswalk_density,
            b.crosswalk_bonus_step1,
            d.average_comm_stories, /*average_comm_stories comes to the left of crosswalk_bonus*/
            b.crosswalk_bonus,
            case when average_comm_stories >= 3 and prelim_transect_zone = 5 and density_index_level = 'very high' and proximity_index_level = 'extreme' then 1 else 0 end as stories_bonus /*Also creates stories_bonus here*/
        from transect_additional_columns_step2 b
        	left join average_stories_for_each_block_group d
            on b.block_group20 = d.block_group20
    	),
	transect_additional_columns as (
        
		select block_group20, density_index_level, proximity_index_level, prelim_transect_zone, prelim_transect_zone_plus_1, crosswalk_density, crosswalk_bonus_step1, average_comm_stories, crosswalk_bonus, stories_bonus,
		
		prelim_transect_zone + crosswalk_bonus + stories_bonus as transect_zone
		
		from transect_additional_columns_step3
		
		),
	transect_without_transect_zone_names as (
        select
            b.block_group20, 
            b.density_index, 
            b.proximity_index, 
            b.density_index_level, 
            b.proximity_index_level, 
            b.prelim_transect_zone, 
            b.crosswalk_density,
            d.average_comm_stories, 
            d.crosswalk_bonus,
            d.stories_bonus,
            d.transect_zone,
			b.geom
        from transect_step1 b
        	left join transect_additional_columns d
            on b.block_group20 = d.block_group20
    	),
	transect_zone_names as (select * from _resources.transect_zone_names),
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
        from transect_without_transect_zone_names b
        	left join transect_zone_names d
            on b.transect_zone = d.transect_zone
            order by block_group20
    	)
    
    
    select row_number() over() as row_number, block_group20, density_index, proximity_index, density_index_level, proximity_index_level, prelim_transect_zone, crosswalk_density, average_comm_stories, crosswalk_bonus, stories_bonus, transect_zone, transect_zone_name, geom from transect