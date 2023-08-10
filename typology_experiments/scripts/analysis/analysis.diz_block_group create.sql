drop materialized view if exists analysis.diz_block_group cascade;

create materialized view analysis.diz_block_group as /*Found out how to create a materialized view, and what it is from https://www.postgresql.org/docs/current/rules-materializedviews.html */
with 
	diz_step1 as (
		
		select block_group20, density_index, proximity_index, density_index_level, proximity_index_level, prelim_diz_zone, crosswalk_density, 
		
		case when prelim_diz_zone < 6 and prelim_diz_zone > 0 then prelim_diz_zone + 1 else 7 end as prelim_diz_zone_plus_1, 
		
		geom from analysis.diz_step1
		
		), --Found out how to conditionally create a column from https://stackoverflow.com/a/19029960 (in turn found on https://stackoverflow.com/questions/19029842/if-then-else-statements-in-postgresql )
	diz_bg_av_com_s as (
		
		select "GEOID" as block_group20, avg(number_of_stories) as average_comm_stories from analysis.costar_number_of_stories
		
		group by "GEOID"
		
		), --Used "av_com_s" in the acronym instead of "acs" because "acs" could be easily confused with American Community Survey
	diz_bg_with_av_com_s as (
        select
            b.block_group20, 
            b.density_index, 
            b.proximity_index, 
            b.density_index_level, 
            b.proximity_index_level, 
            b.prelim_diz_zone, 
            b.prelim_diz_zone_plus_1,
            b.crosswalk_density,
            d.average_comm_stories,
			b.geom
        from diz_step1 b
        	left join diz_bg_av_com_s d
            on b.block_group20 = d.block_group20
    	),
	cds as (
		
		select prelim_diz_zone as prelim_diz_zone_plus_1, percentile_50 from analysis.crosswalk_density_summary
	
		),
	diz_bg_with_bonuses_step1 as (
        select
            b.block_group20, 
            b.density_index, 
            b.proximity_index, 
            b.density_index_level, 
            b.proximity_index_level, 
            b.prelim_diz_zone, 
            b.prelim_diz_zone_plus_1,
            b.crosswalk_density,
            b.average_comm_stories,
            d.percentile_50,
            case when crosswalk_density > percentile_50 then 1 else 0 end as crosswalk_bonus_step1,
            case when average_comm_stories >= 3 and prelim_diz_zone = 5 and density_index_level = 'very high' and proximity_index_level = 'extreme' then 1 else 0 end as stories_bonus, /*Also creates stories_bonus here*/
			b.geom
        from diz_bg_with_av_com_s b
        	left join cds d
            on b.prelim_diz_zone_plus_1 = d.prelim_diz_zone_plus_1
    	),
	diz_bg_with_bonuses as (
		
		select block_group20, density_index, proximity_index, density_index_level, proximity_index_level, prelim_diz_zone, crosswalk_density, average_comm_stories,
		case when prelim_diz_zone in (1,5) then 0 else crosswalk_bonus_step1 end as crosswalk_bonus,
		stories_bonus, geom
		
		from diz_bg_with_bonuses_step1
	
		),
	diz_bg_with_diz_zone as (
		
		select block_group20, density_index, proximity_index, density_index_level, proximity_index_level, prelim_diz_zone, crosswalk_density, average_comm_stories, crosswalk_bonus, stories_bonus, 
		case when prelim_diz_zone <> 0 then prelim_diz_zone + crosswalk_bonus + stories_bonus else 0 end as diz_zone,
		geom
		
		from diz_bg_with_bonuses
	
		),
	diz_block_group as (
        select
            b.block_group20, 
            b.density_index, 
            b.proximity_index, 
            b.density_index_level, 
            b.proximity_index_level, 
            b.prelim_diz_zone, 
            b.crosswalk_density,
            b.average_comm_stories, 
            b.crosswalk_bonus,
            b.stories_bonus,
            b.diz_zone,
            d.diz_zone_name,
			b.geom
        from diz_bg_with_diz_zone b
        	left join _resources.diz_zone_names d
            on b.diz_zone = d.diz_zone
            order by block_group20
    	)
    
    
    select row_number() over() as row_number, block_group20, density_index, proximity_index, density_index_level, proximity_index_level, prelim_diz_zone, crosswalk_density, average_comm_stories, crosswalk_bonus, stories_bonus, diz_zone, diz_zone_name, geom from diz_block_group