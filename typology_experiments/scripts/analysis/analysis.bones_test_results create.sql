drop view if exists analysis.bones_test_results;

create view analysis.bones_test_results as 
with 
	bones_test_results_step1 as (
		
		select block_group20, density_bones, accessibility_bones, density_level, accessibility_level, area_type, crosswalk_non_pos_water_density, 
		
		case when area_type < 6 then area_type + 1 else 7 end as area_type_plus_1, 
		
		geom from analysis.bones_test_results_step1
		
		), /*Found out how to conditionally create a column from https://stackoverflow.com/a/19029960 (in turn found on https://stackoverflow.com/questions/19029842/if-then-else-statements-in-postgresql )*/
	crosswalk_density_summary as (select * from analysis.crosswalk_density_summary),
	bones_test_results_additional_columns_step1 as (
		
		select block_group20, area_type, area_type_plus_1, crosswalk_non_pos_water_density,
		
		case when crosswalk_non_pos_water_density > (select percentile_40 from crosswalk_density_summary where area_type = area_type_plus_1) then 1 else 0 end as promo_40th
		
		from bones_test_results_step1
	
		),
	costar_number_of_stories as (select "GEOID" as block_group20, number_of_stories from analysis.costar_number_of_stories),
	average_stories_for_each_block_group as 
		
		select block_group20, avg(number_of_stories) as average_comm_stories from costar_number_of_stories
		
		group by block_group20
		
		),
	bones_test_results_additional_columns as (
        select
            b.block_group20, 
            b.area_type, 
            b.area_type_plus_1,
            b.crosswalk_non_pos_water_density,
            d.average_comm_stories, /*average_comm_stories comes to the left of promo_40th*/
            b.promo_40th,
            case when average_comm_stories >= 2.95 then 1 else 0 end as stories_bonus /*Also creates stories_bonus here. And it comes to the left of crosswalk_promo_area_type*/
            case when area_type in (1,5,6) then area_type else area_type+promo_40th+stories_bonus end as crosswalk_promo_area_type /*Also creates crosswalk_promo_area_type here*/
        from bones_test_results_additional_columns_step1 b
        	left join average_stories_for_each_block_group d
            on b.block_group20 = d.block_group20
    	),
	bones_test_results as (
        select
            b.block_group20, 
            b.density_bones, 
            b.accessibility_bones, 
            b.density_level, 
            b.accessibility_level, 
            b.area_type, 
            b.crosswalk_non_pos_water_density,
            d.average_comm_stories, 
            d.promo_40th,
            d.stories_bonus,
            d.crosswalk_promo_area_type,
			b.geom
        from bones_test_results_step1 b
        	left join bones_test_results_additional_columns d
            on b.block_group20 = d.block_group20
    	)
    
    
    select row_number() over() as row_number, block_group20, density_bones, accessibility_bones, density_level, accessibility_level, area_type, crosswalk_non_pos_water_density, average_comm_stories, promo_40th, stories_bonus, crosswalk_promo_area_type, geom from bones_test_results