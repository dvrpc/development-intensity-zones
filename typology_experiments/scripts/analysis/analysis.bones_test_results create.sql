drop view if exists analysis.bones_test_results;

create view analysis.bones_test_results as 
with 
	bones_test_results_step3 as (
		
		select block_group20, density_bones, accessibility_bones, density_level, accessibility_level, area_type, crosswalk_non_pos_water_density, 
		
		case when area_type < 6 then area_type + 1 else 7 end as area_type_plus_1, 
		
		geom from analysis.bones_test_results_step3
		
		), /*Found out how to conditionally create a column from https://stackoverflow.com/a/19029960 (in turn found on https://stackoverflow.com/questions/19029842/if-then-else-statements-in-postgresql )*/
	crosswalk_density_summary as (select * from analysis.crosswalk_density_summary),
	bones_test_results_crosswalk_flag_columns_step1 as (
		
		select block_group20, area_type, area_type_plus_1, crosswalk_non_pos_water_density,
		
		case when crosswalk_non_pos_water_density > (select percentile_40 from crosswalk_density_summary where area_type = area_type_plus_1) then 1 else 0 end as promo_40th
		
		from bones_test_results_step3
	
		),
	bones_test_results_crosswalk_flag_columns as (
		
		select block_group20, area_type, area_type_plus_1, crosswalk_non_pos_water_density, promo_40th, 
		
		case when area_type in (1,5,6) then area_type else area_type+promo_40th end as crosswalk_promo_area_type
		
		from bones_test_results_crosswalk_flag_columns_step1
	
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
            d.promo_40th,
            d.crosswalk_promo_area_type,
			b.geom
        from bones_test_results_step3 b
        	left join bones_test_results_crosswalk_flag_columns d
            on b.block_group20 = d.block_group20
    	)
    
    
    select row_number() over() as row_number, block_group20, density_bones, accessibility_bones, density_level, accessibility_level, area_type, crosswalk_non_pos_water_density, promo_40th, crosswalk_promo_area_type, geom from bones_test_results