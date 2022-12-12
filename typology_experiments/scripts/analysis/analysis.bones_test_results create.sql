/*drop view if exists analysis.bones_test_results;

create view analysis.bones_test_results as */
with 
	bones_test_results_step1 as (
		
		select row_number, block_group20, density_bones, accessibility_bones, density_level, accessibility_level, area_type, crosswalk_non_pos_water_density, case when area_type < 6 then area_type + 1 else 7 end as area_type_plus_1 from analysis.bones_test_results_step1
		
		), /*Found out how to conditionally create a column from https://stackoverflow.com/a/19029960 (in turn found on https://stackoverflow.com/questions/19029842/if-then-else-statements-in-postgresql )*/
	crosswalk_density_summary as (select * from analysis.crosswalk_density_summary),
	bones_test_results_crosswalk_flags_columns as (
		
		select area_type, area_type_plus_1, crosswalk_non_pos_water_density,
		
		case when crosswalk_non_pos_water_density > (select mean from crosswalk_density_summary where area_type = area_type_plus_1) then 1 else 0 end as promo_mean
		
		from bones_test_results_step1
	
		) select * from bones_test_results_crosswalk_flags_columns