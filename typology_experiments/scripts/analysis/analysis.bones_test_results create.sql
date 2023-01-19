drop view if exists analysis.bones_test_results;

create view analysis.bones_test_results as 
with 
	bones_test_results_step3 as (
		
		select block_group20, density_bones, accessibility_bones, density_level, accessibility_level, area_type, crosswalk_non_pos_water_density, case when area_type < 6 then area_type + 1 else 7 end as area_type_plus_1, geom from analysis.bones_test_results_step3
		
		), /*Found out how to conditionally create a column from https://stackoverflow.com/a/19029960 (in turn found on https://stackoverflow.com/questions/19029842/if-then-else-statements-in-postgresql )*/
	crosswalk_density_summary as (select * from analysis.crosswalk_density_summary),
	bones_test_results_crosswalk_flags_columns as (
		
		select block_group20, area_type, area_type_plus_1, crosswalk_non_pos_water_density,
		
		case when crosswalk_non_pos_water_density > (select mean from crosswalk_density_summary where area_type = area_type_plus_1) then 1 else 0 end as promo_mean,
		case when crosswalk_non_pos_water_density > (select percentile_10 from crosswalk_density_summary where area_type = area_type_plus_1) then 1 else 0 end as promo_10th,
		case when crosswalk_non_pos_water_density > (select percentile_20 from crosswalk_density_summary where area_type = area_type_plus_1) then 1 else 0 end as promo_20th,
		case when crosswalk_non_pos_water_density > (select percentile_30 from crosswalk_density_summary where area_type = area_type_plus_1) then 1 else 0 end as promo_30th,
		case when crosswalk_non_pos_water_density > (select percentile_40 from crosswalk_density_summary where area_type = area_type_plus_1) then 1 else 0 end as promo_40th,
		case when crosswalk_non_pos_water_density > (select percentile_50 from crosswalk_density_summary where area_type = area_type_plus_1) then 1 else 0 end as promo_50th,
		case when crosswalk_non_pos_water_density > (select percentile_60 from crosswalk_density_summary where area_type = area_type_plus_1) then 1 else 0 end as promo_60th,
		case when crosswalk_non_pos_water_density > (select percentile_70 from crosswalk_density_summary where area_type = area_type_plus_1) then 1 else 0 end as promo_70th,
		case when crosswalk_non_pos_water_density > (select percentile_80 from crosswalk_density_summary where area_type = area_type_plus_1) then 1 else 0 end as promo_80th,
		case when crosswalk_non_pos_water_density > (select percentile_90 from crosswalk_density_summary where area_type = area_type_plus_1) then 1 else 0 end as promo_90th
		
		from bones_test_results_step3
	
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
            d.promo_mean,
            d.promo_10th,
			d.promo_20th,
			d.promo_30th,
			d.promo_40th,
			d.promo_50th,
			d.promo_60th,
			d.promo_70th,
			d.promo_80th,
			d.promo_90th,
            b.geom
        from bones_test_results_step3 b
        	left join bones_test_results_crosswalk_flags_columns d
            on b.block_group20 = d.block_group20
    	)
    
    
    select row_number() over() as row_number, block_group20, density_bones, accessibility_bones, density_level, accessibility_level, area_type, crosswalk_non_pos_water_density, promo_mean, promo_10th, promo_20th, promo_30th, promo_40th, promo_50th, promo_60th, promo_70th, promo_80th, promo_90th, geom from bones_test_results