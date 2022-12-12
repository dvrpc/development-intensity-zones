/*drop view if exists analysis.crosswalk_density_summary;

create view analysis.crosswalk_density_summary as */
with 
	bones_test_results_step1 as (select area_type, crosswalk_non_pos_water_density from analysis.bones_test_results_step1),
	crosswalk_density_summary as (
		
		select area_type, avg(crosswalk_non_pos_water_density) as mean, percentile_cont(0.10) within group (order by crosswalk_non_pos_water_density asc) as percentile_10 from bones_test_results_step1 
		
		group by area_type
		order by area_type
		
		) select * from crosswalk_density_summary
	