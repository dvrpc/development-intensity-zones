drop view if exists analysis.crosswalk_density_summary;

create view analysis.crosswalk_density_summary as 
with 
	bones_test_results_step1 as (select area_type, crosswalk_non_pos_water_density from analysis.bones_test_results_step1),
	crosswalk_density_summary as (
		
		select area_type, 
		percentile_cont(0.40) within group (order by crosswalk_non_pos_water_density asc) as percentile_40  
		from bones_test_results_step1 
		
		group by area_type
		order by area_type
		
		) /*Found out how to get the percentile of a column from https://popsql.com/learn-sql/postgresql/how-to-calculate-percentiles-in-postgresql */
    
    
    select * from crosswalk_density_summary
	