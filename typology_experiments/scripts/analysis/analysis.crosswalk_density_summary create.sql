drop view if exists analysis.crosswalk_density_summary;

create view analysis.crosswalk_density_summary as 
with 
	bones_test_results_step3 as (select area_type, crosswalk_non_pos_water_density from analysis.bones_test_results_step3),
	crosswalk_density_summary as (
		
		select area_type, avg(crosswalk_non_pos_water_density) as mean, 
		percentile_cont(0.10) within group (order by crosswalk_non_pos_water_density asc) as percentile_10, 
		percentile_cont(0.20) within group (order by crosswalk_non_pos_water_density asc) as percentile_20,  
		percentile_cont(0.30) within group (order by crosswalk_non_pos_water_density asc) as percentile_30,  
		percentile_cont(0.40) within group (order by crosswalk_non_pos_water_density asc) as percentile_40,  
		percentile_cont(0.50) within group (order by crosswalk_non_pos_water_density asc) as percentile_50, 
		percentile_cont(0.60) within group (order by crosswalk_non_pos_water_density asc) as percentile_60, 
		percentile_cont(0.70) within group (order by crosswalk_non_pos_water_density asc) as percentile_70, 
		percentile_cont(0.80) within group (order by crosswalk_non_pos_water_density asc) as percentile_80,  
		percentile_cont(0.90) within group (order by crosswalk_non_pos_water_density asc) as percentile_90   
		from bones_test_results_step3 
		
		group by area_type
		order by area_type
		
		) /*Found out how to get the percentile of a column from https://popsql.com/learn-sql/postgresql/how-to-calculate-percentiles-in-postgresql */
    
    
    select * from crosswalk_density_summary
	