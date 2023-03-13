drop view if exists analysis.crosswalk_density_summary;

create view analysis.crosswalk_density_summary as 
with 
	transect_step1 as (select prelim_transect_zone, crosswalk_density from analysis.transect_step1),
	crosswalk_density_summary as (
		
		select prelim_transect_zone, 
		percentile_cont(0.50) within group (order by crosswalk_density asc) as percentile_50  
		from transect_step1 
		
		group by prelim_transect_zone
		order by prelim_transect_zone
		
		) /*Found out how to get the percentile of a column from https://popsql.com/learn-sql/postgresql/how-to-calculate-percentiles-in-postgresql */
    
    
    select * from crosswalk_density_summary
	