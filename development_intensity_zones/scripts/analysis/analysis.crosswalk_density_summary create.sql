drop view if exists analysis.crosswalk_density_summary;
create view analysis.crosswalk_density_summary as 

with 
	diz_bg_s1 as (
		select prelim_diz_zone, crosswalk_density from analysis.diz_block_group_step1
		) --Only worked when this was made as a separate object for some reason

	select prelim_diz_zone, 
	percentile_cont(0.50) within group (order by crosswalk_density asc) as percentile_50  
	from diz_bg_s1
	
	group by prelim_diz_zone
	order by prelim_diz_zone --Found out how to get the percentile of a column from https://popsql.com/learn-sql/postgresql/how-to-calculate-percentiles-in-postgresql 
