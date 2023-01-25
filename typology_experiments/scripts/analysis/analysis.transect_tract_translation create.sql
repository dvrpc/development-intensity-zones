drop view if exists analysis.transect_tract_translation;

create view analysis.transect_tract_translation as 
with 
	block2020_parent_geos as (select block_group20_id, tract20_id, aland from _resources.block2020_parent_geos),
	transect_zones as (select block_group20 as block_group20_id, transect_zone from analysis.transect),
	block2020_parent_geos_with_transect_zone as (
        select
            b.tract20_id, 
            d.transect_zone,
            b.aland
        from block2020_parent_geos b
        	left join transect_zones d
            on b.block_group20_id = d.block_group20_id
    	),
    transect_weighted_averages as (
    	
    	select tract20_id, 
    	
    	sum(transect_zone * aland) / sum(aland) as transect_weighted_average,
    	
    	round(cast(sum(transect_zone * aland) / sum(aland) as numeric), 0) as transect_zone
    	
    	from block2020_parent_geos_with_transect_zone

    	group by tract20_id
    	
    	), /*Found out how to manually calculate weighted average of one column by another from https://stackoverflow.com/a/40078094 (in turn found on https://stackoverflow.com/questions/40078047/sql-weighted-average )*/
	dvrpc_tracts_2020 as (select tract20_id, geom from _raw.dvrpc_tracts_2020),
	transect_tract_translation_without_transect_zone_names as (
        select
            b.tract20_id, 
            d.transect_weighted_average,
            d.transect_zone,
            b.geom
        from dvrpc_tracts_2020 b
        	left join transect_weighted_averages d
            on b.tract20_id = d.tract20_id
    	),
	transect_zone_names as (select * from _resources.transect_zone_names),
	transect_tract_translation as (
        select
            b.tract20_id, 
            b.transect_weighted_average,
            b.transect_zone,
            d.transect_zone_name,
            b.geom
        from transect_tract_translation_without_transect_zone_names b
        	left join transect_zone_names d
            on b.transect_zone = d.transect_zone
    	)
    
    
    select * from transect_tract_translation