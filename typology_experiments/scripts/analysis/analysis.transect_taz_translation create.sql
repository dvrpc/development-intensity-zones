drop view if exists analysis.transect_taz_translation;

create view analysis.transect_taz_translation as 
with 
	block2020_parent_geos as (select block_group20_id, taz_id, aland from _resources.block2020_parent_geos),
	transect_zones as (select block_group20 as block_group20_id, transect_zone, transect_zone_name from analysis.transect),
	block2020_parent_geos_with_transect_zone_info as (
        select
            b.taz_id, 
            d.transect_zone,
            d.transect_zone_name,
            b.aland
        from block2020_parent_geos b
        	left join transect_zones d
            on b.block_group20_id = d.block_group20_id
    	),
    transect_weighted_averages as (
    	
    	select taz_id, 
    	
    	sum(transect_zone * aland) / sum(aland) as transect_weighted_average,
    	
    	round(cast(sum(transect_zone * aland) / sum(aland) as numeric), 0) as transect_zone
    	
    	from block2020_parent_geos_with_transect_zone_info

    	group by taz_id
    	
    	), /*Found out how to manually calculate weighted average of one column by another from https://stackoverflow.com/a/40078094 (in turn found on https://stackoverflow.com/questions/40078047/sql-weighted-average )*/
	transect_weighted_averages_with_transect_zone_name as (
        select
            b.taz_id, 
            b.transect_weighted_average,
            b.transect_zone,
            d.transect_zone_name
        from transect_weighted_averages b
        	left join block2020_parent_geos_with_transect_zone_info d
            on b.transect_zone = d.transect_zone
    	),
	taz_2010_mcdaligned as (select tazt as taz_id, shape as geom from _raw.taz_2010_mcdaligned where dvrpc='yes'),
	transect_taz_translation as (
        select
            b.taz_id, 
            b.transect_weighted_average,
            b.transect_zone,
            b.transect_zone_name,
            d.geom
        from transect_weighted_averages_with_transect_zone_name b
        	left join taz_2010_mcdaligned d
            on b.taz_id = d.taz_id
    	)
    
    
    select row_number() over() as row_number, taz_id, transect_weighted_average, transect_zone, transect_zone_name, geom from transect_taz_translation