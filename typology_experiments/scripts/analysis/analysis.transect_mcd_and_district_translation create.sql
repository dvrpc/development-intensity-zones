drop view if exists analysis.transect_mcd_and_district_translation;

create view analysis.transect_mcd_and_district_translation as 
with 
	block2020_parent_geos as (
		
		select block_group20_id, 
		
		case when mcd20_id = '4210160000' then regexp_replace(district_id, '\.0', '0') else mcd20_id end as district_id,  /*Note that issue with _resources.block2020_parent_geos's district_id column where the district_id values have ".0"'s at the end of them, even though they're string*/
		
		aland 
		
		from _resources.block2020_parent_geos
		
		),
	transect_zones_philly as (
		
		select block_group20 as block_group20_id, transect_zone from analysis.transect 
		
		where block_group20 in (select block_group20_id from block2020_parent_geos)
		
		),
	block2020_parent_geos_with_transect_zone as (
        select
            b.district_id, 
            d.transect_zone,
            b.aland
        from block2020_parent_geos b
        	left join transect_zones_philly d
            on b.block_group20_id = d.block_group20_id
    	),
    transect_weighted_averages as (
    	
    	select district_id, 
    	
    	sum(transect_zone * aland) / sum(aland) as transect_weighted_average,
    	
    	round(cast(sum(transect_zone * aland) / sum(aland) as numeric), 0) as transect_zone
    	
    	from block2020_parent_geos_with_transect_zone

    	group by district_id
    	
    	), /*Found out how to manually calculate weighted average of one column by another from https://stackoverflow.com/a/40078094 (in turn found on https://stackoverflow.com/questions/40078047/sql-weighted-average )*/
	dvrpc_mcd_phicpa as (select geoid as district_id, shape as geom from _raw.dvrpc_mcd_phicpa),
	transect_mcd_and_district_translation_without_transect_zone_names as (
        select
            b.district_id, 
            d.transect_weighted_average,
            d.transect_zone,
            b.geom
        from dvrpc_mcd_phicpa b
        	left join transect_weighted_averages d
            on b.district_id = d.district_id
    	),
	transect_zone_names as (select * from _resources.transect_zone_names),
	transect_mcd_and_district_translation as (
        select
            b.district_id, 
            b.transect_weighted_average,
            b.transect_zone,
            d.transect_zone_name,
            b.geom
        from transect_mcd_and_district_translation_without_transect_zone_names b
        	left join transect_zone_names d
            on b.transect_zone = d.transect_zone
    	)
    
    
    select * from transect_mcd_and_district_translation