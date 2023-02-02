drop table if exists analysis.block_groups_24co_2020;

create table analysis.block_groups_24co_2020 as
with
	block_groups_24co_2020_undissolved as (
		select "STATEFP", "COUNTYFP", "GEOID", "ALAND", geom from _raw.tl_2020_10_bg union
		select "STATEFP", "COUNTYFP", "GEOID", "ALAND", geom from _raw.tl_2020_24_bg union
		select "STATEFP", "COUNTYFP", "GEOID", "ALAND", geom from _raw.tl_2020_34_bg union
		select "STATEFP", "COUNTYFP", "GEOID", "ALAND", geom from _raw.tl_2020_42_bg
	)

	
	select 
		distinct "GEOID",
		sum("ALAND") as "ALAND",
		st_union(geom) as geom
		from block_groups_24co_2020_undissolved where concat("STATEFP","COUNTYFP") in (
		'42101', 
		'42029', 
		'42045', 
		'42091', 
		'42017', 
		'34021', 
		'34005', 
		'34007', 
		'34015', 
		'34033', 
		'10003', 
		'24015', 
		'42071', 
		'42011', 
		'42077', 
		'42095', 
		'34019', 
		'34041', 
		'34035', 
		'34023', 
		'34025', 
		'34001', 
		'34029', 
		'34011'
    	)
		group by "GEOID" /*Found out how to dissolve a shapefile from https://gis.stackexchange.com/a/209715 (in turn found on https://gis.stackexchange.com/questions/209713/postgis-dissolve-geometries-from-shapefiles ), and got the 24 counties' FIPS codes/5-digit county IDs from https://dvrpc-dvrpcgis.opendata.arcgis.com/datasets/county-boundaries-polygon/explore?filters=eyJmaXBzIjpbIjQyMTAxIiwiNDIwMjkiLCI0MjA0NSIsIjQyMDkxIiwiNDIwMTciLCIzNDAyMSIsIjM0MDA1IiwiMzQwMDciLCIzNDAxNSIsIjM0MDMzIiwiMTAwMDMiLCIyNDAxNSIsIjQyMDcxIiwiNDIwMTEiLCI0MjA3NyIsIjQyMDk1IiwiMzQwMTkiLCIzNDA0MSIsIjM0MDM1IiwiMzQwMjMiLCIzNDAyNSIsIjM0MDAxIiwiMzQwMjkiLCIzNDAxMSJdfQ%3D%3D&location=39.725065%2C-75.630939%2C8.00 */