drop table if exists analysis.block_groups_24co_2020;

create table analysis.block_groups_24co_2020 as
with
	block_groups_state_10_2020 as (
		select 
		"STATEFP", 
		"COUNTYFP", 
		concat("STATEFP","COUNTYFP") as county_id_5dig,
		"TRACTCE",
		"BLKGRPCE",
		"GEOID",
		"NAMELSAD",
		"ALAND",
		"AWATER",
		geom
		from _raw.tl_2020_10_bg
	),
	block_groups_state_24_2020 as (
		select 
		"STATEFP", 
		"COUNTYFP", 
		concat("STATEFP","COUNTYFP") as county_id_5dig,
		"TRACTCE",
		"BLKGRPCE",
		"GEOID",
		"NAMELSAD",
		"ALAND",
		"AWATER",
		geom
		from _raw.tl_2020_24_bg
	),
	block_groups_state_34_2020 as (
		select 
		"STATEFP", 
		"COUNTYFP", 
		concat("STATEFP","COUNTYFP") as county_id_5dig,
		"TRACTCE",
		"BLKGRPCE",
		"GEOID",
		"NAMELSAD",
		"ALAND",
		"AWATER",
		geom
		from _raw.tl_2020_34_bg
	),
	block_groups_state_42_2020 as (
		select 
		"STATEFP", 
		"COUNTYFP", 
		concat("STATEFP","COUNTYFP") as county_id_5dig,
		"TRACTCE",
		"BLKGRPCE",
		"GEOID",
		"NAMELSAD",
		"ALAND",
		"AWATER",
		geom
		from _raw.tl_2020_42_bg
	),
	block_groups_de_md_nj_pa_2020_without_in_dvrpc_flag as (
		select * from block_groups_state_10_2020 union
		select * from block_groups_state_24_2020 union
		select * from block_groups_state_34_2020 union
		select * from block_groups_state_42_2020
	)
    
    
    select 
    "STATEFP", 
    "COUNTYFP", 
    county_id_5dig, 
    case when county_id_5dig in (select county_id_5dig from _resources.county_key) then 1 else 0 end as in_dvrpc, 
    "TRACTCE", 
    "BLKGRPCE", 
    "GEOID",
    "NAMELSAD", 
    "ALAND",
    round(cast("ALAND"/4046.856 as numeric), 0) as aland_acres,
    "AWATER", 
    geom 
    from block_groups_de_md_nj_pa_2020_without_in_dvrpc_flag where county_id_5dig in (
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
    	) /*Got the 24 counties' FIPS codes/5-digit county IDs from https://dvrpc-dvrpcgis.opendata.arcgis.com/datasets/county-boundaries-polygon/explore?filters=eyJmaXBzIjpbIjQyMTAxIiwiNDIwMjkiLCI0MjA0NSIsIjQyMDkxIiwiNDIwMTciLCIzNDAyMSIsIjM0MDA1IiwiMzQwMDciLCIzNDAxNSIsIjM0MDMzIiwiMTAwMDMiLCIyNDAxNSIsIjQyMDcxIiwiNDIwMTEiLCI0MjA3NyIsIjQyMDk1IiwiMzQwMTkiLCIzNDA0MSIsIjM0MDM1IiwiMzQwMjMiLCIzNDAyNSIsIjM0MDAxIiwiMzQwMjkiLCIzNDAxMSJdfQ%3D%3D&location=39.725065%2C-75.630939%2C8.00 */