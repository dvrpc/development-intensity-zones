create table density.block_groups_dvrpc_2020 as
with
	block_groups_state_34_2020 as (
		select "STATEFP", 
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
		select "STATEFP", 
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
	block_groups_nj_pa_2020_without_in_dvrpc_flag as (
		select * from block_groups_state_34_2020 union
		select * from block_groups_state_42_2020
		),
	dvrpc_county_id_5dig_values as (select county_id_5dig, 'Yes' as in_dvrpc from _resources.county_key),
    block_groups_nj_pa_2020_with_in_dvrpc_flag as (
        select
            b."STATEFP",
            b."COUNTYFP",
            d.in_dvrpc,
            b."TRACTCE",
            b."BLKGRPCE",
            b."GEOID",
            b."NAMELSAD",
            b."ALAND",
            b."AWATER",
            b.geom
        from block_groups_nj_pa_2020_without_in_dvrpc_flag b
        	left join dvrpc_county_id_5dig_values d
            on b.county_id_5dig = d.county_id_5dig
    )
select "STATEFP", "COUNTYFP", "TRACTCE", "BLKGRPCE", "GEOID", "NAMELSAD", "ALAND", "AWATER", geom from block_groups_nj_pa_2020_with_in_dvrpc_flag where in_dvrpc='Yes'