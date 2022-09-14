drop table if exists density.dvrpc_protected_land_and_water;

create table density.dvrpc_protected_land_and_water as
with
	dvrpc_protected_land_and_water as (
    	select state, county, os_type as land_use, acres, geom from _raw.dvrpc_2020_pos union
    	select state_name as state, co_name as county, lu15catn as land_use, acres, geom from _raw.dvrpc_2015_water
    	)
    
    
    select * from dvrpc_protected_land_and_water
