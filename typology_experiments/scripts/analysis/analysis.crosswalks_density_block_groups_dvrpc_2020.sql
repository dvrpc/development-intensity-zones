drop table if exists analysis.crosswalks_density_block_groups_dvrpc_2020;
create table analysis.crosswalks_density_block_groups_dvrpc_2020 as
select 
ula."STATEFP",
ula."COUNTYFP",
ula."TRACTCE",
ula."BLKGRPCE",
ula."GEOID",
ula."NAMELSAD",
ula."ALAND",
ula."AWATER",
ula.non_pos_water_acres,
ula.aland_acres,
ula.geom,
ula.uid,
cbgd.crosswalk_count / ula.aland_acres as crosswalk_aland_density,
cbgd.crosswalk_count / ula.non_pos_water_acres as crosswalk_density
from analysis.unprotected_land_area ula 
left join analysis.crosswalks_block_groups_dvrpc_2020 cbgd 
on ula."GEOID" = cbgd."GEOID";