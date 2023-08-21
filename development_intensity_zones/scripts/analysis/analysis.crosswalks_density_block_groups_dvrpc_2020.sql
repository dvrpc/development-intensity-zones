drop table if exists analysis.crosswalks_density_block_groups_dvrpc_2020;
create table analysis.crosswalks_density_block_groups_dvrpc_2020 as
select
ula."GEOID",
ula."ALAND",
ula.non_pos_water_acres,
ula.aland_acres,
cbgd.crosswalk_count / ula.aland_acres as crosswalk_aland_density,
cbgd.crosswalk_count / ula.non_pos_water_acres as crosswalk_density,
ula.geom
from analysis.unprotected_land_area ula 
left join analysis.crosswalks_block_groups_dvrpc_2020 cbgd 
on ula."GEOID" = cbgd.geoid;