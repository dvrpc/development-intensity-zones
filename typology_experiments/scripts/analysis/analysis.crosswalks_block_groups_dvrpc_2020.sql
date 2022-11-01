drop table if exists analysis.crosswalks_block_groups_dvrpc_2020;
create table analysis.crosswalks_block_groups_dvrpc_2020 as
select 
    bgd."GEOID",
    count(c.shape) as crosswalk_count
from _raw.crosswalks c 
join analysis.block_groups_dvrpc_2020 bgd on
    st_intersects(c.shape, st_transform(bgd.geom, 4326))
group by bgd."GEOID";