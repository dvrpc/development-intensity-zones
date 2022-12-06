drop table if exists analysis.costarproperties_rentable_area_BG;
create table analysis.costarproperties_rentable_area_BG as
select 
    bgd."GEOID",
    sum(c.rentable_building_area)/1000 as commercial_sqft
from _raw.costarproperties c 
join analysis.block_groups_dvrpc_2020 bgd
    on st_intersects(st_transform(c.shape, 4326), st_transform(bgd.geom, 4326))
where (c.propertytype not like 'Multi-Fam%' or c.propertytype <> 'Student') and c.building_status in ('Existing', 'Under Construction', 'Under Renovation', 'Converted')
group by bgd."GEOID";