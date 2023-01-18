drop table if exists analysis.costarproperties_rentable_area_BG;
create table analysis.costarproperties_rentable_area_BG as
select 
    bgd."GEOID",
    sum(crps.rentable_building_area)/1000 as commercial_sqft
from analysis.costarproperties_region_plus_surrounding crps
join analysis.block_groups_24co_2020 bgd
    on st_intersects(crps.geom, bgd.geom)
group by bgd."GEOID";