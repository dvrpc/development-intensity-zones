drop table if exists analysis.costarproperties_region_plus_surrounding;
create table analysis.costarproperties_region_plus_surrounding as
select
cspsc."geom",
cspsc."Rentable_Building_Area"
from _raw."CoStarProperties_Surrounding_Co_2023_01" cspsc 
where (cspsc."PropertyType" not like 'Multi-Fam%' or cspsc."PropertyType" <> 'Student') and cspsc."Building_Status" in ('Existing', 'Under Construction', 'Under Renovation', 'Converted')
union 
select 
    c."shape" as geom,
    c."rentable_building_area"
from _raw.costarproperties c 
join analysis.block_groups_dvrpc_2020 bgd
    on st_intersects(st_transform(c.shape, 4326), st_transform(bgd.geom, 4326))
where (c.propertytype not like 'Multi-Fam%' or c.propertytype <> 'Student') and c.building_status in ('Existing', 'Under Construction', 'Under Renovation', 'Converted');