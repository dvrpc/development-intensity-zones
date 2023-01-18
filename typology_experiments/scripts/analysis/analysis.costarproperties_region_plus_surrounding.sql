drop table if exists analysis.costarproperties_region_plus_surrounding;
create table analysis.costarproperties_region_plus_surrounding as
select 
c."shape" as "geom",
c."rentable_building_area"
from "_raw"."costarproperties" c 
union
select 
cspsc."geom",
cspsc."Rentable_Building_Area"
from "_raw"."CoStarProperties_Surrounding_Co_2023_01" cspsc;

