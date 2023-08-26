create view output.blockgroup_centroid as
select
	geoid,
	st_centroid(geometry) as geom
from
	source.census_blockgroups_2020;

create view output.blockgroup_buffers as 
select
	geoid,
	st_buffer(geom, 3218.69) as geom, -- 2 miles
	2 as buff_mi
from
	output.blockgroup_centroid
union
select
	geoid,
	st_buffer(geom, 8046.72) as geom, -- 5 miles
	5 as buff_mi
from
	output.blockgroup_centroid;

create view output.block_centroid as 
select
	geoid20,
	st_centroid(geometry) as geom
from
	source.census_blocks_2020;


/*
create the full pos and h2o layer
*/
-- source pos union geoms
create TEMP TABLE union_pos as (
select
	a.clipped_geometry as geom
from
	(
	select
		ST_Intersection(st_force2D(pos.geometry), d.geometry) as clipped_geometry
	from
		source.ches_bay_watershed_pos pos,
		source.countyboundaries d
	where
		ST_Intersects(st_force2D(pos.geometry), d.geometry)) as a
union
select
	a.clipped_geometry as geom
from
	(
	select
		ST_Intersection(st_force2D(pos.geometry), d.geometry) as clipped_geometry
	from
		source.del_river_basin_pos pos,
		source.countyboundaries d
	where
		ST_Intersects(st_force2D(pos.geometry), d.geometry)) as a
union
select
	a.clipped_geometry as geom
from
	(
	select
		ST_Intersection(st_force2D(pos.geometry), d.geometry) as clipped_geometry
	from
		source.nj_pos pos,
		source.countyboundaries d
	where
		ST_Intersects(st_force2D(pos.geometry),
		d.geometry)) as a);
	
-- removes outside source pos geom inside of dvrpc region boundary
create TEMP table minus_pos as 
select
	ST_Difference(pos.geom,
	d.geometry) as geom
from
	union_pos pos,
	source.countyboundaries d
where
	d.dvrpc_reg = 'Yes';

-- adds the dvrpc pos file to the source union
create TEMP table add_dvrpc_pos as
select
	geometry as geom
from
	source.dvrpc_protectedopenspace2020 dpos
union
select
	geom
from
	minus_pos;

-- add in the dvrpc hydro to the pos union
create TEMP table pos_h2o as
select
	0 as zone,
	geom
from
	add_dvrpc_pos
union
select
	0 as zone,
	geometry as geom
from
	source.regional_water_bodies;

-- dissolve the geoms to zone 0
create table output.pos_h2o as
select
	zone,
	st_union(geom) as geom
from
	pos_h2o
group by
	zone;

/*
calculate intersection of pos and block group polygons to get areas of pos in block group
*/
create materialized view output.bg_pos_intersection_geom as
select
	cb.geoid,
	ST_Intersection(cb.geometry, pos.geom) as intersection
from
	source.census_blockgroups_2020 as cb
join output.pos_h2o as pos on
	ST_Intersects(cb.geometry, pos.geom);

-- output pos/non pos block group area in acres
create view output.bg_pos_area_calc as
with area_calcs as (
select
	cb.geoid,
	(SUM(ST_Area(i.intersection)))/ 4046.86 as pos_acres,
	ST_Area(cb.geometry)/4046.86 as bg_acres,
	cb.aland/4046.86 as aland_acres
from
	source.census_blockgroups_2020 as cb
left join output.bg_pos_intersection_geom i on
	cb.geoid = i.geoid
group by
	cb.geoid,
	cb.aland,
	cb.geometry),
area_clean as (
select
	area_calcs.geoid,
	area_calcs.bg_acres,
	case
		when area_calcs.pos_acres is not null then area_calcs.pos_acres
		else 0
	end as pos_acres,
	area_calcs.aland_acres
from 
	area_calcs)
select
	area_clean.*,
	bg_acres-pos_acres as non_pos_acres,
	(pos_acres/bg_acres) * 100 as percent_lu_pos
from
	area_clean;


/*
crosswalk density (not sure if this is even used)
*/
create view output.crosswalk_density as
with blockgroups as (
select
	a.*,
	cbg.geometry as geom
from
	source.census_blockgroups_2020 cbg
join output.bg_pos_area_calc a on
	a.geoid = cbg.geoid
where
	dvrpc_reg = 'y')
select
	bg.geoid,
	count(c.geometry) as crosswalk_count,
	count(c.geometry)/aland_acres as cw_total_density,
	count(c.geometry)/non_pos_acres as cw_nonpos_density
from
	source.pedestriannetwork_lines c
join blockgroups bg on
	st_intersects(c.geometry, bg.geom)
where non_pos_acres > 0
group by
	bg.geoid,
	bg.aland_acres,
	bg.non_pos_acres;

/*
average commercial stories costar data to block group
*/
create view output.costar_stories as 
with costar as (
select
	*
from
	source.costarproperties
where
	number_of_stories is not null
	and building_status in ('Converted', 'Existing', 'Under Construction', 'Under Renovation')
	and year_built <= 2020)
select
	cb.geoid,
	avg(costar.number_of_stories) as avg_stories
from
	costar
join source.census_blockgroups_2020 cb on
	st_intersects(costar.geometry,
	cb.geometry)
group by
	cb.geoid;
	
	
/*
commerical square ft calcs from costar data to block group
*/
create view output.commercial_sqft_calcs as
with costar_rba as
(
select
	c.geometry,
	c.rba
from
	source.costarproperties c
where
	(c.propertytype not like 'Multi-Fam%' or c.propertytype <> 'Student')
	and c.building_status in ('Existing', 'Under Construction', 'Under Renovation', 'Converted')
	and c.year_built <= 2020
-- !!!!! REMOVE WHEN COSTAR ADDS THESE RECORDS !!!!!
union
select
	ST_GeomFromText(nic.geom, 26918) AS geom,
	nic.rentable_building_area as rba
from
	source.not_in_costar nic
-- !!!!!
	),
commsqft_calc as (
select
	cbg.geoid,
	sum(rba) / 1000 as comm_sqft
from
	costar_rba c
join source.census_blockgroups_2020 cbg on
	st_intersects (c.geometry, cbg.geometry)
group by
	cbg.geoid)	
-- updates the commercial_sqft value for some University City block??
select
	*
from
	commsqft_calc
where
	geoid <> '421010369021'
union
select
	geoid,
	1901812 / 1000 as comm_sqft
from
	commsqft_calc
where
	geoid = '421010369021';

/*
apply density index values and levels to block groups 
*/
create view output.bg_density_index_result as 
--join housing units and group quarters to block group
with bg_hu_gq as (
select
	census.geoid,
	bg_area.aland_acres,
	census.housing_units_d20
from
	output.bg_pos_area_calc bg_area
join (
	select
		concat(bg.state, bg.county, bg.tract, bg.block_group) as geoid,
		p5_001n + h1_001n as housing_units_d20
	from
		source.tot_pops_and_hhs_2020_block_group as bg) as census 
on census.geoid = bg_area.geoid
where
	bg_area.aland_acres <>0),
-- join commercial sqft to table 
bg_hu_gq_commsqft as (
select
	bg_hu_gq.*,
	case
		when co.comm_sqft is null then 0
		else co.comm_sqft
	end as comm_sqft
from
	bg_hu_gq
left join output.commercial_sqft_calcs co on
	co.geoid = bg_hu_gq.geoid),
c as (
select
	geoid,
	round(cast((housing_units_d20 + comm_sqft)/ aland_acres as numeric), 3) as density_index
from
	bg_hu_gq_commsqft),
d as(
select
	t.*,
	lag(t.density_index_thresholds,0,0) over (order by t.density_index_thresholds) as prev_threshold,
	lead(t.density_index_thresholds,1,200000.0) over (order by t.density_index_thresholds) as next_threshold
from
	source.thresholds t)
    select
	c.geoid,
	c.density_index,
	case
		when c.density_index < d.prev_threshold then 'lowest'
		when c.density_index >= d.prev_threshold
		and c.density_index < d.next_threshold then d.levels
		else 'highest'
	end as density_level
from
	c
join d on
	c.density_index >= d.prev_threshold
	and c.density_index < d.next_threshold;

/*
make a rule that 'high or greater' density are allowed buffers that reach across the river 
(if their buffer intersects with it). If it is 'moderate, low, or very low' its buffer, 
if intersecting with the river centerline, should be erased on the opposite side. Perhaps the shape of states whose 
IDs don't match the first two digits of the block group ID can be the "eraser shape".
*/
create table output.river_buff_adjustment as
with a as (
select
	bg.geoid,
	bg.buff_mi, 
	bg.geom
from
	output.blockgroup_buffers bg,
	(select st_union(geometry) as geom from source.state_streams group by gnis_name) drc
where
	st_intersects(bg.geom,drc.geom)),
b as (
select
	a.geoid,
	a.buff_mi,
	a.geom,
	bda.density_level
from
	a
join output.bg_density_index_result bda on
	a.geoid = bda.geoid),
c as (
select
	b.geoid,
	b.buff_mi,
	(st_dump(st_collectionextract(st_split(b.geom, drc.geom)))).geom as geom
from
	b,
	(select st_union(geometry) as geom from source.state_streams group by gnis_name) drc
where
	b.density_level in ('very low', 'low', 'moderate')),
d as (
select
	c.*
from
	c,
	output.blockgroup_centroid bgc
where
	st_intersects(c.geom, bgc.geom)
	and 
	bgc.geoid = c.geoid)
select
	bg.geoid,
	bg.buff_mi,
	case 
		when d.geom is null then bg.geom
		else d.geom
	end as geom
from
	output.blockgroup_buffers bg
full join d on
	d.geoid = bg.geoid
	and d.buff_mi = bg.buff_mi
order by
	bg.geoid;
	
/*
proxmity analysis on 2 and 5 mile block group buffers	
*/
-- do analysis for 2mi buffers
create materialized view output.costar_bg_2mi as
with a as 
(
select
	bb.*
from
	output.river_buff_adjustment bb
inner join (
	select
		geoid
	from
		source.census_blockgroups_2020 cb
	where
		cb.aland <> 0) 
as nowater on
	nowater.geoid = bb.geoid
where
	bb.buff_mi = 2),
b as (
select
	propertyid,
	rba,
	rba / 1000 as comm_sqft,
	geometry
from
	source.costarproperties c
where
	(c.propertytype not like 'Multi-Fam%'
		or c.propertytype <> 'Student')
	and c.building_status in ('Existing', 'Under Construction', 'Under Renovation', 'Converted')
		and c.year_built <= 2020
-- !!!!! REMOVE WHEN COSTAR ADDS THESE RECORDS !!!!!
union 
select 
	0 as propertyid,
	nic.rentable_building_area as rba,
	nic.rentable_building_area / 1000 as comm_sqft,
	ST_GeomFromText(nic.geom, 26918) AS geometry
from
	source.not_in_costar nic 
-- !!!!!
		),
c as (
select
	a.geoid,
	a.buff_mi,
	count(b.geometry) as tot_costar,
	sum(b.comm_sqft) as sum_comm_sqft
from
	a
join b on
	st_intersects(b.geometry, a.geom)
group by
	a.geoid,
	a.buff_mi)
select
	a.*,
	c.tot_costar,
	c.sum_comm_sqft
from
	a
left join c on
	c.geoid = a.geoid
	and c.buff_mi = a.buff_mi;

-- 5 mile buffer analysis
create materialized view output.gq_hu_5mi as
with a as (
select
	bb.*
from
	output.river_buff_adjustment bb
inner join (
	select
		geoid
	from
		source.census_blockgroups_2020 cb
	where
		cb.aland <> 0) 
as nowater on
	nowater.geoid = bb.geoid
where
	bb.buff_mi = 5),
b as
(
select
	bc.geoid20,
	bc.geom,
	census.housing_units_d20
from
	output.block_centroid bc
join (
	select
		concat(bg.state, bg.county, bg.tract, bg.block) as geoid20,
		p5_001n + h1_001n as housing_units_d20
	from
		source.tot_pops_and_hhs_2020_block as bg) as census on
	census.geoid20 = bc.geoid20),
c as (
select
	a.geoid,
	a.buff_mi,
	sum(housing_units_d20) as sum_gq_hu
from
	a
join b on
	st_intersects(a.geom,
	b.geom)
group by
	a.geoid,
	a.buff_mi)
select
	a.*,
	c.sum_gq_hu
from
	a
left join c on
	c.geoid = a.geoid
	and c.buff_mi = a.buff_mi;

-- calculate the proximity index
create view output.bg_proximity_index_result as
with a as
(
select
	bg.geoid,
	case
		when costar.sum_comm_sqft is not null then costar.sum_comm_sqft
		else 0
	end as sum_comm_sqft,
	ghm.sum_gq_hu
from
	source.census_blockgroups_2020 bg
left join output.costar_bg_2mi costar on
	costar.geoid = bg.geoid
left join output.gq_hu_5mi ghm on
	ghm.geoid = bg.geoid
where
	ghm.sum_gq_hu is not null),
b as (
select
	a.*,
	(2 * a.sum_comm_sqft * a.sum_gq_hu) / (a.sum_comm_sqft + a.sum_gq_hu) as proximity_index
from
	a),
c as (
select
	t.*,
	lag(t.proximity_index_thresholds, 0, 0) over (order by t.proximity_index_thresholds) as prev_threshold,
	lead(t.proximity_index_thresholds, 1, 500000.0) over (order by t.proximity_index_thresholds) as next_threshold
from
	source.thresholds t)
select
	b.geoid,
	b.sum_comm_sqft,
	b.sum_gq_hu,
	b.proximity_index,
	case
		when b.proximity_index < c.prev_threshold then 'lowest'
		when b.proximity_index >= c.prev_threshold
		and b.proximity_index < c.next_threshold then c.levels
		else 'highest'
	end as proximity_level
from
	b
join c on
	b.proximity_index >= c.prev_threshold
	and b.proximity_index < c.next_threshold;

/*
creates development intensity zones by block group	
*/
create table output.diz_zone as
with a as (
select
	bdir.geoid,
	bdir.density_index,
	bdir.density_level,
	bpig.proximity_index,
	bpig.proximity_level
from
	output.bg_density_index_result bdir
left join output.bg_proximity_index_result bpig on
	bpig.geoid = bdir.geoid),
b as (
select
	a.*,
	cls.prelim_diz_zone
from
	a
left join source.classifications cls on
	cls.proximity_index_levels = a.proximity_level
	and cls.density_index_levels = a.density_level),
c as (
select
	b.geoid,
	b.density_index,
	b.density_level,
	b.proximity_index,
	b.proximity_level,
	case
		when b.geoid in (
		select
			geoid
		from
			output.bg_pos_area_calc bg
		where
			bg.percent_lu_pos >= 95) then 0
		else prelim_diz_zone
	end as prelim_diz_zone
from
	b),
d as (
select
	c.*,
	cd.cw_nonpos_density
from
	c
left join output.crosswalk_density cd on
	cd.geoid = c.geoid),
crosswalk_summary as (
select
	prelim_diz_zone,
	percentile_cont(0.50) within group (
order by
	cw_nonpos_density asc) as percentile_50
from
	d
group by
	prelim_diz_zone),
e as (
select
	d.*,
	case
		when prelim_diz_zone < 6
			and prelim_diz_zone > 0 then prelim_diz_zone + 1
			else 7
		end as prelim_diz_zone_plus_1
	from
		d),
f as (
select
	e.*,
	cs.avg_stories,
	cws.percentile_50
from
	e
left join output.costar_stories cs on
	cs.geoid = e.geoid
left join crosswalk_summary cws on
	cws.prelim_diz_zone = e.prelim_diz_zone_plus_1),
g as (
select 
	f.*, 
	case
		when cw_nonpos_density > percentile_50
			and prelim_diz_zone not in (1, 5) then 1
			else 0
		end as crosswalk_bonus,
		case
			when avg_stories >= 3
				and prelim_diz_zone = 5
				and density_level = 'high'
				and proximity_level = 'highest' then 1
				else 0
			end as stories_bonus
		from
			f),
h as (
select
	g.*,
	case
		when prelim_diz_zone <> 0 then prelim_diz_zone + crosswalk_bonus + stories_bonus
		else 0
	end as diz_zone
from
	g)
select
	h.*,
	dzn.diz_zone_name,
	cb.geometry
from
	h
left join source.diz_zone_names dzn on
	dzn.diz_zone = h.diz_zone
left join source.census_blockgroups_2020 cb on
	cb.geoid = h.geoid