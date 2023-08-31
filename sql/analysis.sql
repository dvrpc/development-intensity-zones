/*
creating initial buffers and centroids for blockgroup geometries
*/
begin;
create view output.blockgroup_centroid as
select
	geoid,
	ST_Centroid(geometry) as geom
from
	source.census_blockgroups_2020;
create view output.blockgroup_buffers as 
select
	geoid,
	ST_Buffer(geom, 3218.69) as geom, -- 2 miles
	2 as buff_mi
from
	output.blockgroup_centroid
union
select
	geoid,
	ST_Buffer(geom, 8046.72) as geom, -- 5 miles
	5 as buff_mi
from
	output.blockgroup_centroid;
create view output.block_centroid as 
select
	geoid20,
	ST_Centroid(geometry) as geom
from
	source.census_blocks_2020;
commit;
/*
creating the full undevelopable land layer (pos, h2o, landuse)
*/
-- union outside source pos geoms
begin;
create temp table union_pos as (
select
	0 as zone,
	a.clipped_geometry as geom
from
	(
	select
		ST_Intersection(ST_Force2D(pos.geometry), d.geometry) as clipped_geometry
	from
		source.ches_bay_watershed_pos pos,
		source.countyboundaries d
	where
		ST_Intersects(ST_Force2D(pos.geometry), d.geometry)) as a
union
select
	0 as zone,
	a.clipped_geometry as geom
from
	(
	select
		ST_Intersection(ST_Force2D(pos.geometry), d.geometry) as clipped_geometry
	from
		source.del_river_basin_pos pos,
		source.countyboundaries d
	where
		ST_Intersects(ST_Force2D(pos.geometry), d.geometry)) as a
union
select
	0 as zone,
	a.clipped_geometry as geom
from
	(
	select
		ST_Intersection(ST_Force2D(pos.geometry), d.geometry) as clipped_geometry
	from
		source.nj_pos pos,
		source.countyboundaries d
	where
		ST_Intersects(ST_Force2D(pos.geometry),
		d.geometry)) as a);
-- removes outside source pos inside of dvrpc region boundary	
create temp table minus_pos as 
select
	ST_Difference(pos.geom, cb.geometry) as difference_geom
from
	(select 
		ST_Union(union_pos.geom) as geom 
	from union_pos 
	group by 
		zone) pos,
	(select ST_Union(geometry) as geometry 
	from source.countyboundaries d 
	where 
		d.dvrpc_reg = 'Yes') cb;
-- adds the dvrpc pos file to the source pos union
create temp table add_dvrpc_pos as
select
	geometry as geom
from
	source.dvrpc_protectedopenspace2020 dpos
union
select
	difference_geom as geom
from
	minus_pos;
-- adds filtered dvrpc land use file
-- Transportation: Highway Right-of-Way (04010), Transportation: Roadway (04011), 
-- Transportation: Rail Right-of-Way (04020), Utility: Right-of-Way (05000), 
-- Utility: Other Facility (05030), Institutional: Cemetery (07050), 
-- Recreation: General (09000), and Undeveloped: Drainage Basin (14020), Water (13000)
create temp table add_dvrpc_lu as
select
	geometry as geom
from
	source.dvrpc_landuse_2015 dlu
union
select
	geom
from
	add_dvrpc_pos;
-- add in the dvrpc hydro to the pos union
create temp table add_h2o as
select
	0 as zone,
	geom
from
	add_dvrpc_lu
union
select
	0 as zone,
	geometry as geom
from
	source.regional_water_bodies;
-- dissolve the geoms to zone 0
create table output.undevelopable as
select
	zone,
	st_union(geom) as geom
from
	add_h2o
group by
	zone;
create index undevelopable_idx on output.undevelopable using GIST (geom);
-- MAYBE DISREGARD pos and h2o only (no landuse stuff)
create temp table add_h2o_2 as
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
-- MAYBE DISREGARD dissolve the geoms to zone 0
create table output.pos_h2o_only as
select
	zone,
	st_union(geom) as geom
from
	add_h2o_2
group by
	zone;
create index pos_h2o_only_idx on output.pos_h2o_only using GIST (geom);
commit;
/*
calculate intersection of undevelopable land and block group polygons to get areas of undevelopable land in block group
*/
begin;
create materialized view output.bg_undev_intersection as
select
	cb.geoid,
	ST_Intersection(cb.geometry, undev.geom) as geom
from
	source.census_blockgroups_2020 as cb
join output.undevelopable as undev on
	ST_Intersects(cb.geometry, undev.geom);
-- output undevelopable/developable block group area in acres
create view output.bg_undev_area_calc as
with area_calcs as (
select
	cb.geoid,
	(SUM(ST_Area(i.geom)))/ 4046.86 as undev_acres,
	ST_Area(cb.geometry)/4046.86 as bg_acres,
	cb.aland/4046.86 as aland_acres
from
	source.census_blockgroups_2020 as cb
left join output.bg_undev_intersection i on
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
		when area_calcs.undev_acres is not null then area_calcs.undev_acres
		else 0
	end as undev_acres,
	area_calcs.aland_acres
from 
	area_calcs)
select
	area_clean.*,
	bg_acres-undev_acres as dev_acres,
	(undev_acres/bg_acres) * 100 as percent_undev
from
	area_clean;
commit;
/*
crosswalk density
*/
begin;
create view output.crosswalk_density as
with blockgroups as (
select
	a.*,
	cbg.geometry as geom
from
	source.census_blockgroups_2020 cbg
join output.bg_undev_area_calc a on
	a.geoid = cbg.geoid
where
	dvrpc_reg = 'y')
select
	bg.geoid,
	count(c.geometry) as crosswalk_count,
	count(c.geometry)/aland_acres as cw_total_density,
	count(c.geometry)/dev_acres as cw_dev_density
from
	source.pedestriannetwork_lines c
join blockgroups bg on
	ST_Intersects(c.geometry, bg.geom)
where dev_acres > 0
group by
	bg.geoid,
	bg.aland_acres,
	bg.dev_acres;
commit;
/*
average commercial stories costar data to block group
*/
begin;
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
	ST_Intersects(costar.geometry,
	cb.geometry)
group by
	cb.geoid;
commit;
/*
commerical square ft calcs from costar data to block group
*/
begin;
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
	ST_Intersects (c.geometry, cbg.geometry)
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
commit;
/*
applying density index values and levels to block groups 
*/
begin;
create view output.bg_density_index_result as 
--join housing units and group quarters to block group
with bg_hu_gq as (
select
	census.geoid,
	bg_area.aland_acres,
	census.housing_units_d20
from
	output.bg_undev_area_calc bg_area
join (
	select
		concat(bg.state, bg.county, bg.tract, bg.block_group) as geoid,
		p5_001n + h1_001n as housing_units_d20
	from
		source.tot_pops_and_hhs_2020_block_group as bg) as census 
on census.geoid = bg_area.geoid
where
	bg_area.aland_acres <> 0),
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
-- calculate density index 
calc_den_indx as (
select
	geoid,
	round(cast((housing_units_d20 + comm_sqft)/ aland_acres as numeric), 3) as density_index
from
	bg_hu_gq_commsqft),
-- creates threshold ranges
match_values as(
select
	t.*,
	lag(t.density_index_thresholds,0,0) over (order by t.density_index_thresholds) as prev_threshold,
	lead(t.density_index_thresholds,1,200000.0) over (order by t.density_index_thresholds) as next_threshold
from
	source.thresholds t)
-- match density index values to values in thresholds ranges
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
	calc_den_indx c
join match_values d on
	c.density_index >= d.prev_threshold
	and c.density_index < d.next_threshold;
commit;
/*
'high or greater' density are allowed buffers that reach across the river 
(if their buffer intersects with it). If it is 'moderate, low, or very low' its buffer, 
if intersecting with the river centerline, should be erased on the opposite side. Perhaps the shape of states whose 
IDs don't match the first two digits of the block group ID can be the "eraser shape".
*/
begin;
create table output.river_buff_adjustment as
-- block group buffers that intersect with the dissolved del river centerline
with riv_buffs as (
select
	bg.geoid,
	bg.buff_mi, 
	bg.geom
from
	output.blockgroup_buffers bg,
	(select ST_Union(geometry) as geom from source.state_streams group by gnis_name) drc
where
	ST_Intersects(bg.geom,drc.geom)),
-- join density index to river blockgroup buffers
riv_buff_density as (
select
	a.geoid,
	a.buff_mi,
	a.geom,
	bda.density_level
from
	riv_buffs a
join output.bg_density_index_result bda on
	a.geoid = bda.geoid),
-- split and cut blockgroup river buffers that have a very low, low, moderate density index
riv_buff_split as (
select
	b.geoid,
	b.buff_mi,
	(ST_Dump(ST_Collectionextract(st_split(b.geom, drc.geom)))).geom as geom
from
	riv_buff_density b,
	(select ST_Union(geometry) as geom from source.state_streams group by gnis_name) drc
where
	b.density_level in ('very low', 'low', 'moderate')),
-- only keep piece of cut buffer where the block group resides
cut_buffs as (
select
	c.*
from
	riv_buff_split c,
	output.blockgroup_centroid bgc
where
	ST_Intersects(c.geom, bgc.geom)
	and 
	bgc.geoid = c.geoid)
-- output all the normal buffers and cut ones
select
	bg.geoid,
	bg.buff_mi,
	case 
		when cut_buffs.geom is null then bg.geom
		else cut_buffs.geom
	end as geom
from
	output.blockgroup_buffers bg
full join cut_buffs on
	cut_buffs.geoid = bg.geoid
	and cut_buffs.buff_mi = bg.buff_mi
order by
	bg.geoid;
commit;
/*
proxmity analysis on 2 and 5 mile block group buffers	
*/
-- do analysis for 2mi buffers
begin;
create materialized view output.costar_bg_2mi as
with buff_2mi as 
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
costar as (
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
	source.not_in_costar nic --these are the records added
-- !!!!!
		),
buff2_costar as (
select
	buff_2mi.geoid,
	buff_2mi.buff_mi,
	count(costar.geometry) as tot_costar,
	sum(costar.comm_sqft) as sum_comm_sqft
from
	buff_2mi
join costar on
	ST_Intersects(costar.geometry, buff_2mi.geom)
group by
	buff_2mi.geoid,
	buff_2mi.buff_mi)
select
	buff_2mi.*,
	buff2_costar.tot_costar,
	buff2_costar.sum_comm_sqft
from
	buff_2mi
left join buff2_costar on
	buff2_costar.geoid = buff_2mi.geoid
	and buff2_costar.buff_mi = buff_2mi.buff_mi;
-- 5 mile buffer analysis
create materialized view output.gq_hu_5mi as
with buff_5mi as (
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
census_data as
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
buff5_census as (
select
	buff_5mi.geoid,
	buff_5mi.buff_mi,
	sum(housing_units_d20) as sum_gq_hu
from
	buff_5mi
join census_data on
	ST_Intersects(buff_5mi.geom, census_data.geom)
group by
	buff_5mi.geoid,
	buff_5mi.buff_mi)
select
	buff_5mi.*,
	buff5_census.sum_gq_hu
from
	buff_5mi
left join buff5_census on
	buff5_census.geoid = buff_5mi.geoid
	and buff5_census.buff_mi = buff_5mi.buff_mi;
-- calculate the proximity index
create view output.bg_proximity_index_result as
with bg_values as
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
calc_proximity as (
select
	bg_values.*,
	(2 * sum_comm_sqft * sum_gq_hu) / (sum_comm_sqft + sum_gq_hu) as proximity_index
from
	bg_values),
-- creates threshold ranges
threshold_ranges as (
select
	t.*,
	lag(t.proximity_index_thresholds, 0, 0) over (order by t.proximity_index_thresholds) as prev_threshold,
	lead(t.proximity_index_thresholds, 1, 500000.0) over (order by t.proximity_index_thresholds) as next_threshold
from
	source.thresholds t)
-- match proximity index values to values in thresholds ranges
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
	calc_proximity b
join threshold_ranges c on
	b.proximity_index >= c.prev_threshold
	and b.proximity_index < c.next_threshold;
commit;
/*
creating development intensity zones by block group	
*/
begin;
create table output.diz_zone as
with all_indexes as (
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
-- uses the classification matrix table to assign diz zone	
add_classification as (
select
	a.*,
	cls.prelim_diz_zone
from
	all_indexes a
left join source.classifications cls on
	cls.proximity_index_levels = a.proximity_level
	and cls.density_index_levels = a.density_level),
diz_w_0_zone as (
select
	c.geoid,
	c.density_index,
	c.density_level,
	c.proximity_index,
	c.proximity_level,
	case
		when c.geoid in (
		select
			geoid
		from
			output.bg_undev_area_calc undev
		where
			undev.percent_undev >= 95) then 0
		else prelim_diz_zone
	end as prelim_diz_zone
from
	add_classification c),
-- join the crosswalk density
diz_w_cw as (
select
	d.*,
	cd.cw_dev_density
from
	diz_w_0_zone d
left join output.crosswalk_density cd on
	cd.geoid = d.geoid),
-- create 50th percentile for crosswalk density by diz zone
crosswalk_summary as (
select
	prelim_diz_zone,
	percentile_cont(0.50) within group (
order by
	cw_dev_density asc) as percentile_50
from
	diz_w_cw d
group by
	prelim_diz_zone),
-- adds +1 to the preliminary diz zone
diz_adjusted as (
select
	d.*,
	case
		when prelim_diz_zone < 6 and prelim_diz_zone > 0 then prelim_diz_zone + 1
		when prelim_diz_zone = 0 then prelim_diz_zone
			else 7
		end as prelim_diz_zone_plus_1
	from
		diz_w_cw d),
-- joins the average stories of costar by bg and cw percentile to the diz+1
diz_adjusted2 as (
select
	da.*,
	cs.avg_stories,
	cws.percentile_50
from
	diz_adjusted da
left join output.costar_stories cs on
	cs.geoid = da.geoid
left join crosswalk_summary cws on
	cws.prelim_diz_zone = da.prelim_diz_zone_plus_1),
-- more adjustments to zone numbering to sw density and costar average stories
diz_adjusted3 as (
select 
	da2.*, 
	case
		when cw_dev_density > percentile_50
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
			diz_adjusted2 da2),
-- 
diz_calcs as (
select
	da3.*,
	case
		when prelim_diz_zone <> 0 then prelim_diz_zone + crosswalk_bonus + stories_bonus
		else 0
	end as diz_zone
from
	diz_adjusted3 da3)
select
	dc.*,
	dzn.diz_zone_name,
	cb.geometry
from
	diz_calcs dc
left join source.diz_zone_names dzn on
	dzn.diz_zone = dc.diz_zone
left join source.census_blockgroups_2020 cb on
	cb.geoid = dc.geoid;
create index diz_zone_idx on output.diz_zone using GIST (geometry);
-- merge diz blockgroups with undevelopable land as protected zone 0
create table output.diz_all as 
with diz_undev_diff as (
select
	ST_Difference(diz.geom, undev.geom) as geom,
	diz.diz_zone
from
	(
	select
		ST_Union(diz_zone.geometry) as geom,
		diz_zone
	from
		output.diz_zone
	group by
		diz_zone) diz,
	(
	select
		ST_Union(undev.geom) as geom
	from
		output.bg_undev_intersection undev) undev),
-- union back difference to cut dizz
add_diff as (
select
	*
from
	diz_undev_diff
union
select
	undev.geom,
	0 as diz_zone
from
	output.bg_undev_intersection undev)
select
	st_union(ad.geom) as geom,
	ad.diz_zone,
	dzn.diz_zone_name 
from
	add_diff ad
join source.diz_zone_names dzn on
	dzn.diz_zone = ad.diz_zone
group by
	ad.diz_zone,
	dzn.diz_zone_name;
create index diz_all_idx on output.diz_all using GIST (geom);
commit;
/*
translating block group diz to various geometries
*/
begin;
create view output.geometry_translation as
-- initial translation table 
select 
	geoid20 as block_id, 
	aland20 as block_aland, 
	left(geoid20, -3) as blockg_id,
	mcd.geoid as mcd_id,
	left(geoid20, -4) as tract_id,
	pd.geoid as phil_id,
	taz.tazt as taz_id
from
	source.census_blocks_2020 cb
left join source.census_mcds_2020 mcd on
	ST_Intersects(mcd.geometry, ST_Centroid (cb.geometry))
left join source.census_mcds_phipd_2020 pd on
	ST_Intersects(pd.geometry, ST_Centroid (cb.geometry))
left join source.taz taz on
	ST_Intersects(taz.geometry, ST_Centroid (cb.geometry));
-- mcd translation w/ weighted average
create table output.diz_mcd as
with weighted_average as (
select
	b.mcd_id,
	sum(t.diz_zone * b.block_aland) / sum(b.block_aland) as diz_weighted_average,
	round(sum(t.diz_zone * b.block_aland) / sum(b.block_aland), 0) as diz_zone
from
	output.geometry_translation b
left join output.diz_zone t on
	b.blockg_id = t.geoid
where
	t.diz_zone > 0
	and block_aland > 0
group by
	b.mcd_id)
select
	wa.*,
	mcd.geometry
from
	weighted_average wa
join source.census_mcds_2020 mcd on
	mcd.geoid = wa.mcd_id;
create index diz_mcd_idx on output.diz_mcd using GIST (geometry);
-- tract translation w/ weighted average
create table output.diz_tract as
with weighted_average as (
select
	b.tract_id,
	sum(t.diz_zone * b.block_aland) / sum(b.block_aland) as diz_weighted_average,
	round(sum(t.diz_zone * b.block_aland) / sum(b.block_aland), 0) as diz_zone
from
	output.geometry_translation b
left join output.diz_zone t on
	b.blockg_id = t.geoid
where
	t.diz_zone > 0
	and block_aland > 0
group by
	b.tract_id)
select
	wa.*,
	ct.geometry
from
	weighted_average wa
join source.census_tracts_2020 ct on
	ct.geoid = wa.tract_id;
create index diz_tract_idx on output.diz_tract using GIST (geometry);
-- philly cpa translation w/ weighted average
create table output.diz_phil as 
with weighted_average as (
select
	b.phil_id,
	sum(t.diz_zone * b.block_aland) / sum(b.block_aland) as diz_weighted_average,
	round(sum(t.diz_zone * b.block_aland) / sum(b.block_aland), 0) as diz_zone
from
	output.geometry_translation b
left join output.diz_zone t on
	b.blockg_id = t.geoid
where
	t.diz_zone > 0
	and block_aland > 0
group by
	b.phil_id)
select
	wa.*,
	phi.geometry
from
	weighted_average wa
join source.census_mcds_phipd_2020 phi on
	phi.geoid = wa.phil_id;
create index diz_phil_idx on output.diz_phil using GIST (geometry);
-- taz translation w/ weighted average
create table output.diz_taz as 
with weighted_average as (
select
	b.taz_id,
	sum(t.diz_zone * b.block_aland) / sum(b.block_aland) as diz_weighted_average,
	round(sum(t.diz_zone * b.block_aland) / sum(b.block_aland), 0) as diz_zone
from
	output.geometry_translation b
left join output.diz_zone t on
	b.blockg_id = t.geoid
where
	t.diz_zone > 0
	and block_aland > 0
group by
	b.taz_id)
select
	wa.*,
	taz.geometry
from
	weighted_average wa
join source.taz on
	taz.tazt = wa.taz_id;
create index diz_taz_idx on output.diz_taz using GIST (geometry);
commit;