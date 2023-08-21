drop table if exists analysis.diz cascade;
create table analysis.diz as

with
	diz_region_boundary as (
		select 1 as dissolve, st_union(geom) as geom 
		from analysis.diz_block_group 
		group by dissolve
		),
	pos_and_water as (
		select zone as diz_zone, 'Protected' as diz_zone_name, geom 
		from _raw.pos_h2o_diz_zone_0
		),
	pos_and_water_clipped as (
		select 
			b.diz_zone,
			b.diz_zone_name,
			st_intersection(b.geom, d.geom) as clipped_geom 
		from pos_and_water b, diz_region_boundary d
		where st_intersects(b.geom, d.geom)
		),
	diz_dissolved_by_zone as (
		select diz_zone, diz_zone_name, st_union(geom) as geom 
		from analysis.diz_block_group 
		group by diz_zone, diz_zone_name
		),
	diz as (
		diz_dissolved_by_zone union
		(
			select diz_zone, diz_zone_name, clipped_geom as geom 
			from pos_and_water_clipped
		)
		order by diz_zone
		)
    
    
    select row_number() over() as row_number, diz_zone, diz_zone_name, geom from diz