drop table if exists analysis.incorp_del_river_bg_centroids_24co_2020_buffers;

create table analysis.incorp_del_river_bg_centroids_24co_2020_buffers as 
with /*The rest of this script from here on down was created by Sean Lawrence, I just wrote the 2 lines above, more indented most of the lines below, and changed "bones_test_results" to "bones_test_results_step2" to accommodate for the resulting scripting process change*/
	a as (
		select
			bg."GEOID",
			bg.buff_mi, 
			bg.geom
		from
			analysis.block_group_centroid_buffers_24co_2020 bg,
			_raw.delaware_river_centerline drc
		where
			st_intersects(bg.geom,drc.geom)
		),
	b as (
		select
			a."GEOID",
			a.buff_mi,
			a.geom,
			bda.density_level
		from
			a
		join analysis.bones_test_results_step2 bda on
			a."GEOID" = bda.block_group20
		),
	c as (
		select
			b."GEOID",
			b.buff_mi,
			(st_dump(st_collectionextract(st_split(b.geom,drc.geom)))).geom as geom
		from
			b,
			_raw.delaware_river_centerline drc
		where
			b.density_level in ('very low', 'low', 'moderate')
		),
	d as (
		select
			c.*
		from
			c,
			analysis.block_group_centroids_24co_2020 bgc
		where
			st_intersects(c.geom,bgc.geom)
			and bgc."GEOID" = c."GEOID"
		)
		

select
	bg."GEOID",
	bg.buff_mi,
	case 
		when d.geom is null then bg.geom 
		else d.geom 
	end as geom
from
	analysis.block_group_centroid_buffers_24co_2020 bg
full join d on
	d."GEOID" = bg."GEOID" and d.buff_mi = bg.buff_mi 
order by bg."GEOID"