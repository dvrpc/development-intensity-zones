CREATE MATERIALIZED VIEW analysis.test3
TABLESPACE pg_default
AS WITH diz_region_boundary AS (
         SELECT 1 AS dissolve,
            st_union(diz_block_group.geom) AS geom
           FROM analysis.diz_block_group
          GROUP BY 1::integer
        ), pos_and_water AS (
         SELECT pos_h2o_diz_zone_0.zone AS diz_zone,
            'Protected'::text AS diz_zone_name,
            pos_h2o_diz_zone_0.geom
           FROM _raw.pos_h2o_diz_zone_0
        ), pos_and_water_clipped AS (
         SELECT b.diz_zone,
            b.diz_zone_name,
            st_intersection(b.geom, d.geom) AS geom
           FROM pos_and_water b,
            diz_region_boundary d
          WHERE st_intersects(b.geom, d.geom)
        ), diz AS (
         SELECT diz_block_group.diz_zone,
            diz_block_group.diz_zone_name,
            st_union(diz_block_group.geom) AS geom
           FROM analysis.diz_block_group
          GROUP BY diz_block_group.diz_zone, diz_block_group.diz_zone_name
          ORDER BY diz_block_group.diz_zone
        ), diz_pos AS (
         SELECT 0 AS diz_zone,
            'Protected'::character varying AS diz_zone_name,
            st_intersection(pos_and_water_clipped.geom, diz.geom) AS geom
           FROM diz,
            pos_and_water_clipped
        UNION
         SELECT diz.diz_zone,
            diz.diz_zone_name,
            diz.geom
           FROM diz
        )
 SELECT row_number() OVER () AS row_number,
    diz_pos.diz_zone,
    diz_pos.diz_zone_name,
    st_union(diz_pos.geom) AS st_union
   FROM diz_pos
  GROUP BY diz_pos.diz_zone, diz_pos.diz_zone_name
WITH DATA;