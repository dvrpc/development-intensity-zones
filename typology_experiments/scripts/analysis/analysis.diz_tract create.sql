DROP VIEW IF EXISTS analysis.transect_tract_translation;


CREATE VIEW analysis.transect_tract_translation AS WITH weighted_averages AS
    (SELECT b.tract20_id,
            sum(t.transect_zone * b.aland) / sum(b.aland) AS transect_weighted_average,
            round(sum(t.transect_zone * b.aland) / sum(b.aland), 0) AS transect_zone
     FROM _resources.block2020_parent_geos b
     LEFT JOIN analysis.transect t ON b.block_group20_id = t.block_group20
     GROUP BY b.tract20_id)
SELECT w.tract20_id,
       w.transect_weighted_average,
       w.transect_zone,
       z.transect_zone_name,
       d.shape AS geom
FROM weighted_averages w
LEFT JOIN _raw.census_tracts_2020 d ON w.tract20_id = d.geoid::text
LEFT JOIN _resources.transect_zone_names z ON w.transect_zone = z.transect_zone::numeric;