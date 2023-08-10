DROP VIEW IF EXISTS analysis.transect_philadelphia_planning_district_translation;


CREATE VIEW analysis.transect_philadelphia_planning_district_translation AS WITH weighted_averages AS
    (SELECT regexp_replace(b.district_id, '\.0', '') as d_id,
            sum(t.transect_zone * b.aland) / sum(b.aland) AS transect_weighted_average,
            round(sum(t.transect_zone * b.aland) / sum(b.aland), 0) AS transect_zone
     FROM _resources.block2020_parent_geos b
     LEFT JOIN analysis.transect t ON b.block_group20_id = t.block_group20
     WHERE b.district_id <> 'nan'
         AND b.district_id != '0.0'
     GROUP BY d_id)
SELECT w.d_id,
       w.transect_weighted_average,
       w.transect_zone,
       z.transect_zone_name,
       d.shape AS geom
FROM weighted_averages w
LEFT JOIN _raw.census_mcds_phipd_2020 d ON w.d_id = d.geoid::text
LEFT JOIN _resources.transect_zone_names z ON w.transect_zone = z.transect_zone::numeric;