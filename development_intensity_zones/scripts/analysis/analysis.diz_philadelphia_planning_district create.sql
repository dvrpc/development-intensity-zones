DROP VIEW IF EXISTS analysis.diz_philadelphia_planning_district;


CREATE VIEW analysis.diz_philadelphia_planning_district AS WITH weighted_averages AS
    (SELECT regexp_replace(b.district_id, '\.0', '') as d_id,
            sum(t.diz_zone * b.aland) / sum(b.aland) AS diz_weighted_average,
            round(sum(t.diz_zone * b.aland) / sum(b.aland), 0) AS diz_zone
     FROM _resources.block2020_parent_geos b
     LEFT JOIN analysis.diz_block_group t ON b.block_group20_id = t.block_group20
     WHERE b.district_id <> 'nan'
         AND b.district_id != '0.0'
         AND t.diz_zone > 0
     GROUP BY d_id)
SELECT w.d_id,
       w.diz_weighted_average,
       w.diz_zone,
       z.diz_zone_name,
       d.shape AS geom
FROM weighted_averages w
LEFT JOIN _raw.census_mcds_phipd_2020 d ON w.d_id = d.geoid::text
LEFT JOIN _resources.diz_zone_names z ON w.diz_zone = z.diz_zone::numeric;