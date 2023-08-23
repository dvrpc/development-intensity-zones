DROP VIEW IF EXISTS analysis.diz_mcd;


CREATE VIEW analysis.diz_mcd AS WITH weighted_averages AS
    (SELECT b.mcd20_id,
            sum(t.diz_zone * b.aland) / sum(b.aland) AS diz_weighted_average,
            round(sum(t.diz_zone * b.aland) / sum(b.aland), 0) AS diz_zone
     FROM _resources.block2020_parent_geos b
     LEFT JOIN analysis.diz_block_group t ON b.block_group20_id = t.block_group20
     WHERE t.diz_zone > 0
     GROUP BY b.mcd20_id)
SELECT w.mcd20_id,
       w.diz_weighted_average,
       w.diz_zone,
       z.diz_zone_name,
       d.shape AS geom
FROM weighted_averages w
LEFT JOIN _raw.census_mcds_2020 d ON w.mcd20_id = d.geoid::text
LEFT JOIN _resources.diz_zone_names z ON w.diz_zone = z.diz_zone::numeric;