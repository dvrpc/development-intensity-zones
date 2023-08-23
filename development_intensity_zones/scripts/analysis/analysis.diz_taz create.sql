DROP VIEW IF EXISTS analysis.diz_taz;


CREATE VIEW analysis.diz_taz AS WITH weighted_averages AS
    (SELECT b.taz_id,
            sum(t.diz_zone * b.aland) / sum(b.aland) AS diz_weighted_average,
            round(sum(t.diz_zone * b.aland) / sum(b.aland), 0) AS diz_zone
     FROM _resources.block2020_parent_geos b
     LEFT JOIN analysis.diz_block_group t ON b.block_group20_id = t.block_group20
     WHERE t.diz_zone > 0
     GROUP BY b.taz_id)
SELECT w.taz_id,
       w.diz_weighted_average,
       w.diz_zone,
       z.diz_zone_name,
       d.shape AS geom
FROM weighted_averages w
LEFT JOIN _raw.taz_2010_mcdaligned d ON w.taz_id = d.tazt::text
LEFT JOIN _resources.diz_zone_names z ON w.diz_zone = z.diz_zone::numeric;