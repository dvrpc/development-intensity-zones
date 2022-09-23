drop table if exists analysis.block_groups_dvrpc_2020;

create table analysis.block_groups_dvrpc_2020 as
with
	block_groups_dvrpc_2020_with_potential_duplicates as (
		select * from _raw.tl_2020_34_bg where "COUNTYFP" in ('005', '007', '015', '021') union 
		select * from _raw.tl_2020_42_bg where "COUNTYFP" in ('017', '029', '045', '091', '101')
	)
    
    
    select distinct on ("STATEFP", "COUNTYFP", "TRACTCE", "BLKGRPCE", "GEOID", "NAMELSAD", "ALAND", "AWATER") "STATEFP", "COUNTYFP", "TRACTCE", "BLKGRPCE", "GEOID", "NAMELSAD", "ALAND", "AWATER", geom from block_groups_dvrpc_2020_with_potential_duplicates /*FOUND OUT HOW TO GET DISTINCT ROWS OF A DATA FRAME BY ONLY CERTAIN COLUMNS FROM https://stackoverflow.com/a/16918028 (IN TURN FOUND ON https://stackoverflow.com/questions/16913969/postgres-distinct-but-only-for-one-column )*/ 