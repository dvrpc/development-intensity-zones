drop table if exists _raw.crosswalks;
create table _raw.crosswalks as
select * from _raw.pedestriannetwork_lines pl 
where line_type = 2;