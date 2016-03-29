drop table if exists boston_sweep_sched;
create table boston_sweep_sched (
	 id integer,
   csm_id integer,
   street varchar,
   from_st varchar,
   to_st varchar,
   d_id varchar,
   start_time varchar,
   end_time varchar,
   side varchar,
   miles float,
   oneway boolean,
   week1 boolean,
   week2 boolean,
   week3 boolean,
   week4 boolean,
   week5 boolean,
   daily boolean,
   mon boolean,
   tue boolean,
   wed boolean,
   thu boolean,
   fri boolean,
   sat boolean,
   sun boolean,
   allyear boolean
);

-- load from csv
copy boston_sweep_sched from '{}'
WITH CSV HEADER DELIMITER ',' ENCODING 'UTF-8';
