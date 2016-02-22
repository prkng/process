DROP TABLE IF EXISTS boston_rules_translation;
CREATE TABLE boston_rules_translation (
    id serial,
    code varchar,
    description varchar,
    periods varchar DEFAULT '{{}}',
    time_max_parking float DEFAULT 0.0,
    time_start float,
    time_end float,
    time_duration float,
    lun smallint,
    mar smallint,
    mer smallint,
    jeu smallint,
    ven smallint,
    sam smallint,
    dim smallint,
    daily float,
    special_days varchar DEFAULT '',
    restrict_types varchar DEFAULT '',
    permit_no varchar DEFAULT ''
);

copy boston_rules_translation (code,description,periods,time_max_parking,
    time_start,time_end,time_duration,lun,mar,mer,jeu,ven,
    sam,dim,daily,special_days,restrict_types)
from '{}'
WITH CSV HEADER DELIMITER ',' ENCODING 'UTF-8';
