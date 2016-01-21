DROP TABLE IF EXISTS montreal_data_verdun;
CREATE TABLE montreal_data_verdun (
    id serial,
    id_trc varchar,
    rue_nom varchar,
    rue_de varchar,
    rue_a varchar,
    id_trc_pair varchar,
    rule_pair_1 varchar,
    rule_pair_2 varchar,
    id_trc_impair varchar,
    rule_impair_1 varchar,
    rule_impair_2 varchar
);

COPY montreal_data_verdun (id_trc, rue_nom, rue_de, rue_a, id_trc_pair, rule_pair_1,
    rule_pair_2, id_trc_impair, rule_impair_1, rule_impair_2)
from '{}'
WITH CSV HEADER DELIMITER ',' ENCODING 'UTF-8';
