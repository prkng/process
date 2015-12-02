CREATE TABLE IF NOT EXISTS parking_lots_streetview (
    id serial,
    partner_id varchar,
    partner_name varchar,
    name varchar,
    address varchar,
    street_view_lat float,
    street_view_long float,
    street_view_head float,
    street_view_id varchar
);

copy parking_lots_streetview (partner_id, partner_name, name, address, street_view_lat,
  street_view_long, street_view_head, street_view_id)
from '{}'
WITH CSV HEADER DELIMITER ',' ENCODING 'UTF-8';
