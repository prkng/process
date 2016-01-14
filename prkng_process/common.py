# -*- coding: utf-8 -*-
from __future__ import unicode_literals


rules_columns = (
    'code',
    'description',
    'season_start',
    'season_end',
    'time_max_parking',
    'agenda',
    'special_days',
    'restrict_types',
    'permit_no'
)

create_rules = """
DROP TABLE IF EXISTS rules;
CREATE TABLE rules (
    id serial PRIMARY KEY
    , code varchar
    , description varchar
    , season_start varchar DEFAULT ''
    , season_end varchar DEFAULT ''
    , time_max_parking float DEFAULT 0.0
    , agenda jsonb
    , special_days varchar DEFAULT ''
    , restrict_types varchar[]
    , permit_no varchar
)
"""

get_rules_from_source = """
SELECT
    code
    , description
    , season_start
    , season_end
    , time_max_parking
    , time_start
    , time_end
    , time_duration
    , lun
    , mar
    , mer
    , jeu
    , ven
    , sam
    , dim
    , daily
    , special_days
    , restrict_types
    , permit_no
FROM {source}
"""

create_slots_temp = """
DROP TABLE IF EXISTS {city}_slots_temp;
CREATE TABLE {city}_slots_temp
(
  id serial PRIMARY KEY,
  r15id varchar,
  position float,
  signposts integer[],
  rules jsonb,
  way_name varchar,
  geom geometry(LineString,3857)
)
"""

create_slots = """
CREATE TABLE IF NOT EXISTS slots
(
  id varchar PRIMARY KEY,
  city varchar,
  r15id varchar,
  signposts integer[],
  rules jsonb,
  way_name varchar,
  geom geometry(LineString,3857),
  geojson jsonb,
  centerpoint jsonb,
  button_locations jsonb
)
"""

create_slots_partition = """
DROP RULE IF EXISTS slots_insert_{city} ON slots;
DROP TABLE IF EXISTS {city}_slots;
CREATE TABLE {city}_slots (
    CHECK ( city = '{city}' )
) INHERITS (slots);

CREATE RULE slots_insert_{city} AS
    ON INSERT TO slots
        WHERE ( city = '{city}' )
    DO INSTEAD
        INSERT INTO {city}_slots VALUES (NEW.*);
"""

create_parking_lots_raw = """
DROP TABLE IF EXISTS {city}_parking_lots;
CREATE TABLE {city}_parking_lots (
  id serial primary key,
  name varchar,
  operator varchar,
  address varchar,
  description varchar,
  lun_normal varchar,
  mar_normal varchar,
  mer_normal varchar,
  jeu_normal varchar,
  ven_normal varchar,
  sam_normal varchar,
  dim_normal varchar,
  hourly_normal float,
  max_normal float,
  daily_normal float,
  lun_special varchar,
  mar_special varchar,
  mer_special varchar,
  jeu_special varchar,
  ven_special varchar,
  sam_special varchar,
  dim_special varchar,
  hourly_special float,
  max_special float,
  daily_special float,
  lun_free varchar,
  mar_free varchar,
  mer_free varchar,
  jeu_free varchar,
  ven_free varchar,
  sam_free varchar,
  dim_free varchar,
  daily_free float,
  indoor boolean,
  handicap boolean,
  card boolean,
  valet boolean,
  lat float,
  long float,
  capacity integer,
  street_view_lat float,
  street_view_long float,
  street_view_head float,
  street_view_id varchar,
  active boolean,
  partner_name varchar,
  partner_id varchar
)
"""

create_parking_lots = """
DROP TABLE IF EXISTS parking_lots;
CREATE TABLE parking_lots
(
  id serial PRIMARY KEY,
  active boolean,
  partner_id integer,
  partner_name varchar,
  city varchar,
  name varchar,
  operator varchar,
  capacity integer,
  available integer,
  address varchar,
  description varchar,
  agenda jsonb,
  attrs jsonb,
  geom geometry(Point,3857),
  geojson jsonb,
  street_view jsonb
)
"""

aggregate_like_slots = """
DO
$$
DECLARE
  slot record;
  scount integer;
  id_match varchar;
BEGIN
  FOR slot IN SELECT * FROM {city}_slots_temp ORDER BY r15id, position LOOP
    SELECT COUNT(*) FROM slots s WHERE slot.r15id = s.r15id INTO scount;
    SELECT id FROM slots s
      WHERE slot.r15id = s.r15id
        AND slot.rules = s.rules
        AND ST_DWithin(slot.geom, s.geom, 0.1)
      LIMIT 1 INTO id_match;

    IF id_match IS NULL THEN
      INSERT INTO slots (id, city, r15id, signposts, rules, geom, way_name) VALUES
        ((slot.r15id || to_char(scount, 'fm00')), '{city}', slot.r15id, ARRAY[slot.signposts],
            slot.rules, slot.geom, slot.way_name);
    ELSE
      UPDATE slots SET geom =
        (CASE WHEN ST_DWithin(ST_StartPoint(slot.geom), ST_EndPoint(geom), 0.5)
            THEN ST_MakeLine(geom, slot.geom)
            ELSE ST_MakeLine(slot.geom, geom)
        END),
        signposts = (signposts || ARRAY[slot.signposts])
      WHERE slots.id = id_match;
    END IF;
  END LOOP;
END;
$$ language plpgsql;
"""

cut_slots_crossing_slots = """
UPDATE {city}_slots_temp s set geom = (
with tmp as (
select
    array_sort(
        array_agg(
            ST_Line_Locate_Point(s.geom, st_intersection(s.geom, o.geom))
        )
    ) as locations
from {city}_slots_temp o
where st_crosses(s.geom, o.geom) and s.id != o.id
and st_geometrytype(st_intersection(s.geom, o.geom)) = 'ST_Point'
)
select
    st_linesubstring(s.geom, locs.start, locs.stop)::geometry('linestring', 3857)
from tmp, get_max_range(tmp.locations) as locs
)
where exists (
    select 1 from {city}_slots_temp a
    where st_crosses(s.geom, a.geom)
          and s.id != a.id
          and st_geometrytype(st_intersection(s.geom, a.geom)) = 'ST_Point'
)
"""

cut_slots_crossing_roads = """
WITH exclusions AS (
    SELECT s.id, ST_Difference(s.geom, ST_Union(ST_Buffer(r.geom, {offset}, 'endcap=flat join=round'))) AS new_geom
    FROM {city}_slots_temp s
    JOIN roads r ON ST_DWithin(s.geom, r.geom, 4)
    GROUP BY s.id, s.geom
), update_original AS (
    DELETE FROM {city}_slots_temp
    USING exclusions
    WHERE {city}_slots_temp.id = exclusions.id
    RETURNING {city}_slots_temp.*
), new_slots AS (
    SELECT
        uo.*,
        CASE ST_GeometryType(ex.new_geom)
            WHEN 'ST_LineString' THEN
                ex.new_geom
            ELSE
                (ST_Dump(ex.new_geom)).geom
        END AS new_geom
    FROM exclusions ex
    JOIN update_original uo ON ex.id = uo.id
)
INSERT INTO {city}_slots_temp (r15id, position, signposts, rules, way_name, geom)
    SELECT
        r15id,
        position,
        signposts,
        rules,
        way_name,
        new_geom
    FROM new_slots
    WHERE ST_Length(new_geom) >= 4
"""

create_client_data = """
UPDATE slots SET
    geojson = ST_AsGeoJSON(ST_Transform(geom, 4326))::jsonb,
    centerpoint = json_build_object('long', ST_X(ST_Transform(ST_Line_Interpolate_Point(geom, 0.5), 4326)),
        'lat', ST_Y(ST_Transform(ST_Line_Interpolate_Point(geom, 0.5), 4326)))::jsonb,
    button_locations = (case when st_length(geom) >= 300 then array_to_json(array[
        json_build_object('long', ST_X(ST_Transform(ST_Line_Interpolate_Point(geom, 0.333), 4326)),
            'lat', ST_Y(ST_Transform(ST_Line_Interpolate_Point(geom, 0.333), 4326))),
        json_build_object('long', ST_X(ST_Transform(ST_Line_Interpolate_Point(geom, 0.666), 4326)),
            'lat', ST_Y(ST_Transform(ST_Line_Interpolate_Point(geom, 0.666), 4326)))])::jsonb
        else array_to_json(array[
            json_build_object('long', ST_X(ST_Transform(ST_Line_Interpolate_Point(geom, 0.5), 4326)),
            'lat', ST_Y(ST_Transform(ST_Line_Interpolate_Point(geom, 0.5), 4326)))])::jsonb end)
    WHERE city = '{city}'
"""


create_permit_lists = """
DROP TABLE IF EXISTS permits;
CREATE TABLE permits (
    id serial primary key,
    city varchar,
    permit varchar,
    residential boolean
);
"""

insert_permit_lists = """
    INSERT INTO permits (city, permit, residential)
    SELECT DISTINCT
        '{city}',
        rules->>'permit_no',
        NOT (rules->>'permit_no' = ANY(ARRAY['bus','motorcycle','commercial','press','carshare','carpool']))
    FROM (
        SELECT jsonb_array_elements(rules) AS rules FROM slots WHERE city = '{city}'
    ) foo
    WHERE rules->>'permit_no' != ''
    ORDER BY 1;
"""
