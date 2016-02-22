# -*- coding: utf-8 -*-
from __future__ import unicode_literals


# create table hosting all signs
create_sign = """
DROP TABLE IF EXISTS boston_sign;
CREATE TABLE boston_sign (
    id serial PRIMARY KEY
    , geom geometry(Point, 3857)
    , roadsegment integer
    , distance float
    , direction smallint -- direction the rule applies
    , code varchar -- code of rule
    , description varchar -- description of rule
)
"""


insert_sign = """
WITH wholeroads AS (
    SELECT min(StreetName) AS name, min(StreetNa_1) AS alt_name, ST_LineMerge(ST_Union(geom))
    FROM boston_geobase
    GROUP BY StreetList
), substrings AS (
    SELECT b.id, r.name, array_sort(array_agg(DISTINCT ST_Line_Locate_Point(r.geom, ST_Intersection(r.geom, s.geom)))) AS linepoints
    FROM boston_sweep_sched b
    JOIN wholeroads r ON (b.street = r.name OR b.street = r.alt_name)
    JOIN boston_geobase s ON ST_DWithin(s.geom, r.geom, 1)
    WHERE (b.from = s.StreetName OR b.from = s.StreetNa_1) OR (b.to = s.StreetName OR b.to = s.StreetNa_1)
), linebufs AS (
    SELECT s.id, ST_Buffer(ST_LineSubstring(r.geom, s.linepoints[0], s.linepoints[1]), 5, 'endcap=flat') AS geom
    FROM substrings s
    JOIN wholeroads r ON r.name = s.name
), lines AS (
    SELECT r.id, s.StreetName, s.RoadSegmen
    FROM boston_geobase s
    JOIN linebufs r ON ST_Contains(r.geom, s.geom)
    GROUP BY r.id
), linesides AS (
    SELECT r.id, a.geom, r.RoadSegmen, r.geom AS road_geom,
        ROW_NUMBER() OVER (PARTITION BY r.RoadSegmen ORDER BY ST_Distance(a.geom, r.geom))
    FROM boston_addresses a
    JOIN lines r ON upper(a.street_bod + ' ' + a.street_suf) = r.StreetName
    JOIN boston_sweep_sched b ON r.id = b.id
    WHERE (a.STREET_NUM % 2 = 0 AND b.side = 'even') OR (a.STREET_NUM % 2 = 1 AND b.side = 'odd')
)
INSERT INTO boston_sign (geom, roadsegment, distance, direction, code, description)
SELECT
    l.geom,
    l.RoadSegmen,
    ST_Distance(l.geom, ST_StartPoint(ST_LineMerge(l.road_geom))),
    0,
    r.code,
    r.description
FROM linesides l
JOIN rules r ON r.code = ('BOS-SSWP-' || l.id)
WHERE l.rank = 1
"""


# try to match osm ways with geobase
match_roads_geobase = """
DROP TABLE IF EXISTS boston_roads_geobase;
CREATE TABLE boston_roads_geobase (
    id integer
    , osm_id bigint
    , name varchar
    , roadsegment integer
    , geom geometry(Linestring, 3857)
);

WITH tmp AS (
    SELECT
        o.*
        , m.roadsegmen AS roadsegment
        , rank() OVER (
            PARTITION BY o.id ORDER BY
              ST_HausdorffDistance(o.geom, m.geom)
              , levenshtein(o.name, m.street_nam)
              , abs(ST_Length(o.geom) - ST_Length(m.geom)) / greatest(ST_Length(o.geom), ST_Length(m.geom))
          ) AS rank
    FROM roads o
    JOIN boston_geobase m ON o.geom && ST_Expand(m.geom, 10)
    WHERE ST_Contains(ST_Buffer(m.geom, 30), o.geom)
)
INSERT INTO boston_roads_geobase
SELECT
    DISTINCT ON (id)
    id
    , osm_id
    , name
    , roadsegment
    , geom
FROM tmp
WHERE rank = 1;


-- invert buffer comparison to catch more ways
WITH tmp AS (
      SELECT
          o.*
          , m.roadsegmen AS roadsegment
          , rank() OVER (
              PARTITION BY o.id ORDER BY
                ST_HausdorffDistance(o.geom, m.geom)
                , levenshtein(o.name, m.street_nam)
                , abs(ST_Length(o.geom) - ST_Length(m.geom)) / greatest(ST_Length(o.geom), ST_Length(m.geom))
            ) AS rank
      FROM roads o
      LEFT JOIN boston_roads_geobase orig ON orig.id = o.id
      JOIN boston_geobase m ON o.geom && ST_Expand(m.geom, 10)
      WHERE ST_Contains(ST_Buffer(o.geom, 30), m.geom)
        AND orig.id IS NULL
)
INSERT INTO boston_roads_geobase
SELECT
    DISTINCT ON (id)
    id
    , osm_id
    , name
    , roadsegment
    , geom
FROM tmp
WHERE rank = 1;
"""


# create signpost table
create_signpost = """
DROP TABLE IF EXISTS boston_signpost;
CREATE TABLE boston_signpost (
    id serial PRIMARY KEY
    , geobase_id integer
    , signs integer[]
    , geom geometry(Point, 3857)
);
"""


insert_signpost = """
INSERT INTO boston_signpost (geobase_id, signs, geom)
SELECT
    min(s.roadsegment),
    array_agg(DISTINCT s.id),
    ST_SetSRID(ST_MakePoint(avg(ST_X(s.geom)), avg(ST_Y(s.geom))), 3857)
FROM boston_sign s
JOIN boston_roads_geobase g ON s.roadsegment = g.roadsegment
GROUP BY s.roadsegment, s.distance, ST_isLeft(g.geom, s.geom)
"""


# project signposts on road and
# determine if they were on the left side or right side of the road
project_signposts = """
DROP TABLE IF EXISTS boston_signpost_onroad;
CREATE TABLE boston_signpost_onroad AS
    SELECT
        DISTINCT ON (sp.id) sp.id  -- hack to prevent duplicata
        , s.id AS road_id
        , ST_ClosestPoint(s.geom, sp.geom)::geometry(point, 3857) AS geom
        , ST_isLeft(s.geom, sp.geom) AS isleft
    FROM boston_signpost sp
    JOIN boston_roads_geobase s ON sp.geobase_id = s.roadsegment
    ORDER BY sp.id, ST_Distance(s.geom, sp.geom);

SELECT id FROM boston_signpost_onroad GROUP BY id HAVING count(*) > 1
"""


# how many signposts have been projected ?
count_signpost_projected = """
WITH tmp AS (
    SELECT
        (SELECT count(*) FROM boston_signpost_onroad) as a
        , (SELECT count(*) FROM boston_signpost) as b
)
SELECT
    a::float / b * 100, b
FROM tmp
"""


# generate signposts orphans
generate_signposts_orphans = """
DROP TABLE IF EXISTS boston_signposts_orphans;
CREATE TABLE boston_signposts_orphans AS
(WITH tmp as (
    SELECT id FROM boston_signpost
    EXCEPT
    SELECT id FROM boston_signpost_onroad
) SELECT
    s.*
FROM tmp t
JOIN boston_signpost s using(id)
)
"""


# create potential slots determined with signposts projected as start and end points
create_slots_likely = """
DROP TABLE IF EXISTS boston_slots_likely;
CREATE TABLE boston_slots_likely(
    id serial
    , signposts integer[]
    , rid integer  -- road id
    , position float
    , geom geometry(linestring, 3857)
);
"""


insert_slots_likely = """
WITH selected_roads AS (
    SELECT
        r.id as rid
        , r.geom as rgeom
        , p.id as pid
        , p.geom as pgeom
    FROM boston_roads_geobase r, boston_signpost_onroad p
    where r.geom && p.geom
        AND r.id = p.road_id
        AND p.isleft = {isleft}
), point_list AS (
    SELECT
        distinct rid
        , 0 as position
        , 0 as signpost
    FROM selected_roads
UNION ALL
    SELECT
        distinct rid
        , 1 as position
        , 0 as signpost
    FROM selected_roads
UNION ALL
    SELECT
        rid
        , st_line_locate_point(rgeom, pgeom) as position
        , pid as signpost
    FROM selected_roads
), loc_with_idx as (
    SELECT DISTINCT ON (rid, position)
        rid
        , position
        , rank() over (partition by rid order by position) as idx
        , signpost
    FROM point_list
)
INSERT INTO boston_slots_likely (signposts, rid, position, geom)
SELECT
    ARRAY[loc1.signpost, loc2.signpost]
    , w.id
    , loc1.position as position
    , st_line_substring(w.geom, loc1.position, loc2.position) as geom
FROM loc_with_idx loc1
JOIN loc_with_idx loc2 using (rid)
JOIN boston_roads_geobase w on w.id = loc1.rid
WHERE loc2.idx = loc1.idx+1;
"""


create_nextpoints_for_signposts = """
DROP TABLE IF EXISTS boston_nextpoints;
CREATE TABLE boston_nextpoints AS
(WITH tmp as (
SELECT
    spo.id
    , sl.id as slot_id
    , spo.geom as spgeom
    , case
        when st_equals(
                ST_SnapToGrid(st_startpoint(sl.geom), 0.01),
                ST_SnapToGrid(spo.geom, 0.01)
            ) then st_pointN(sl.geom, 2)
        when st_equals(
                ST_SnapToGrid(st_endpoint(sl.geom), 0.01),
                ST_SnapToGrid(spo.geom, 0.01)
            ) then st_pointN(st_reverse(sl.geom), 2)
        else NULL
      end as geom
    , sp.geom as sgeom
FROM boston_signpost_onroad spo
JOIN boston_signpost sp on sp.id = spo.id
JOIN boston_slots_likely sl on ARRAY[spo.id] <@ sl.signposts
) select
    id
    , slot_id
    , CASE  -- compute signed area to find if the nexpoint is on left or right
        WHEN
            sign((st_x(sgeom) - st_x(spgeom)) * (st_y(geom) - st_y(spgeom)) -
            (st_x(geom) - st_x(spgeom)) * (st_y(sgeom) - st_y(spgeom))) = 1 THEN 1 -- on left
        ELSE 2 -- right
        END as direction
    , geom
from tmp)
"""


insert_slots_temp = """
WITH tmp AS (
    -- select north and south from signpost
    SELECT
        sl.*
        , s.code
        , s.description
        , s.direction
        , spo.isleft
        , rb.name
    FROM boston_slots_likely sl
    JOIN boston_sign s on ARRAY[s.signpost] <@ sl.signposts
    JOIN boston_signpost_onroad spo on s.signpost = spo.id
    JOIN boston_nextpoints np on np.slot_id = sl.id AND
                          s.signpost = np.id AND
                          s.direction = np.direction
    JOIN boston_roads_geobase rb on spo.road_id = rb.id

    UNION ALL
    -- both direction from signpost
    SELECT
        sl.*
        , s.code
        , s.description
        , s.direction
        , spo.isleft
        , rb.name
    FROM boston_slots_likely sl
    JOIN boston_sign s on ARRAY[s.signpost] <@ sl.signposts and direction = 0
    JOIN boston_signpost_onroad spo on s.signpost = spo.id
    JOIN boston_roads_geobase rb on spo.road_id = rb.id
),
selection as (
SELECT
    distinct on (t.id) t.id
    , min(signposts) as signposts
    , min(isleft) as isleft
    , min(rid) as rid
    , min(position) as position
    , min(name) as way_name
    , array_to_json(
        array_agg(distinct
        json_build_object(
            'code', t.code,
            'description', r.description,
            'periods', r.periods,
            'agenda', r.agenda,
            'time_max_parking', r.time_max_parking,
            'special_days', r.special_days,
            'restrict_types', r.restrict_types,
            'permit_no', (CASE WHEN r.permit_no = '' THEN NULL ELSE r.permit_no END)
        )::jsonb
    ))::jsonb as rules
    , ST_OffsetCurve(min(t.geom), ({offset} * min(isleft)),
        'quad_segs=4 join=round')::geometry(linestring, 3857) AS geom
FROM tmp t
JOIN rules r ON t.code = r.code
WHERE ST_GeometryType(ST_OffsetCurve(t.geom, ({offset} * isleft), 'quad_segs=4 join=round')::geometry) = 'ST_LineString'
GROUP BY t.id
) INSERT INTO boston_slots_temp (rid, position, signposts, rules, geom, way_name)
SELECT
    rid
    , position
    , signposts
    , rules
    , geom
    , way_name
FROM selection
"""


create_slots_for_debug = """
DROP TABLE IF EXISTS boston_slots_debug;
CREATE TABLE boston_slots_debug as
(
    WITH tmp as (
    -- select north and south from signpost
    SELECT
        sl.*
        , s.code
        , s.description
        , s.direction
        , spo.isleft
        , rb.name
    FROM boston_slots_likely sl
    JOIN boston_sign s on ARRAY[s.signpost] <@ sl.signposts
    JOIN boston_signpost_onroad spo on s.signpost = spo.id
    JOIN boston_nextpoints np on np.slot_id = sl.id AND
                          s.signpost = np.id AND
                          s.direction = np.direction
    JOIN boston_roads_geobase rb on spo.road_id = rb.id

    UNION ALL
    -- both direction from signpost
    SELECT
        sl.*
        , s.code
        , s.description
        , s.direction
        , spo.isleft
        , rb.name
    FROM boston_slots_likely sl
    JOIN boston_sign s on ARRAY[s.signpost] <@ sl.signposts and direction = 0
    JOIN boston_signpost_onroad spo on s.signpost = spo.id
    JOIN boston_roads_geobase rb on spo.road_id = rb.id
)
SELECT
    distinct on (t.id, t.code)
    row_number() over () as pkid
    , t.id
    , t.code
    , t.signposts
    , t.isleft
    , t.name as way_name
    , rt.description
    , rt.periods
    , rt.time_max_parking
    , rt.time_start
    , rt.time_end
    , rt.time_duration
    , rt.lun
    , rt.mar
    , rt.mer
    , rt.jeu
    , rt.ven
    , rt.sam
    , rt.dim
    , rt.daily
    , rt.special_days
    , rt.restrict_types
    , r.agenda::text as agenda
    , ST_OffsetCurve(t.geom, ({offset} * t.isleft), 'quad_segs=4 join=round')::geometry(linestring, 3857) AS geom
FROM tmp t
JOIN rules r on t.code = r.code
JOIN boston_rules_translation rt on rt.code = r.code
WHERE ST_GeometryType(ST_OffsetCurve(t.geom, ({offset} * t.isleft), 'quad_segs=4 join=round')::geometry) = 'ST_LineString'
)
"""
