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
    , signpost integer
    , direction smallint -- direction the rule applies
    , code varchar -- code of rule
    , description varchar -- description of rule
)
"""


insert_sign = """
WITH wholeroads AS (
    SELECT StreetList AS id, min(StreetName) AS name, min(StreetNa_1) AS alt_name,
        ST_LineMerge(ST_Union(geom)) AS geom
    FROM boston_geobase
    WHERE StreetName IS NOT NULL AND FacilityTy != 0
    GROUP BY StreetList
), substrings AS (
    SELECT b.id, r1.id AS r1id, r2.id AS r2id, (ST_Dump(ST_Split(r.geom, ST_Union(r1.geom, r2.geom)))).geom AS geom
    FROM boston_sweep_sched b
    JOIN wholeroads r  ON (b.street = r.name OR b.street = r.alt_name)
    JOIN wholeroads r1 ON (b.from_st = r1.name OR b.from_st = r1.alt_name) AND ST_Intersects(r.geom, r1.geom)
    JOIN wholeroads r2 ON (b.to_st = r2.name OR b.to_st = r2.alt_name) AND ST_Intersects(r.geom, r2.geom)
    WHERE (b.from_st != 'DEAD END' AND b.to_st != 'DEAD END')
      AND ST_GeometryType(ST_Intersection(r.geom, r1.geom)) LIKE '%Point%'
      AND ST_GeometryType(ST_Intersection(r.geom, r2.geom)) LIKE '%Point%'
    UNION ALL
    SELECT b.id, r1.id AS r1id, NULL AS r2id, (ST_Dump(ST_Split(r.geom, r1.geom))).geom AS geom
    FROM boston_sweep_sched b
    JOIN wholeroads r  ON (b.street = r.name OR b.street = r.alt_name)
    JOIN wholeroads r1 ON ((b.from_st = r1.name OR b.from_st = r1.alt_name)
                        OR (b.to_st = r1.name OR b.to_st = r1.alt_name)) AND ST_Intersects(r.geom, r1.geom)
    WHERE (b.from_st = 'DEAD END' OR b.to_st = 'DEAD END')
       AND ST_GeometryType(ST_Intersection(r.geom, r1.geom)) LIKE '%Point%'
), linebufs AS (
    SELECT r.id, ST_Buffer(r.geom, 5, 'endcap=flat') AS geom
    FROM substrings r
    JOIN wholeroads r1 ON r.r1id = r1.id
    LEFT JOIN wholeroads r2 ON r.r2id IS NOT NULL AND r.r2id = r2.id
    WHERE ST_GeometryType(r.geom) LIKE '%LineString%'
      AND ST_DWithin(r.geom, r1.geom, 1)
      AND ((r2id IS NOT NULL AND ST_DWithin(r.geom, r2.geom, 1))
       OR (r2id IS NULL AND ((SELECT count(x.*) FROM boston_geobase x WHERE ST_DWithin(ST_StartPoint(r.geom), x.geom, 1)) = 1
         OR (SELECT count(x.*) FROM boston_geobase x WHERE ST_DWithin(ST_EndPoint(r.geom), x.geom, 1)) = 1)))
), lines AS (
    SELECT r.id, s.ogc_fid
    FROM linebufs r
    JOIN boston_geobase s ON ST_Contains(r.geom, s.geom)
), linesides AS (
    SELECT DISTINCT ON (r.RoadSegmen, round(a.street_number_sort::int % 2)) l.id, a.geom, r.RoadSegmen,
        ST_Distance(a.geom, ST_StartPoint(ST_LineMerge(r.geom))) AS distance
    FROM lines l
    JOIN boston_geobase r ON l.ogc_fid = r.ogc_fid
    JOIN boston_address a ON ST_DWithin(r.geom, a.geom, 50)
        AND (upper(a.street_body || ' ' || a.street_full_suffix) = r.StreetName
          OR upper(a.street_body || ' ' || a.street_full_suffix) = r.StreetNa_1)
    JOIN boston_sweep_sched b ON l.id = b.id
    WHERE (round(a.street_number_sort::int % 2) = 0 AND b.side = 'Even') OR (round(a.street_number_sort::int % 2) = 1 AND b.side = 'Odd')
         OR (b.side IS NULL)
    ORDER BY r.RoadSegmen, round(a.street_number_sort::int % 2), ST_Distance(a.geom, ST_LineInterpolatePoint(ST_LineMerge(r.geom), 0.5))
)
INSERT INTO boston_sign (geom, roadsegment, distance, direction, code, description)
SELECT
    l.geom,
    l.RoadSegmen,
    l.distance,
    0,
    r.code,
    r.description
FROM linesides l
JOIN rules r ON r.code = ('BOS-SSWP-' || l.id)
"""


insert_sign_cambridge = """
WITH stnames AS (
    SELECT *,
        (CASE WHEN substring(stnm from '(^[0-9]*)')::int % 2 = 0 THEN 'Even' ELSE 'Odd' END) AS side,
        (CASE
            WHEN substring(stname from 'St$') IS NOT NULL THEN upper(regexp_replace(stname, 'St$', 'Street'))
            WHEN substring(stname from 'Ave$') IS NOT NULL THEN upper(regexp_replace(stname, 'Ave$', 'Avenue'))
            WHEN substring(stname from 'Rd$') IS NOT NULL THEN upper(regexp_replace(stname, 'Rd$', 'Road'))
            WHEN substring(stname from 'Pl$') IS NOT NULL THEN upper(regexp_replace(stname, 'Pl$', 'Place'))
            WHEN substring(stname from 'Dr$') IS NOT NULL THEN upper(regexp_replace(stname, 'Dr$', 'Drive'))
            WHEN substring(stname from 'Ct$') IS NOT NULL THEN upper(regexp_replace(stname, 'Ct$', 'Court'))
            WHEN substring(stname from 'Ter$') IS NOT NULL THEN upper(regexp_replace(stname, 'Ter$', 'Terrace'))
            WHEN substring(stname from 'Pkwy$') IS NOT NULL THEN upper(regexp_replace(stname, 'Pkwy$', 'Parkway'))
            WHEN substring(stname from 'Pk$') IS NOT NULL THEN upper(regexp_replace(stname, 'Pk$', 'Park'))
            WHEN substring(stname from 'Ln$') IS NOT NULL THEN upper(regexp_replace(stname, 'Ln$', 'Lane'))
            WHEN substring(stname from 'Blvd$') IS NOT NULL THEN upper(regexp_replace(stname, 'Blvd$', 'Boulevard'))
            WHEN substring(stname from 'Tpk$') IS NOT NULL THEN upper(regexp_replace(stname, 'Tpk$', 'Turnpike'))
            WHEN substring(stname from 'Cir$') IS NOT NULL THEN upper(regexp_replace(stname, 'Cir$', 'Circle'))
            WHEN substring(stname from 'Hwy$') IS NOT NULL THEN upper(regexp_replace(stname, 'Hwy$', 'Highway'))
            WHEN substring(stname from 'St N$') IS NOT NULL THEN upper(regexp_replace(stname, 'St N$', 'Street North'))
            WHEN substring(stname from 'Sq$') IS NOT NULL THEN upper(regexp_replace(stname, 'Sq$', 'Square'))
            WHEN substring(stname from 'Aly$') IS NOT NULL THEN upper(regexp_replace(stname, 'Aly$', 'Alley'))
            WHEN substring(stname from 'Ext$') IS NOT NULL THEN upper(regexp_replace(stname, 'Ext$', 'Extension'))
            ELSE upper(stname)
         END) AS realName
    FROM cambridge_address
), points AS (
    SELECT DISTINCT ON (r.roadsegmen, a.side) substring(z.district from '^([A-Z])') AS district,
        a.geom, a.side, r.roadsegmen, r.geom AS road_geom
    FROM boston_metro_geobase r
    JOIN stnames a ON r.mgis_town = 'CAMBRIDGE' AND ST_DWithin(r.geom, a.geom, 50) AND r.streetname = a.realName
    JOIN cambridge_sweep_zones z ON ST_Intersects(z.geom, r.geom)
    WHERE z.district IS NOT NULL AND z.type = 'RD-PAVED'
    ORDER BY r.roadsegmen, a.side, ST_Length(ST_Intersection(r.geom, z.geom)) DESC,
        ST_Distance(a.geom, ST_LineInterpolatePoint(ST_LineMerge(r.geom), 0.5))
)
INSERT INTO boston_sign (geom, roadsegment, distance, direction, code, description)
SELECT
    l.geom,
    l.roadsegmen,
    ST_Distance(l.geom, ST_StartPoint(ST_LineMerge(l.road_geom))),
    0,
    r.code,
    r.description
FROM points l
JOIN rules r ON r.code = ('CMB-SSWP-' || l.district || '-' || l.side)
"""


# try to match osm ways with geobase
create_roads_geobase = """
DROP TABLE IF EXISTS boston_roads_geobase;
CREATE TABLE boston_roads_geobase (
    id integer
    , osm_id bigint
    , city varchar
    , name varchar
    , roadsegment integer
    , geom geometry(Linestring, 3857)
);
"""

match_roads_geobase = """
WITH tmp AS (
    SELECT
        o.*
        , m.roadsegmen AS roadsegment
        , rank() OVER (
            PARTITION BY o.id ORDER BY
              ST_HausdorffDistance(o.geom, m.geom)
              , levenshtein(o.name, m.streetname)
              , abs(ST_Length(o.geom) - ST_Length(m.geom)) / greatest(ST_Length(o.geom), ST_Length(m.geom))
          ) AS rank
    FROM roads o
    JOIN {tbl} m ON o.geom && ST_Expand(m.geom, 10)
    WHERE ST_Contains(ST_Buffer(m.geom, 30), o.geom)
        AND o.name != 'Boston Marathon Finish Line'
)
INSERT INTO boston_roads_geobase
SELECT
    DISTINCT ON (id)
    id
    , osm_id
    , 'boston'
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
                , levenshtein(o.name, m.streetname)
                , abs(ST_Length(o.geom) - ST_Length(m.geom)) / greatest(ST_Length(o.geom), ST_Length(m.geom))
            ) AS rank
      FROM roads o
      LEFT JOIN boston_roads_geobase orig ON orig.id = o.id
      JOIN {tbl} m ON o.geom && ST_Expand(m.geom, 10)
      WHERE ST_Contains(ST_Buffer(o.geom, 30), m.geom)
        AND orig.id IS NULL
        AND o.name != 'Boston Marathon Finish Line'
)
INSERT INTO boston_roads_geobase
SELECT
    DISTINCT ON (id)
    id
    , osm_id
    , 'boston'
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
        , ST_LineInterpolatePoint(s.geom, 0.5) AS geom
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

add_signposts_to_sign = """
WITH tmp AS (
    SELECT DISTINCT s.id AS sign_id, p.id AS post_id
    FROM boston_sign s
    JOIN boston_signpost p ON s.id = ANY(p.signs)
)
UPDATE boston_sign s
SET signpost = tmp.post_id
FROM tmp
WHERE s.id = tmp.sign_id;
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
        , ST_LineLocatePoint(rgeom, pgeom) as position
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
    , ST_LineSubstring(w.geom, loc1.position, loc2.position) as geom
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

overlay_paid_rules = """
WITH tmp AS (
    SELECT DISTINCT ON (foo.id)
        b.gid AS id,
        1.0 AS rate,
        (CASE WHEN b.city = 'boston' THEN 'BOS-PAID' WHEN b.city = 'cambridge' THEN 'CMB-PAID'
            ELSE NULL END) AS rule,
        foo.id AS slot_id,
        array_agg(foo.rules) AS orig_rules
    FROM meters_boston b, boston_roads_geobase r,
        (
            SELECT id, rid, geom, jsonb_array_elements(rules) AS rules
            FROM boston_slots_temp
            GROUP BY id
        ) foo
    WHERE r.roadsegment = b.roadsegmen
        AND r.id = foo.rid
        AND ST_DWithin(foo.geom, b.geom, 12)
    GROUP BY b.gid, b.geom, foo.id, foo.geom
    ORDER BY foo.id, ST_Distance(foo.geom, b.geom)
), new_slots AS (
    SELECT t.slot_id, array_to_json(array_cat(t.orig_rules, array_agg(
        distinct json_build_object(
            'code', r.code,
            'description', r.description,
            'periods', r.periods,
            'agenda', r.agenda,
            'time_max_parking', r.time_max_parking,
            'special_days', r.special_days,
            'restrict_types', r.restrict_types,
            'paid_hourly_rate', 1.00
        )::jsonb)
    ))::jsonb AS rules
    FROM tmp t
    JOIN rules r ON r.code = t.rule
    GROUP BY t.slot_id, t.orig_rules
)
UPDATE boston_slots_temp s
SET rules = n.rules
FROM new_slots n
WHERE n.slot_id = s.id
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
