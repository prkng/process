# -*- coding: utf-8 -*-
from __future__ import unicode_literals


# create table hosting all signs
create_sign = """
DROP TABLE IF EXISTS seattle_sign;
CREATE TABLE seattle_sign (
    id serial PRIMARY KEY
    , sid varchar NOT NULL
    , geom geometry(Point, 3857)
    , segkey integer
    , distance integer
    , facing varchar
    , direction smallint -- direction the rule applies
    , code varchar -- code of rule
    , signpost integer
    , description varchar -- description of rule
)
"""


# insert seattle signs
insert_sign = """
INSERT INTO seattle_sign
(
    sid
    , geom
    , segkey
    , distance
    , facing
    , code
    , description
)
SELECT
    DISTINCT ON (p.unitid)
    p.unitid
    , p.geom
    , p.segkey
    , p.distance
    , p.facing
    , r.code
    , r.description
FROM seattle_signs_raw p
JOIN seattle_sign_codes c ON p.unitid = ANY(c.signs) -- only keep those existing in rules
JOIN rules r ON r.code = c.code
ORDER BY p.unitid
"""


insert_virtual_signs = """
WITH tmp AS (
    SELECT
        *,
        (CASE WHEN substring(customtext, '.*THIS SPACE.*') IS NOT NULL THEN 6
         ELSE substring(fieldnotes, '([0-9]+\.*[0-9]*)\\''')::int * 0.3048
        END) AS offset, -- feet to metres
        (CASE WHEN facing = 'N'  THEN radians(0)
              WHEN facing = 'NE' THEN radians(45)
              WHEN facing = 'E'  THEN radians(90)
              WHEN facing = 'SE' THEN radians(135)
              WHEN facing = 'S'  THEN radians(180)
              WHEN facing = 'SW' THEN radians(225)
              WHEN facing = 'W'  THEN radians(270)
              WHEN facing = 'NW' THEN radians(315)
        END) AS azimuth,
        (CASE WHEN facing = 'N'  THEN 'S'
              WHEN facing = 'NE' THEN 'SW'
              WHEN facing = 'E'  THEN 'W'
              WHEN facing = 'SE' THEN 'NW'
              WHEN facing = 'S'  THEN 'N'
              WHEN facing = 'SW' THEN 'NE'
              WHEN facing = 'W'  THEN 'E'
              WHEN facing = 'NW' THEN 'SE'
        END) AS new_facing
    FROM seattle_signs_raw
    WHERE facing IS NOT NULL
        AND (substring(customtext, '.*THIS SPACE.*') IS NOT NULL
            OR substring(fieldnotes, '([0-9]+\.*[0-9]*\\'')') IS NOT NULL)
)
INSERT INTO seattle_sign
(
    sid
    , geom
    , segkey
    , distance
    , facing
    , code
    , description
)
SELECT
    DISTINCT ON (p.unitid)
    p.unitid
    , ST_Transform(ST_Project(ST_Transform(p.geom, 4326)::geography, p.offset, p.azimuth)::geometry, 3857)
    , p.segkey
    , (p.distance + p.offset + 0.98765) -- random distance to be relatively sure of uniqueness
    , p.new_facing
    , r.code
    , r.description
FROM tmp p
JOIN seattle_sign_codes c ON p.unitid = ANY(c.signs)
JOIN rules r ON r.code = c.code
JOIN seattle_geobase g ON p.segkey = g.compkey
"""


# create signpost table
create_signpost = """
DROP TABLE IF EXISTS seattle_signpost;
CREATE TABLE seattle_signpost (
    id serial PRIMARY KEY
    , geobase_id integer
    , signs integer[]
    , geom geometry(Point, 3857)
);
"""


insert_signpost = """
INSERT INTO seattle_signpost (geobase_id, signs, geom)
SELECT
    min(s.segkey),
    array_agg(DISTINCT s.id),
    ST_SetSRID(ST_MakePoint(avg(ST_X(s.geom)), avg(ST_Y(s.geom))), 3857)
FROM seattle_sign s
GROUP BY s.segkey, s.distance
"""


# try to match osm ways with geobase
match_roads_geobase = """
DROP TABLE IF EXISTS seattle_roads_geobase;
CREATE TABLE seattle_roads_geobase (
    id integer
    , osm_id bigint
    , name varchar
    , compkey integer
    , geom geometry(Linestring, 3857)
);

WITH tmp AS (
    SELECT
        o.*
        , m.compkey
        , rank() OVER (
            PARTITION BY o.id ORDER BY
              ST_HausdorffDistance(o.geom, m.geom)
              , levenshtein(o.name, m.ord_stname)
              , abs(ST_Length(o.geom) - ST_Length(m.geom)) / greatest(ST_Length(o.geom), ST_Length(m.geom))
          ) AS rank
    FROM roads o
    JOIN seattle_geobase m ON o.geom && ST_Expand(m.geom, 10)
    WHERE ST_Contains(ST_Buffer(m.geom, 30), o.geom)
)
INSERT INTO seattle_roads_geobase
SELECT
    DISTINCT ON (id)
    id
    , osm_id
    , name
    , compkey
    , geom
FROM tmp
WHERE rank = 1;


-- invert buffer comparison to catch more ways
WITH tmp AS (
      SELECT
          o.*
          , m.compkey
          , rank() OVER (
              PARTITION BY o.id ORDER BY
                ST_HausdorffDistance(o.geom, m.geom)
                , levenshtein(o.name, m.ord_stname)
                , abs(ST_Length(o.geom) - ST_Length(m.geom)) / greatest(ST_Length(o.geom), ST_Length(m.geom))
            ) AS rank
      FROM roads o
      LEFT JOIN seattle_roads_geobase orig ON orig.id = o.id
      JOIN seattle_geobase m ON o.geom && ST_Expand(m.geom, 10)
      WHERE ST_Contains(ST_Buffer(o.geom, 30), m.geom)
        AND orig.id IS NULL
)
INSERT INTO seattle_roads_geobase
SELECT
    DISTINCT ON (id)
    id
    , osm_id
    , name
    , compkey
    , geom
FROM tmp
WHERE rank = 1;
"""


assign_directions = """
UPDATE seattle_sign s
SET direction = (
    CASE WHEN (degrees(ST_Azimuth(ST_EndPoint(g.geom), ST_StartPoint(g.geom))) BETWEEN 45 AND 135) THEN ( -- E to W
       CASE WHEN ST_isLeft(g.geom, s.geom) = 1 THEN (
        CASE WHEN substring(lower(p.customtext), '.*(west |right arrow|rt arrow).*') IS NOT NULL THEN 1
             WHEN substring(lower(p.customtext), '.*(east |left arrow|lt arrow).*')  IS NOT NULL THEN 2
             WHEN s.facing IS NOT NULL AND s.facing = ANY(ARRAY['W','NW','SW']) THEN 1
             WHEN s.facing IS NOT NULL AND s.facing = ANY(ARRAY['E','NE','SE']) THEN 2
             ELSE 0
        END
       )
       ELSE (
        CASE WHEN substring(lower(p.customtext), '.*(west |right arrow|rt arrow).*') IS NOT NULL THEN 2
             WHEN substring(lower(p.customtext), '.*(east |left arrow|lt arrow).*')  IS NOT NULL THEN 1
             WHEN s.facing IS NOT NULL AND s.facing = ANY(ARRAY['W','NW','SW']) THEN 2
             WHEN s.facing IS NOT NULL AND s.facing = ANY(ARRAY['E','NE','SE']) THEN 1
             ELSE 0
        END
       )
       END
      )
      WHEN (degrees(ST_Azimuth(ST_EndPoint(g.geom), ST_StartPoint(g.geom))) BETWEEN 225 AND 315) THEN ( -- W to E
        CASE WHEN ST_isLeft(g.geom, s.geom) = -1 THEN (
         CASE WHEN substring(lower(p.customtext), '.*(west |right arrow|rt arrow).*') IS NOT NULL THEN 2
              WHEN substring(lower(p.customtext), '.*(east |left arrow|lt arrow).*')  IS NOT NULL THEN 1
              WHEN s.facing IS NOT NULL AND s.facing = ANY(ARRAY['W','NW','SW']) THEN 2
              WHEN s.facing IS NOT NULL AND s.facing = ANY(ARRAY['E','NE','SE']) THEN 1
              ELSE 0
         END
        )
        ELSE (
         CASE WHEN substring(lower(p.customtext), '.*(west |right arrow|rt arrow).*') IS NOT NULL THEN 1
              WHEN substring(lower(p.customtext), '.*(east |left arrow|lt arrow).*')  IS NOT NULL THEN 2
              WHEN s.facing IS NOT NULL AND s.facing = ANY(ARRAY['W','NW','SW']) THEN 1
              WHEN s.facing IS NOT NULL AND s.facing = ANY(ARRAY['E','NE','SE']) THEN 2
              ELSE 0
         END
        )
        END
       )
       WHEN (degrees(ST_Azimuth(ST_EndPoint(g.geom), ST_StartPoint(g.geom))) BETWEEN 145 AND 225) THEN ( -- S to N
        CASE WHEN ST_isLeft(g.geom, s.geom) = -1 THEN (
         CASE WHEN substring(lower(p.customtext), '.*(north |right arrow|rt arrow).*') IS NOT NULL THEN 1
              WHEN substring(lower(p.customtext), '.*(south |left arrow|lt arrow).*')  IS NOT NULL THEN 2
              WHEN s.facing IS NOT NULL AND s.facing = ANY(ARRAY['N','NW','NE']) THEN 1
              WHEN s.facing IS NOT NULL AND s.facing = ANY(ARRAY['S','SW','SE']) THEN 2
              ELSE 0
         END
        )
        ELSE (
         CASE WHEN substring(lower(p.customtext), '.*(north |right arrow|rt arrow).*') IS NOT NULL THEN 2
              WHEN substring(lower(p.customtext), '.*(south |left arrow|lt arrow).*')  IS NOT NULL THEN 1
              WHEN s.facing IS NOT NULL AND s.facing = ANY(ARRAY['N','NW','NE']) THEN 2
              WHEN s.facing IS NOT NULL AND s.facing = ANY(ARRAY['S','SW','SE']) THEN 1
              ELSE 0
         END
        )
        END
       )
       ELSE ( -- N to S
        CASE WHEN ST_isLeft(g.geom, s.geom) = -1 THEN (
         CASE WHEN substring(lower(p.customtext), '.*(north |right arrow|rt arrow).*') IS NOT NULL THEN 2
              WHEN substring(lower(p.customtext), '.*(south |left arrow|lt arrow).*')  IS NOT NULL THEN 1
              WHEN s.facing IS NOT NULL AND s.facing = ANY(ARRAY['N','NW','NE']) THEN 2
              WHEN s.facing IS NOT NULL AND s.facing = ANY(ARRAY['S','SW','SE']) THEN 1
              ELSE 0
         END
        )
        ELSE (
         CASE WHEN substring(lower(p.customtext), '.*(north |right arrow|rt arrow).*') IS NOT NULL THEN 1
              WHEN substring(lower(p.customtext), '.*(south |left arrow|lt arrow).*')  IS NOT NULL THEN 2
              WHEN s.facing IS NOT NULL AND s.facing = ANY(ARRAY['N','NW','NE']) THEN 1
              WHEN s.facing IS NOT NULL AND s.facing = ANY(ARRAY['S','SW','SE']) THEN 2
              ELSE 0
         END
        )
        END
       )
       END
      )
FROM seattle_signs_raw p, seattle_signpost x, seattle_roads_geobase g
WHERE s.sid = p.unitid AND s.signpost = x.id AND x.geobase_id = g.compkey
"""


# project signposts on road and
# determine if they were on the left side or right side of the road
project_signposts = """
DROP TABLE IF EXISTS seattle_signpost_onroad;
CREATE TABLE seattle_signpost_onroad AS
    SELECT
        DISTINCT ON (sp.id) sp.id  -- hack to prevent duplicata
        , s.id AS road_id
        , ST_ClosestPoint(s.geom, sp.geom)::geometry(point, 3857) AS geom
        , ST_isLeft(s.geom, sp.geom) AS isleft
    FROM seattle_signpost sp
    JOIN seattle_roads_geobase s ON sp.geobase_id = s.compkey
    ORDER BY sp.id, ST_Distance(s.geom, sp.geom);

SELECT id FROM seattle_signpost_onroad GROUP BY id HAVING count(*) > 1
"""


# how many signposts have been projected ?
count_signpost_projected = """
WITH tmp AS (
    SELECT
        (SELECT count(*) FROM seattle_signpost_onroad) as a
        , (SELECT count(*) FROM seattle_signpost) as b
)
SELECT
    a::float / b * 100, b
FROM tmp
"""

# generate signposts orphans
generate_signposts_orphans = """
DROP TABLE IF EXISTS seattle_signposts_orphans;
CREATE TABLE seattle_signposts_orphans AS
(WITH tmp as (
    SELECT id FROM seattle_signpost
    EXCEPT
    SELECT id FROM seattle_signpost_onroad
) SELECT
    s.*
FROM tmp t
JOIN seattle_signpost s using(id)
)
"""


add_signposts_to_sign = """
WITH tmp AS (
    SELECT DISTINCT s.id AS sign_id, p.id AS post_id
    FROM seattle_sign s
    JOIN seattle_signpost p ON s.id = ANY(p.signs)
)
UPDATE seattle_sign s
SET signpost = tmp.post_id
FROM tmp
WHERE s.id = tmp.sign_id;
"""


# create potential slots determined with signposts projected as start and end points
create_slots_likely = """
DROP TABLE IF EXISTS seattle_slots_likely;
CREATE TABLE seattle_slots_likely(
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
    FROM seattle_roads_geobase r, seattle_signpost_onroad p
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
INSERT INTO seattle_slots_likely (signposts, rid, position, geom)
SELECT
    ARRAY[loc1.signpost, loc2.signpost]
    , w.id
    , loc1.position as position
    , st_line_substring(w.geom, loc1.position, loc2.position) as geom
FROM loc_with_idx loc1
JOIN loc_with_idx loc2 using (rid)
JOIN seattle_roads_geobase w on w.id = loc1.rid
WHERE loc2.idx = loc1.idx+1;
"""


create_nextpoints_for_signposts = """
DROP TABLE IF EXISTS seattle_nextpoints;
CREATE TABLE seattle_nextpoints AS
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
FROM seattle_signpost_onroad spo
JOIN seattle_signpost sp on sp.id = spo.id
JOIN seattle_slots_likely sl on ARRAY[spo.id] <@ sl.signposts
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
    FROM seattle_slots_likely sl
    JOIN seattle_sign s on ARRAY[s.signpost] <@ sl.signposts
    JOIN seattle_signpost_onroad spo on s.signpost = spo.id
    JOIN seattle_nextpoints np on np.slot_id = sl.id AND
                          s.signpost = np.id AND
                          s.direction = np.direction
    JOIN seattle_roads_geobase rb on spo.road_id = rb.id

    UNION ALL
    -- both direction from signpost
    SELECT
        sl.*
        , s.code
        , s.description
        , s.direction
        , spo.isleft
        , rb.name
    FROM seattle_slots_likely sl
    JOIN seattle_sign s on ARRAY[s.signpost] <@ sl.signposts and direction = 0
    JOIN seattle_signpost_onroad spo on s.signpost = spo.id
    JOIN seattle_roads_geobase rb on spo.road_id = rb.id
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
            'address', name,
            'season_start', r.season_start,
            'season_end', r.season_end,
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
) INSERT INTO seattle_slots_temp (rid, position, signposts, rules, geom, way_name)
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
DROP TABLE IF EXISTS seattle_slots_debug;
CREATE TABLE seattle_slots_debug as
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
    FROM seattle_slots_likely sl
    JOIN seattle_sign s on ARRAY[s.signpost] <@ sl.signposts
    JOIN seattle_signpost_onroad spo on s.signpost = spo.id
    JOIN seattle_nextpoints np on np.slot_id = sl.id AND
                          s.signpost = np.id AND
                          s.direction = np.direction
    JOIN seattle_roads_geobase rb on spo.road_id = rb.id

    UNION ALL
    -- both direction from signpost
    SELECT
        sl.*
        , s.code
        , s.description
        , s.direction
        , spo.isleft
        , rb.name
    FROM seattle_slots_likely sl
    JOIN seattle_sign s on ARRAY[s.signpost] <@ sl.signposts and direction = 0
    JOIN seattle_signpost_onroad spo on s.signpost = spo.id
    JOIN seattle_roads_geobase rb on spo.road_id = rb.id
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
    , rt.season_start
    , rt.season_end
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
JOIN seattle_rules_translation rt on rt.code = r.code
WHERE ST_GeometryType(ST_OffsetCurve(t.geom, ({offset} * t.isleft), 'quad_segs=4 join=round')::geometry) = 'ST_LineString'
)
"""
