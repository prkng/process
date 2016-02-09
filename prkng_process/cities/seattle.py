# -*- coding: utf-8 -*-
from __future__ import unicode_literals


# create table hosting all signs
create_sign = """
DROP TABLE IF EXISTS seattle_sign;
CREATE TABLE seattle_sign (
    id serial PRIMARY KEY
    , sid varchar
    , geom geometry(Point, 3857)
    , segkey integer
    , distance float
    , facing varchar
    , direction smallint -- direction the rule applies
    , code varchar -- code of rule
    , signpost integer
    , description varchar -- description of rule
)
"""

# insert seattle signs based on curblines dataset
#  - creates signs that point to each other on each end of the curbline
#     (gets proper facing by analyzing direction of the azimuth of the curbline direction)
#  - ranks blockface line to associate with curbline via "closest within one metre"
#  - ranks nearest real sign to curbline with the same type to get proper rules for that section
#  - for 'unrestricted' curblines on 'unrestricted' blockfaces, insert a dummy rule so a slot
#     can still be drawn
insert_sign = """
WITH tmp AS (
    SELECT c.objectid, p.unitid, l.elmntkey, l.segkey, ST_LineMerge(c.geom) AS curb_geom,
        ROW_NUMBER() OVER (PARTITION BY c.objectid ORDER BY ST_Distance(p.geom, ST_StartPoint(ST_LineMerge(c.geom)))) AS rank_sign,
        RANK() OVER (PARTITION BY c.objectid ORDER BY ST_Distance(l.geom, c.geom)) AS rank_parkline,
        r.code, r.description
    FROM seattle_curblines c
    JOIN seattle_parklines l ON c.side = l.side AND ST_DWithin(c.geom, l.geom, 1)
    JOIN seattle_signs_raw p ON l.segkey = p.segkey AND (
        (c.spacetype = ANY(ARRAY['XW', 'XWAREA', 'NP', 'TAZ']) AND p.category = ANY(ARRAY['PNP', 'PNS'])) OR
        (c.spacetype = ANY(ARRAY['BUS', 'BUSLAY']) AND p.category = ANY(ARRAY['PBZ', 'PBLO'])) OR
        (c.spacetype = ANY(ARRAY['TL', 'TL-PKA', 'TL-PKP']) AND p.category = 'PTIML') OR
        (left(c.spacetype, 2) = 'CV' AND p.category = 'PCVL') OR
        (c.spacetype LIKE '%PLZ%' AND p.category = ANY(ARRAY['PPL', 'PLU'])) OR
        (c.spacetype = ANY(ARRAY['L/UL', 'TL-LUL']) AND p.category = ANY(ARRAY['PPL', 'PLU'])) OR
        (c.spacetype LIKE '%RPZ%' AND p.category = 'PRZ') OR
        (c.spacetype = 'UNR' AND p.category = ANY(ARRAY['PRZ', 'PTIML'])) OR
        (c.spacetype = ANY(ARRAY['PS-TAX', 'CV-TAX', 'TAXI', 'CHRTR', 'CZ']) AND p.category = 'PZONE') OR
        (c.spacetype = 'DISABL' AND p.category = 'PDIS') OR
        (c.spacetype = ANY(ARRAY['SFD', 'LEVO']) AND p.category = 'PGA')
    )
    JOIN seattle_sign_codes x ON p.unitid = ANY(x.signs) -- only keep those existing in rules
    JOIN rules r ON r.code = x.code
    WHERE c.current_status = 'INSVC'
), tall AS (
    (SELECT c.objectid, c.objectid::text AS unitid, l.elmntkey, l.segkey, ST_LineMerge(c.geom) AS curb_geom,
        1 AS rank_sign, RANK() OVER (PARTITION BY c.objectid ORDER BY ST_Distance(l.geom, c.geom)) AS rank_parkline,
        r.code, r.description
    FROM seattle_curblines c
    JOIN seattle_parklines l ON c.side = l.side AND ST_DWithin(c.geom, l.geom, 1)
    JOIN rules r ON r.code = 'SEA-PRM'
    LEFT JOIN tmp x ON x.objectid = c.objectid
    WHERE x.objectid IS NULL AND c.spacetype = 'UNR' AND c.current_status = 'INSVC'
        AND l.parking_category = 'Unrestricted Parking')
    UNION ALL
    (SELECT * FROM tmp)
)
INSERT INTO seattle_sign
(
    sid
    , geom
    , segkey
    , facing
    , distance
    , code
    , description
)
(SELECT
    DISTINCT ON (t.objectid)
    t.unitid
    , ST_StartPoint(t.curb_geom)
    , t.segkey
    , (CASE
        WHEN degrees(ST_Azimuth(ST_StartPoint(t.curb_geom), ST_EndPoint(t.curb_geom))) BETWEEN 45  AND 135 THEN 'E'
        WHEN degrees(ST_Azimuth(ST_StartPoint(t.curb_geom), ST_EndPoint(t.curb_geom))) BETWEEN 225 AND 315 THEN 'W'
        WHEN degrees(ST_Azimuth(ST_StartPoint(t.curb_geom), ST_EndPoint(t.curb_geom))) BETWEEN 145 AND 225 THEN 'S'
        ELSE 'N'
      END)
    , round(ST_Distance(ST_StartPoint(g.geom), ST_StartPoint(t.curb_geom)))
    , t.code
    , t.description
FROM tall t
JOIN seattle_roads_geobase g ON t.segkey = g.compkey
WHERE t.rank_sign = 1 AND t.rank_parkline = 1
ORDER BY t.objectid)
UNION ALL
(SELECT
    DISTINCT ON (t.objectid)
    t.unitid
    , ST_EndPoint(t.curb_geom)
    , t.segkey
    , (CASE
        WHEN degrees(ST_Azimuth(ST_EndPoint(t.curb_geom), ST_StartPoint(t.curb_geom))) BETWEEN 45  AND 135 THEN 'E'
        WHEN degrees(ST_Azimuth(ST_EndPoint(t.curb_geom), ST_StartPoint(t.curb_geom))) BETWEEN 225 AND 315 THEN 'W'
        WHEN degrees(ST_Azimuth(ST_EndPoint(t.curb_geom), ST_StartPoint(t.curb_geom))) BETWEEN 145 AND 225 THEN 'S'
        ELSE 'N'
      END)
    , round(ST_Distance(ST_StartPoint(g.geom), ST_EndPoint(t.curb_geom)))
    , t.code
    , t.description
FROM tall t
JOIN seattle_roads_geobase g ON t.segkey = g.compkey
WHERE t.rank_sign = 1 AND t.rank_parkline = 1
ORDER BY t.objectid)
"""


# insert seattle signs based on curblines dataset (paid)
#  - creates signs that point to each other on each end of the curbline, like above
#  - ranks blockface line to associate with curbline via "closest within one metre"
#  - insert and join the dynamic paid parking rule created and registered during download process
insert_sign_paid = """
WITH tmp AS (
    SELECT c.objectid, l.elmntkey, l.segkey, ST_LineMerge(c.geom) AS curb_geom,
        degrees(ST_Azimuth(ST_StartPoint(ST_LineMerge(c.geom)), ST_EndPoint(ST_LineMerge(c.geom)))) AS azimuth1,
        degrees(ST_Azimuth(ST_EndPoint(ST_LineMerge(c.geom)), ST_StartPoint(ST_LineMerge(c.geom)))) AS azimuth2,
        ROW_NUMBER() OVER (PARTITION BY c.objectid ORDER BY ST_Distance(l.geom, c.geom)) AS rank
    FROM seattle_curblines c
    JOIN seattle_parklines l ON c.side = l.side AND ST_DWithin(c.geom, l.geom, 1)
    WHERE c.current_status = 'INSVC' AND c.spacetype = ANY(ARRAY['PS', 'PS-RPZ', 'PS-SCH'])
)
INSERT INTO seattle_sign
(
    sid
    , geom
    , segkey
    , facing
    , distance
    , code
    , description
)
(SELECT
    DISTINCT ON (t.objectid, r.code)
    t.objectid
    , ST_StartPoint(t.curb_geom)
    , t.segkey
    , (CASE
        WHEN t.azimuth1 BETWEEN 45  AND 135 THEN 'E'
        WHEN t.azimuth1 BETWEEN 225 AND 315 THEN 'W'
        WHEN t.azimuth1 BETWEEN 145 AND 225 THEN 'S'
        ELSE 'N'
      END)
    , round(ST_Distance(ST_StartPoint(g.geom), ST_StartPoint(t.curb_geom)))
    , r.code
    , r.description
FROM tmp t
JOIN seattle_roads_geobase g ON t.segkey = g.compkey
JOIN seattle_sign_codes c ON t.elmntkey::varchar = ANY(c.signs) -- only keep those existing in rules
JOIN rules r ON r.code = c.code
WHERE t.rank = 1
ORDER BY t.objectid)
UNION ALL
(SELECT
    DISTINCT ON (t.objectid, r.code)
    t.objectid
    , ST_EndPoint(t.curb_geom)
    , t.segkey
    , (CASE
        WHEN t.azimuth2 BETWEEN 45  AND 135 THEN 'E'
        WHEN t.azimuth2 BETWEEN 225 AND 315 THEN 'W'
        WHEN t.azimuth2 BETWEEN 145 AND 225 THEN 'S'
        ELSE 'N'
      END)
    , round(ST_Distance(ST_StartPoint(g.geom), ST_EndPoint(t.curb_geom)))
    , r.code
    , r.description
FROM tmp t
JOIN seattle_roads_geobase g ON t.segkey = g.compkey
JOIN seattle_sign_codes c ON t.elmntkey::varchar = ANY(c.signs) -- only keep those existing in rules
JOIN rules r ON r.code = c.code
WHERE t.rank = 1
ORDER BY t.objectid)
"""


# insert seattle signs for directional restrictions (in absence of curblines)
#  - for no parking, no stopping and time-max parking sections on roads that don't have curbline
#     data available from the city
#  - grabs signs with directional data on the sign OR on the same post when the notes reference it (as `s2`)
#  - puts directional data in, either as facing OR as a direction if the sign has an arrow (easy)
#  - only do this on streets that don't already have curbline-based signs added, of course
insert_sign_directional = """
WITH tmp AS (
    SELECT DISTINCT s1.unitid, round(ST_Distance(s1.geom, ST_StartPoint(g.geom))) AS distance,
        (CASE WHEN s2.unitid IS NOT NULL THEN s2.customtext
              WHEN s1.customtext LIKE '% OF HERE%' THEN substring(s1.customtext from '(\S+ OF HERE)')
              ELSE NULL
         END) AS cardinal,
        (CASE WHEN (s2.unitid IS NULL AND s1.customtext LIKE '% ARROW]%'
                AND left(substring(s1.customtext from '\[(\S+) ARROW\]'), 1) = 'L') THEN 1
              WHEN (s2.unitid IS NULL AND s1.customtext LIKE '% ARROW]%'
                AND left(substring(s1.customtext from '\[(\S+) ARROW\]'), 1) = 'R') THEN 2
              ELSE NULL
         END) AS direction
    FROM seattle_signs_raw s1
    JOIN seattle_roads_geobase g ON s1.segkey = g.compkey
    LEFT JOIN seattle_signs_raw s2 ON s1.segkey = s2.segkey AND s1.distance = s2.distance
        AND ST_isLeft(g.geom, s1.geom) = ST_isLeft(g.geom, s2.geom)
    LEFT JOIN seattle_sign x ON s1.segkey = x.segkey
    WHERE x.id IS NULL AND s1.category = ANY(ARRAY['PNP', 'PNS', 'PTIML']) AND
        ((s2.unitid IS NULL AND s1.customtext LIKE '% OF HERE%') OR
         (s2.unitid IS NULL AND s1.customtext LIKE '% ARROW%') OR
         (s2.unitid IS NOT NULL AND right(s1.fieldnotes, 1) = left(s2.customtext, 1)
            AND s2.category = 'PINST'))
)
INSERT INTO seattle_sign (geom, segkey, facing, direction, code, description)
SELECT
    p.geom, p.segkey, left(t.cardinal, 1), t.direction, r.code, r.description
FROM tmp t
JOIN seattle_signs_raw p ON t.unitid = p.unitid
JOIN seattle_sign_codes c ON p.unitid = ANY(c.signs) -- only keep those existing in rules
JOIN rules r ON r.code = c.code
"""


# insert signs based on parklines dataset (in absence of curblines)
#  - for roads with no curbline data available from the city
#  - grabs the centrepoint of the blockface line, creates a bidirectional sign there matching:
#     A) the nearest sign on that street with a similar rule, or
#     B) in the case of 'unrestricted parking', adds a dummy rule to create empty slot
#  - ranks blockface line to associate with curbline via "closest within one metre"
#  - assumes same rule for entire street, less directional "no parking" signs added earlier
insert_sign_parklines = """
WITH parklines AS (
    SELECT DISTINCT p.elmntkey, p.segkey, p.side, p.parking_category,
        ST_Line_Interpolate_Point(ST_LineMerge(p.geom), 0.5) AS centerpoint
    FROM seattle_parklines p
    JOIN seattle_roads_geobase g ON p.segkey = g.compkey
    LEFT JOIN seattle_sign s ON s.segkey = p.segkey
        AND ST_isLeft(g.geom, s.geom) = ST_isLeft(g.geom, ST_Line_Interpolate_Point(ST_LineMerge(p.geom), 0.5))
    LEFT JOIN seattle_signs_raw sr ON s.sid = sr.unitid
    WHERE (s.id IS NULL OR (sr.unitid IS NOT NULL AND sr.category != ANY(ARRAY['PNP', 'PNS', 'PTIML'])))
        AND p.parking_category = ANY(ARRAY['Time Limited Parking', 'Restricted Parking Zone',
            'No Parking Allowed', 'Unrestricted Parking'])
        AND ST_GeometryType(ST_LineMerge(p.geom)) = 'ST_LineString'
), tmp AS (
    (SELECT p.elmntkey, p.segkey, p.centerpoint, r.code, r.description,
        ROW_NUMBER() OVER (PARTITION BY (p.elmntkey, left(s.signtype, 2)) ORDER BY ST_Distance(p.centerpoint, s.geom)) AS rank_sign
    FROM parklines p
    JOIN seattle_roads_geobase g ON p.segkey = g.compkey
    JOIN seattle_signs_raw s ON p.segkey = s.segkey AND ST_isLeft(g.geom, p.centerpoint) = ST_isLeft(g.geom, s.geom) AND (
        (p.parking_category = 'Time Limited Parking' AND s.category = 'PTIML') OR
        (p.parking_category = 'Restricted Parking Zone' AND s.category = 'PRZ') OR
        (p.parking_category = 'No Parking Allowed' AND s.category = 'PNP')
    )
    JOIN seattle_sign_codes c ON s.unitid = ANY(c.signs) -- only keep those existing in rules
    JOIN rules r ON r.code = c.code)
    UNION ALL
    (SELECT p.elmntkey, p.segkey, p.centerpoint, r.code, r.description, 1 AS rank_sign
     FROM parklines p
     JOIN rules r ON r.code = 'SEA-PRM'
     WHERE p.parking_category = 'Unrestricted Parking')
)
INSERT INTO seattle_sign (geom, segkey, code, description)
SELECT
    t.centerpoint, t.segkey, t.code, t.description
FROM tmp t
WHERE t.rank_sign = 1
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
JOIN seattle_roads_geobase g ON s.segkey = g.compkey
GROUP BY s.segkey, s.distance, ST_isLeft(g.geom, s.geom)
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
        CASE WHEN s.facing IS NOT NULL AND s.facing = ANY(ARRAY['W','NW','SW']) THEN 2
             WHEN s.facing IS NOT NULL AND s.facing = ANY(ARRAY['E','NE','SE']) THEN 1
             ELSE 0
        END
       )
       ELSE (
        CASE WHEN s.facing IS NOT NULL AND s.facing = ANY(ARRAY['W','NW','SW']) THEN 1
             WHEN s.facing IS NOT NULL AND s.facing = ANY(ARRAY['E','NE','SE']) THEN 2
             ELSE 0
        END
       )
       END
      )
      WHEN (degrees(ST_Azimuth(ST_EndPoint(g.geom), ST_StartPoint(g.geom))) BETWEEN 225 AND 315) THEN ( -- W to E
        CASE WHEN ST_isLeft(g.geom, s.geom) = -1 THEN (
         CASE WHEN s.facing IS NOT NULL AND s.facing = ANY(ARRAY['W','NW','SW']) THEN 2
              WHEN s.facing IS NOT NULL AND s.facing = ANY(ARRAY['E','NE','SE']) THEN 1
              ELSE 0
         END
        )
        ELSE (
         CASE WHEN s.facing IS NOT NULL AND s.facing = ANY(ARRAY['W','NW','SW']) THEN 1
              WHEN s.facing IS NOT NULL AND s.facing = ANY(ARRAY['E','NE','SE']) THEN 2
              ELSE 0
         END
        )
        END
       )
       WHEN (degrees(ST_Azimuth(ST_EndPoint(g.geom), ST_StartPoint(g.geom))) BETWEEN 145 AND 225) THEN ( -- S to N
        CASE WHEN ST_isLeft(g.geom, s.geom) = -1 THEN (
         CASE WHEN s.facing IS NOT NULL AND s.facing = ANY(ARRAY['N','NW','NE']) THEN 1
              WHEN s.facing IS NOT NULL AND s.facing = ANY(ARRAY['S','SW','SE']) THEN 2
              ELSE 0
         END
        )
        ELSE (
         CASE WHEN s.facing IS NOT NULL AND s.facing = ANY(ARRAY['N','NW','NE']) THEN 2
              WHEN s.facing IS NOT NULL AND s.facing = ANY(ARRAY['S','SW','SE']) THEN 1
              ELSE 0
         END
        )
        END
       )
       ELSE ( -- N to S
        CASE WHEN ST_isLeft(g.geom, s.geom) = -1 THEN (
         CASE WHEN s.facing IS NOT NULL AND s.facing = ANY(ARRAY['N','NW','NE']) THEN 2
              WHEN s.facing IS NOT NULL AND s.facing = ANY(ARRAY['S','SW','SE']) THEN 1
              ELSE 0
         END
        )
        ELSE (
         CASE WHEN s.facing IS NOT NULL AND s.facing = ANY(ARRAY['N','NW','NE']) THEN 1
              WHEN s.facing IS NOT NULL AND s.facing = ANY(ARRAY['S','SW','SE']) THEN 2
              ELSE 0
         END
        )
        END
       )
       END
      )
FROM seattle_signpost x, seattle_roads_geobase g
WHERE s.signpost = x.id AND x.geobase_id = g.compkey AND s.direction IS NULL
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
