# -*- coding: utf-8 -*-
from __future__ import unicode_literals


# create table hosting all signs
create_sign = """
DROP TABLE IF EXISTS seattle_sign;
CREATE TABLE seattle_sign (
    id serial PRIMARY KEY
    , sid integer NOT NULL
    , geom geometry(Point, 3857)
    , direction smallint -- direction the rule applies (0: both side, 1: left, 2: right)
    , signpost integer NOT NULL
    , elevation smallint -- higher is prioritary
    , code varchar -- code of rule
    , description varchar -- description of rule
)
"""


# insert seattle signs with associated postsigns
insert_sign = """
INSERT INTO montreal_sign
(
    sid
    , geom
    , direction
    , signpost
    , elevation
    , code
    , description
)
SELECT
    p.panneau_id_pan
    , pt.geom
    , case p.fleche_pan
        when 2 then 1 -- Left
        when 3 then 2 -- Right
        when 0 then 0 -- both sides
        when 8 then 0 -- both sides
        else NULL
      end as direction
    , pt.poteau_id_pot
    , p.position_pop
    , p.code_rpa
    , p.description_rpa
FROM montreal_descr_panneau p
JOIN montreal_poteaux pt on pt.poteau_id_pot = p.poteau_id_pot
JOIN rules r on r.code = p.code_rpa -- only keep those existing in rules
WHERE
    pt.description_rep = 'Réel'
    AND p.description_rpa not ilike '%panonceau%'
    AND p.code_rpa !~ '^R[BCGHK].*' -- don't match rules starting with 'R*'
    AND p.code_rpa <> 'RD-TT' -- don't match 'debarcaderes'
    AND substring(p.description_rpa, '.*\((flexible)\).*') is NULL
    AND p.fleche_pan in (0, 2, 3, 8)
"""
