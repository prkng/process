# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import csv
import json
import os

from . import CONFIG, common, osm, plfunctions
from .cities import montreal as mrl
from .cities import quebec as qbc
from .cities import newyork as nyc
from .cities import seattle as sea
from .cities import boston as bos
from .database import PostgresWrapper
from .filters import group_rules
from .logger import Logger
from .utils import pretty_time, tstr_to_float


# distance from road to slot
LINE_OFFSET = 6
CITIES = ["montreal", "quebec", "newyork", "seattle", "boston"]
db = PostgresWrapper(
    "host='{PG_HOST}' port={PG_PORT} dbname={PG_DATABASE} "
    "user={PG_USERNAME} password={PG_PASSWORD} ".format(**CONFIG))


def process_quebec(debug=False):
    """
    Process Quebec data
    """
    def info(msg):
        return Logger.info("Québec: {}".format(msg))

    def debug(msg):
        return Logger.debug("Québec: {}".format(msg))

    def warning(msg):
        return Logger.warning("Québec: {}".format(msg))

    info('Loading and translating rules')
    insert_rules('quebec_rules_translation')
    db.vacuum_analyze('public', 'rules')

    info("Creating sign table")
    db.query(qbc.create_sign)

    info("Loading signs")
    db.query(qbc.insert_sign)
    db.create_index('quebec_sign', 'direction')
    db.create_index('quebec_sign', 'code')
    db.create_index('quebec_sign', 'geom', index_type='gist')
    db.vacuum_analyze('public', 'quebec_sign')

    info("Creating signposts")
    db.query(qbc.create_signpost)
    db.create_index('quebec_signpost', 'id')
    db.create_index('quebec_signpost', 'rid')
    db.create_index('quebec_signpost', 'signs', index_type='gin')
    db.create_index('quebec_signpost', 'geom', index_type='gist')
    db.vacuum_analyze('public', 'quebec_signpost')

    info("Add signpost id to signs")
    db.query(qbc.add_signposts_to_sign)
    db.vacuum_analyze('public', 'quebec_sign')

    info("Projection signposts on road")
    duplicates = db.query(qbc.project_signposts)
    if duplicates:
        warning("Duplicates found for projected signposts : {}"
                .format(str(duplicates)))

    percent, total = db.query(qbc.count_signpost_projected)[0]

    if percent < 100:
        warning("Only {:.0f}% of signposts have been bound to a road. Total is {}"
                .format(percent, total))
        db.query(qbc.generate_signposts_orphans)
        info("Table 'signpost_orphans' has been generated to check for orphans")

    info("Creating slots between signposts")
    db.query(qbc.create_slots_likely)
    db.query(qbc.insert_slots_likely.format(isleft=1))
    db.query(qbc.insert_slots_likely.format(isleft=-1))
    db.create_index('quebec_slots_likely', 'id')
    db.create_index('quebec_slots_likely', 'signposts', index_type='gin')
    db.create_index('quebec_slots_likely', 'geom', index_type='gist')
    db.vacuum_analyze('public', 'quebec_slots_likely')

    db.query(qbc.create_nextpoints_for_signposts)
    db.create_index('quebec_nextpoints', 'id')
    db.create_index('quebec_nextpoints', 'slot_id')
    db.create_index('quebec_nextpoints', 'direction')
    db.vacuum_analyze('public', 'quebec_nextpoints')

    db.query(qbc.insert_slots_temp.format(offset=LINE_OFFSET))
    db.create_index('quebec_slots_temp', 'id')
    db.create_index('quebec_slots_temp', 'geom', index_type='gist')
    db.create_index('quebec_slots_temp', 'rules', index_type='gin')
    db.vacuum_analyze('public', 'quebec_slots_temp')

    info("Creating and overlaying paid slots")
    db.query(qbc.create_bornes_raw)
    db.query(qbc.create_paid_signpost)
    db.query(qbc.aggregate_paid_signposts.format(offset=LINE_OFFSET))
    db.query(qbc.overlay_paid_rules)
    db.query(qbc.create_paid_slots_standalone)

    if debug:
        info("Creating debug slots")
        db.query(qbc.create_slots_for_debug.format(offset=LINE_OFFSET))
        db.create_index('quebec_slots_debug', 'pkid')
        db.create_index('quebec_slots_debug', 'geom', index_type='gist')
        db.vacuum_analyze('public', 'quebec_slots_debug')


def process_montreal(debug=False):
    """
    process montreal data and generate parking slots
    """
    def info(msg):
        return Logger.info("Montréal: {}".format(msg))

    def debug(msg):
        return Logger.debug("Montréal: {}".format(msg))

    def warning(msg):
        return Logger.warning("Montréal: {}".format(msg))

    debug('Loading and translating rules')
    insert_rules('montreal_rules_translation')
    db.vacuum_analyze('public', 'rules')

    info("Matching osm roads with geobase")
    db.query(mrl.match_roads_geobase)
    db.create_index('montreal_roads_geobase', 'id')
    db.create_index('montreal_roads_geobase', 'id_trc')
    db.create_index('montreal_roads_geobase', 'osm_id')
    db.create_index('montreal_roads_geobase', 'name')
    db.create_index('montreal_roads_geobase', 'geom', index_type='gist')
    db.vacuum_analyze('public', 'montreal_roads_geobase')

    info("Creating sign table")
    db.query(mrl.create_sign)

    info("Loading signs")
    db.query(mrl.insert_sign)
    db.query(mrl.insert_signpost_verdun)
    db.query(mrl.insert_sign_verdun)
    db.create_index('montreal_sign', 'geom', index_type='gist')
    db.create_index('montreal_sign', 'direction')
    db.create_index('montreal_sign', 'elevation')
    db.create_index('montreal_sign', 'signpost')
    db.vacuum_analyze('public', 'montreal_sign')

    info("Creating sign posts")
    db.query(mrl.create_signpost)
    db.query(mrl.insert_signpost)
    db.create_index('montreal_signpost', 'geom', index_type='gist')
    db.create_index('montreal_signpost', 'geobase_id')
    db.vacuum_analyze('public', 'montreal_signpost')

    info("Projecting signposts on road")
    duplicates = db.query(mrl.project_signposts)
    if duplicates:
        warning("Duplicates found for projected signposts : {}"
                .format(str(duplicates)))

    db.create_index('montreal_signpost_onroad', 'id')
    db.create_index('montreal_signpost_onroad', 'road_id')
    db.create_index('montreal_signpost_onroad', 'isleft')
    db.create_index('montreal_signpost_onroad', 'geom', index_type='gist')
    db.vacuum_analyze('public', 'montreal_signpost_onroad')

    percent, total = db.query(mrl.count_signpost_projected)[0]

    if percent < 100:
        warning("Only {:.0f}% of signposts have been bound to a road. Total is {}"
                .format(percent, total))
        db.query(mrl.generate_signposts_orphans)
        info("Table 'montreal_signpost_orphans' has been generated to check for orphans")

    info("Creating slots between signposts")
    db.query(mrl.create_slots_likely)
    db.query(mrl.insert_slots_likely.format(isleft=1))
    db.query(mrl.insert_slots_likely.format(isleft=-1))
    db.create_index('montreal_slots_likely', 'id')
    db.create_index('montreal_slots_likely', 'signposts', index_type='gin')
    db.create_index('montreal_slots_likely', 'geom', index_type='gist')
    db.vacuum_analyze('public', 'montreal_slots_likely')

    db.query(mrl.create_nextpoints_for_signposts)
    db.create_index('montreal_nextpoints', 'id')
    db.create_index('montreal_nextpoints', 'slot_id')
    db.create_index('montreal_nextpoints', 'direction')
    db.vacuum_analyze('public', 'montreal_nextpoints')

    db.create_index('montreal_slots_temp', 'id')
    db.create_index('montreal_slots_temp', 'geom', index_type='gist')
    db.create_index('montreal_slots_temp', 'rules', index_type='gin')
    db.query(mrl.insert_slots_temp.format(offset=LINE_OFFSET))

    info("Creating and overlaying paid slots")
    db.query(mrl.overlay_paid_rules)
    db.vacuum_analyze('public', 'montreal_slots_temp')

    if debug:
        info("Creating debug slots")
        db.query(mrl.create_slots_for_debug.format(offset=LINE_OFFSET))
        db.create_index('montreal_slots_debug', 'pkid')
        db.create_index('montreal_slots_debug', 'geom', index_type='gist')
        db.vacuum_analyze('public', 'montreal_slots_debug')


def process_newyork(debug=False):
    """
    Process New York data
    """
    def info(msg):
        return Logger.info("New York: {}".format(msg))

    def debug(msg):
        return Logger.debug("New York: {}".format(msg))

    def warning(msg):
        return Logger.warning("New York: {}".format(msg))

    info('Loading and translating rules')
    insert_rules('newyork_rules_translation')
    db.vacuum_analyze('public', 'rules')

    info("Loading signs")
    db.query(nyc.create_sign)
    db.query(nyc.insert_sign)
    db.create_index('newyork_sign', 'direction')
    db.create_index('newyork_sign', 'code')
    db.create_index('newyork_sign', 'geom', index_type='gist')
    db.vacuum_analyze('public', 'newyork_sign')

    info("Creating signposts")
    db.query(nyc.create_signpost)
    db.query(nyc.insert_signpost)
    db.create_index('newyork_signpost', 'id')
    db.create_index('newyork_signpost', 'geobase_id')
    db.create_index('newyork_signpost', 'signs', index_type='gin')
    db.create_index('newyork_signpost', 'geom', index_type='gist')
    db.vacuum_analyze('public', 'newyork_signpost')

    info("Matching osm roads with geobase")
    db.query(nyc.match_roads_geobase)
    db.create_index('newyork_roads_geobase', 'id')
    db.create_index('newyork_roads_geobase', 'osm_id')
    db.create_index('newyork_roads_geobase', 'name')
    db.create_index('newyork_roads_geobase', 'boro')
    db.create_index('newyork_roads_geobase', 'geom', index_type='gist')
    db.vacuum_analyze('public', 'newyork_roads_geobase')

    info("Match signposts to geobase")
    db.query(nyc.match_signposts)
    db.vacuum_analyze('public', 'newyork_signpost')

    info("Add signpost id to signs")
    db.query(nyc.add_signposts_to_sign)
    db.vacuum_analyze('public', 'newyork_sign')

    info("Projecting signposts on road")
    duplicates = db.query(nyc.project_signposts)
    if duplicates:
        warning("Duplicates found for projected signposts : {}"
                .format(str(duplicates)))

    percent, total = db.query(nyc.count_signpost_projected)[0]

    if percent < 100:
        warning("Only {:.0f}% of signposts have been bound to a road. Total is {}"
                .format(percent, total))
        db.query(nyc.generate_signposts_orphans)
        info("Table 'newyork_signpost_orphans' has been generated to check for orphans")

    info("Creating likely slots")
    db.query(nyc.create_slots_likely)
    db.query(nyc.insert_slots_likely.format(isleft=1))
    db.query(nyc.insert_slots_likely.format(isleft=-1))

    # Get rid of problem segments FIXME
    db.query("""
        with tmp as (
            select *
            from (
                select g.id, count(distinct s.order_no)
                from newyork_roads_geobase g
                join newyork_signpost s on s.geobase_id = g.id
                group by g.id
            ) foo where count > 2
        )
        delete from newyork_slots_likely s using tmp t where t.id = s.rid;
    """)

    db.create_index('newyork_slots_likely', 'id')
    db.create_index('newyork_slots_likely', 'signposts', index_type='gin')
    db.create_index('newyork_slots_likely', 'geom', index_type='gist')
    db.vacuum_analyze('public', 'newyork_slots_likely')

    info("Creating nextpoints")
    db.query(nyc.create_nextpoints_for_signposts)
    db.create_index('newyork_nextpoints', 'id')
    db.create_index('newyork_nextpoints', 'slot_id')
    db.create_index('newyork_nextpoints', 'direction')
    db.vacuum_analyze('public', 'newyork_nextpoints')

    for x in ['K', 'M', 'Q', 'B', 'S']:
        info("Creating slots between signposts (borough {})".format(x))
        db.query(nyc.insert_slots_temp.format(boro=x, offset=LINE_OFFSET))
        db.create_index('newyork_slots_temp', 'id')
        db.create_index('newyork_slots_temp', 'geom', index_type='gist')
        db.create_index('newyork_slots_temp', 'rules', index_type='gin')
        db.vacuum_analyze('public', 'newyork_slots_temp')

    if debug:
        info("Creating debug slots")
        for x in ['K', 'M', 'Q', 'B', 'S']:
            db.query(nyc.create_slots_for_debug.format(boro=x, offset=LINE_OFFSET))
            db.create_index('newyork_slots_debug', 'pkid')
            db.create_index('newyork_slots_debug', 'geom', index_type='gist')
            db.vacuum_analyze('public', 'newyork_slots_debug')


def process_seattle(debug=False):
    """
    Process Seattle data
    """
    def info(msg):
        return Logger.info("Seattle: {}".format(msg))

    def debug(msg):
        return Logger.debug("Seattle: {}".format(msg))

    def warning(msg):
        return Logger.warning("Seattle: {}".format(msg))

    info('Loading and translating rules')
    insert_rules('seattle_rules_translation')
    insert_dynamic_rules_seattle()
    db.vacuum_analyze('public', 'rules')

    info("Matching OSM roads with geobase")
    db.query(sea.match_roads_geobase)
    db.create_index('seattle_roads_geobase', 'id')
    db.create_index('seattle_roads_geobase', 'osm_id')
    db.create_index('seattle_roads_geobase', 'name')
    db.create_index('seattle_roads_geobase', 'geom', index_type='gist')
    db.vacuum_analyze('public', 'seattle_roads_geobase')

    info("Loading signs")
    db.query(sea.create_sign)
    db.query(sea.insert_sign)
    db.query(sea.insert_sign_paid)
    db.query(sea.insert_sign_directional)
    db.query(sea.insert_sign_parklines)
    db.create_index('seattle_sign', 'direction')
    db.create_index('seattle_sign', 'code')
    db.create_index('seattle_sign', 'geom', index_type='gist')
    db.vacuum_analyze('public', 'seattle_sign')

    info("Creating signposts")
    db.query(sea.create_signpost)
    db.query(sea.insert_signpost)
    db.create_index('seattle_signpost', 'id')
    db.create_index('seattle_signpost', 'geobase_id')
    db.create_index('seattle_signpost', 'signs', index_type='gin')
    db.create_index('seattle_signpost', 'geom', index_type='gist')
    db.query(sea.add_signposts_to_sign)
    db.vacuum_analyze('public', 'seattle_signpost')

    info("Projecting signposts on road")
    duplicates = db.query(sea.project_signposts)
    if duplicates:
        warning("Duplicates found for projected signposts : {}"
                .format(str(duplicates)))

    percent, total = db.query(sea.count_signpost_projected)[0]

    if percent < 100:
        warning("Only {:.0f}% of signposts have been bound to a road. Total is {}"
                .format(percent, total))
        db.query(sea.generate_signposts_orphans)
        info("Table 'seattle_signpost_orphans' has been generated to check for orphans")

    db.query(sea.assign_directions)
    db.vacuum_analyze('public', 'seattle_sign')

    info("Creating likely slots")
    db.query(sea.create_slots_likely)
    db.query(sea.insert_slots_likely.format(isleft=1))
    db.query(sea.insert_slots_likely.format(isleft=-1))
    db.create_index('seattle_slots_likely', 'id')
    db.create_index('seattle_slots_likely', 'signposts', index_type='gin')
    db.create_index('seattle_slots_likely', 'geom', index_type='gist')
    db.vacuum_analyze('public', 'seattle_slots_likely')

    info("Creating nextpoints")
    db.query(sea.create_nextpoints_for_signposts)
    db.create_index('seattle_nextpoints', 'id')
    db.create_index('seattle_nextpoints', 'slot_id')
    db.create_index('seattle_nextpoints', 'direction')
    db.vacuum_analyze('public', 'seattle_nextpoints')

    info("Creating slots between signposts")
    db.query(sea.insert_slots_temp.format(offset=LINE_OFFSET))
    db.create_index('seattle_slots_temp', 'id')
    db.create_index('seattle_slots_temp', 'geom', index_type='gist')
    db.create_index('seattle_slots_temp', 'rules', index_type='gin')
    db.vacuum_analyze('public', 'seattle_slots_temp')

    if debug:
        info("Creating debug slots")
        db.query(sea.create_slots_for_debug.format(offset=LINE_OFFSET))
        db.create_index('seattle_slots_debug', 'pkid')
        db.create_index('seattle_slots_debug', 'geom', index_type='gist')
        db.vacuum_analyze('public', 'seattle_slots_debug')


def process_boston(debug=False):
    """
    process boston data and generate parking slots
    """
    def info(msg):
        return Logger.info("Boston: {}".format(msg))

    def debug(msg):
        return Logger.debug("Boston: {}".format(msg))

    def warning(msg):
        return Logger.warning("Boston: {}".format(msg))

    debug('Loading and translating rules')
    insert_rules('boston_rules_translation')
    db.vacuum_analyze('public', 'rules')

    info("Matching OSM roads with geobase")
    db.query(bos.create_roads_geobase)
    db.query(bos.match_roads_geobase.format(tbl="boston_geobase"))
    db.query(bos.match_roads_geobase.format(tbl="boston_metro_geobase"))
    db.create_index('boston_roads_geobase', 'id')
    db.create_index('boston_roads_geobase', 'roadsegment')
    db.create_index('boston_roads_geobase', 'osm_id')
    db.create_index('boston_roads_geobase', 'name')
    db.create_index('boston_roads_geobase', 'geom', index_type='gist')
    db.vacuum_analyze('public', 'boston_roads_geobase')

    info("Creating sign table")
    db.query(bos.create_sign)

    info("Loading signs")
    db.query(bos.insert_sign)
    db.query(bos.insert_sign_cambridge)
    db.create_index('boston_sign', 'geom', index_type='gist')
    db.create_index('boston_sign', 'direction')
    db.create_index('boston_sign', 'signpost')
    db.vacuum_analyze('public', 'boston_sign')

    info("Creating sign posts")
    db.query(bos.create_signpost)
    db.query(bos.insert_signpost)
    db.create_index('boston_signpost', 'geom', index_type='gist')
    db.create_index('boston_signpost', 'geobase_id')
    db.query(bos.add_signposts_to_sign)
    db.vacuum_analyze('public', 'boston_signpost')

    info("Projecting signposts on road")
    duplicates = db.query(bos.project_signposts)
    if duplicates:
        warning("Duplicates found for projected signposts : {}"
                .format(str(duplicates)))

    db.create_index('boston_signpost_onroad', 'id')
    db.create_index('boston_signpost_onroad', 'road_id')
    db.create_index('boston_signpost_onroad', 'isleft')
    db.create_index('boston_signpost_onroad', 'geom', index_type='gist')
    db.vacuum_analyze('public', 'boston_signpost_onroad')

    percent, total = db.query(bos.count_signpost_projected)[0]

    if percent < 100:
        warning("Only {:.0f}% of signposts have been bound to a road. Total is {}"
                .format(percent, total))
        db.query(bos.generate_signposts_orphans)
        info("Table 'boston_signpost_orphans' has been generated to check for orphans")

    info("Creating slots between signposts")
    db.query(bos.create_slots_likely)
    db.query(bos.insert_slots_likely.format(isleft=1))
    db.query(bos.insert_slots_likely.format(isleft=-1))
    db.create_index('boston_slots_likely', 'id')
    db.create_index('boston_slots_likely', 'signposts', index_type='gin')
    db.create_index('boston_slots_likely', 'geom', index_type='gist')
    db.vacuum_analyze('public', 'boston_slots_likely')

    db.query(bos.create_nextpoints_for_signposts)
    db.create_index('boston_nextpoints', 'id')
    db.create_index('boston_nextpoints', 'slot_id')
    db.create_index('boston_nextpoints', 'direction')
    db.vacuum_analyze('public', 'boston_nextpoints')

    db.create_index('boston_slots_temp', 'id')
    db.create_index('boston_slots_temp', 'geom', index_type='gist')
    db.create_index('boston_slots_temp', 'rules', index_type='gin')
    db.query(bos.insert_slots_temp.format(offset=LINE_OFFSET))

    info("Creating and overlaying paid slots")
    db.query(bos.overlay_paid_rules)
    db.vacuum_analyze('public', 'boston_slots_temp')

    if debug:
        info("Creating debug slots")
        db.query(bos.create_slots_for_debug.format(offset=LINE_OFFSET))
        db.create_index('boston_slots_debug', 'pkid')
        db.create_index('boston_slots_debug', 'geom', index_type='gist')
        db.vacuum_analyze('public', 'boston_slots_debug')


def cleanup_table():
    """
    Remove temporary tables
    """
    Logger.info("Cleanup schema")

    # drop universal temp tables
    for x in ["bad_intersection", "way_intersection", "roads", "signpost_onroad", "parking_lots_raw"]:
        db.query("DROP TABLE IF EXISTS {}".format(x))

    # drop per-city temp tables
    for x in ["slots_likely", "slots_temp", "nextpoints", "paid_temp", "signpost_temp",
            "paid_slots_raw", "bornes_raw", "bornes_clustered"]:
        for y in CITIES:
            db.query("DROP TABLE IF EXISTS {}_{}".format(y, x))


def process_osm():
    """
    Process OSM data
    """
    def info(msg):
        return Logger.info("OpenStreetMap: {}".format(msg))

    def debug(msg):
        return Logger.debug("OpenStreetMap: {}".format(msg))

    def warning(msg):
        return Logger.warning("OpenStreetMap: {}".format(msg))

    info("Filtering ways")
    db.query(osm.create_osm_ways)
    db.create_index('osm_ways', 'geom', index_type='gist')
    db.create_index('osm_ways', 'osm_id')
    db.create_index('osm_ways', 'name')

    info("Creating way intersections from planet lines")
    db.query(osm.create_way_intersection)
    db.create_index('way_intersection', 'way_id')
    db.create_index('way_intersection', 'geom', index_type='gist')
    db.vacuum_analyze('public', 'way_intersection')
    res = db.query(osm.remove_bad_intersection)
    if res:
        debug("Removed {} bad intersections".format(len(res)))

    info("Splitting ways on intersections")
    db.query(osm.split_osm_roads)
    db.create_index('roads', 'id')
    db.create_index('roads', 'osm_id')
    db.create_index('roads', 'name')
    db.create_index('roads', 'geom', index_type='gist')
    db.vacuum_analyze('public', 'roads')


def run(cities=CITIES, osm=False, debug=False):
    """
    Run the entire pipeline
    """
    Logger.debug("Loading extensions and custom functions")
    db.query("create extension if not exists fuzzystrmatch")
    db.query("create extension if not exists intarray")
    db.query(plfunctions.st_isleft_func)
    db.query(plfunctions.array_sort)
    db.query(plfunctions.get_max_range)

    if osm:
        process_osm()

    # create common tables
    db.query(common.create_rules)
    db.create_index('rules', 'code')
    db.query(common.create_slots)

    for x in cities:
        db.query(common.create_slots_temp.format(city=x))
        db.query(common.create_slots_partition.format(city=x))

    Logger.info("Processing parking lot / garage data")
    db.query(common.create_parking_lots)
    db.query(common.create_parking_lots_raw.format(city="montreal"))
    insert_raw_lots("montreal", "lots_montreal.csv")
    insert_parking_lots("montreal")
    db.query(common.create_parking_lots_raw.format(city="quebec"))
    insert_raw_lots("quebec", "lots_quebec.csv")
    insert_parking_lots("quebec")
    db.query(common.create_parking_lots_raw.format(city="seattle"))
    insert_raw_lots("seattle", "lots_seattle.csv")
    insert_parking_lots("seattle")
    db.query(common.create_parking_lots_raw.format(city="boston"))
    insert_raw_lots("boston", "lots_boston.csv")
    insert_parking_lots("boston")
    db.create_index('parking_lots', 'id')
    db.create_index('parking_lots', 'city')
    db.create_index('parking_lots', 'geom', index_type='gist')
    db.create_index('parking_lots', 'agenda', index_type='gin')

    db.query("DROP TABLE IF EXISTS parking_lots_streetview;")
    insert_lots_streetview("lots_newyork_streetview.csv")

    if 'montreal' in cities:
        process_montreal(debug)
    if 'quebec' in cities:
        process_quebec(debug)
    if 'newyork' in cities:
        process_newyork(debug)
    if 'seattle' in cities:
        process_seattle(debug)
    if 'boston' in cities:
        process_boston(debug)

    Logger.info("Shorten slots that intersect with roads or other slots")
    for x in cities:
        db.query(common.cut_slots_crossing_roads.format(city=x, offset=LINE_OFFSET))
        db.query(common.cut_slots_crossing_slots.format(city=x))

    Logger.info("Aggregating like slots")
    for x in cities:
        db.create_index(x+'_slots', 'id')
        db.create_index(x+'_slots', 'geom', index_type='gist')
        db.create_index(x+'_slots', 'rules', index_type='gin')
        db.query(common.aggregate_like_slots.format(city=x, within=3 if x == "seattle" else 0.1))
        db.query(common.create_client_data.format(city=x))
        db.vacuum_analyze('public', x+'_slots')

    Logger.info("Creating permit lists")
    db.query(common.create_permit_lists)
    for x in cities:
        db.query(common.insert_permit_lists.format(city=x))

    if not debug:
        cleanup_table()


def insert_rules(from_table):
    """
    Get rules from specific location (montreal, quebec),
    group them, make a simpler model and load them into database
    """
    Logger.debug("Get rules from {} and simplify them".format(from_table))
    rules = db.query(
        common.get_rules_from_source.format(source=from_table),
        namedtuple=True
    )
    rules_grouped = group_rules(rules)

    Logger.debug("Load rules into rules table")

    db.copy_from('public', 'rules', common.rules_columns, [
        [
            json.dumps(val).replace('\\', '\\\\') if isinstance(val, dict) else val
            for val in rule._asdict().values()]
        for rule in rules_grouped
    ])


def insert_raw_lots(city, filename):
    db.query("""
        COPY {}_parking_lots (name, operator, address, description, lun_normal, mar_normal, mer_normal,
            jeu_normal, ven_normal, sam_normal, dim_normal, hourly_normal, daily_normal, max_normal,
            lun_special, mar_special, mer_special, jeu_special, ven_special, sam_special, dim_special,
            hourly_special, daily_special, max_special, lun_free, mar_free, mer_free, jeu_free,
            ven_free, sam_free, dim_free, daily_free, indoor, handicap, card, valet, lat, long,
            capacity, street_view_lat, street_view_long, street_view_head, street_view_id, active,
            partner_name, partner_id)
        FROM '{}'
        WITH CSV HEADER
    """.format(city, os.path.join(os.path.dirname(__file__), 'data', filename)))


def insert_lots_streetview(filename):
    with open(os.path.join(os.path.dirname(__file__), 'data', 'load_lots_streetview.sql'), 'rb') as infile:
        db.query(infile.read().format(os.path.join(os.path.dirname(__file__), 'data', filename)))
        db.vacuum_analyze("public", "parking_lots_streetview")


def insert_parking_lots(city):
    columns = ["city", "name", "operator", "address", "description", "agenda", "capacity", "attrs",
        "geom", "active", "street_view", "partner_name", "partner_id", "geojson"]
    days = ["lun", "mar", "mer", "jeu", "ven", "sam", "dim"]
    lots, queries = [], []

    for row in db.query("""
        SELECT *, ST_Transform(ST_SetSRID(ST_MakePoint(long, lat), 4326), 3857) AS geom
        FROM {}_parking_lots
    """.format(city), namedtuple=True):
        lot = [(x.decode('utf-8').replace("'", "''") if x else '') for x in [row.name, row.operator, row.address, row.description]]

        # Create pricing rules per time period the lot is open
        agenda = {str(y): [] for y in range(1,8)}
        for x in range(1,8):
            if getattr(row, days[x - 1] + "_normal"):
                y = getattr(row, days[x - 1] + "_normal")
                hours = [float(z) for z in y.split(",")]
                if hours != [0.0, 24.0] and hours[0] > hours[1]:
                    nextday = str(x+1) if (x < 7) else "1"
                    agenda[nextday].append({"hours": [0.0, hours[1]], "max": row.max_normal or None,
                        "hourly": row.hourly_normal or None, "daily": row.daily_normal or None})
                    hours = [hours[0], 24.0]
                agenda[str(x)].append({"hours": hours, "hourly": row.hourly_normal or None,
                    "max": row.max_normal or None, "daily": row.daily_normal or None})
            if getattr(row, days[x - 1] + "_special"):
                y = getattr(row, days[x - 1] + "_special")
                hours = [float(z) for z in y.split(",")]
                if hours != [0.0, 24.0] and hours[0] > hours[1]:
                    nextday = str(x+1) if (x < 7) else "1"
                    agenda[nextday].append({"hours": [0.0, hours[1]], "max": row.max_special or None,
                        "hourly": row.hourly_special or None, "daily": row.daily_special or None})
                    hours = [hours[0], 24.0]
                agenda[str(x)].append({"hours": hours, "hourly": row.hourly_special or None,
                    "max": row.max_special or None, "daily": row.daily_special or None})
            if getattr(row, days[x - 1] + "_free"):
                y = getattr(row, days[x - 1] + "_free")
                hours = [float(z) for z in y.split(",")]
                if hours != [0.0, 24.0] and hours[0] > hours[1]:
                    nextday = str(x+1) if (x < 7) else "1"
                    agenda[nextday].append({"hours": [0.0, hours[1]], "max": None,
                        "hourly": 0, "daily": row.daily_free or None})
                    hours = [hours[0], 24.0]
                agenda[str(x)].append({"hours": hours, "hourly": 0, "max": None,
                    "daily": row.daily_free or None})

        # Create "closed" rules for periods not covered by an open rule
        for x in agenda:
            hours = sorted([y["hours"] for y in agenda[x]], key=lambda z: z[0])
            for i, y in enumerate(hours):
                starts = [z[0] for z in hours]
                if y[0] == 0.0:
                    continue
                last_end = hours[i-1][1] if not i == 0 else 0.0
                next_start = hours[i+1][0] if not i == (len(hours) - 1) else 24.0
                if not last_end in starts:
                    agenda[x].append({"hours": [last_end, y[0]], "hourly": None, "max": None,
                        "daily": None})
                if not next_start in starts and y[1] != 24.0:
                    agenda[x].append({"hours": [y[1], next_start], "hourly": None, "max": None,
                        "daily": None})
            if agenda[x] == []:
                agenda[x].append({"hours": [0.0,24.0], "hourly": None, "max": None, "daily": None})

        lot += [json.dumps(agenda), row.capacity or 0, json.dumps({"indoor": row.indoor,
            "handicap": row.handicap, "card": row.card, "valet": row.valet}), row.geom, row.active,
            row.street_view_head, row.street_view_id,
            "'{}'".format(row.partner_name) if row.partner_name else "NULL",
            "'{}'".format(row.partner_id) if row.partner_id else "NULL"]
        lots.append(lot)

    for x in lots:
        queries.append("""
            INSERT INTO parking_lots ({}) VALUES ('{city}', '{}', '{}', '{}', '{}', '{}'::jsonb, {},
                '{}'::jsonb, '{}'::geometry, '{}', json_build_object('head', {}, 'id', '{}')::jsonb,
                {}, {}, ST_AsGeoJSON(ST_Transform('{geom}'::geometry, 4326))::jsonb)
        """.format(",".join(columns), *[y for y in x], city=city, geom=x[-6]))
    db.queries(queries)


def insert_dynamic_rules_seattle():
    # load dynamic paid parking rules for Seattle
    paid_rules = []
    data = db.query("""
        SELECT ROW_NUMBER() OVER (ORDER BY wkd_start1), array_agg(elmntkey), wkd_start1,
            wkd_end1, wkd_start2, wkd_end2, wkd_start3, wkd_end3, sat_start1, sat_end1,
            sat_start2, sat_end2, sat_start3, sat_end3, sun_start1, sun_end1, sun_start2,
            sun_end2, sun_start3, sun_end3, wkd_rate1, wkd_rate2, wkd_rate3, sat_rate1,
            sat_rate2, sat_rate3, sun_rate1, sun_rate2, sun_rate3, parking_time_limit,
            rpz_spaces != 0, rpz_zone, peak_hour
        FROM seattle_parklines
        WHERE parking_category = 'Paid Parking'
        GROUP BY wkd_start1, wkd_end1, wkd_start2, wkd_end2, wkd_start3,
            wkd_end3, sat_start1, sat_end1, sat_start2, sat_end2, sat_start3, sat_end3,
            sun_start1, sun_end1, sun_start2, sun_end2, sun_start3, sun_end3, wkd_rate1,
            wkd_rate2, wkd_rate3, sat_rate1, sat_rate2, sat_rate3, sun_rate1, sun_rate2,
            sun_rate3, parking_time_limit, rpz_spaces != 0, rpz_zone, peak_hour
    """)
    for x in data:
        wkd2 = wkd3 = sat2 = sat3 = sun2 = sun3 = False
        if x[2] and x[3]:
            # weekday start/end times no1
            start, end = x[2], x[3]
            if x[4] and x[5] and x[4] == (end + 1) and x[20] == x[21]:
                end = x[5]
                wkd2 = True
                if x[6] and x[7] and x[6] == (end + 1) and x[21] == x[22]:
                    end = x[7]
                    wkd3 = True
            paid_rules.append(_dynrule(x, "MON-FRI", start, end, 1))
        if x[4] and x[5] and not wkd2:
            # weekday start/end times no2
            start, end = x[4], x[5]
            if x[6] and x[7] and x[6] == (end + 1) and x[21] == x[22]:
                end = x[7]
                wkd3 = True
            paid_rules.append(_dynrule(x, "MON-FRI", start, end, 2))
        if x[6] and x[7] and not wkd3:
            # weekday start/end times no3
            paid_rules.append(_dynrule(x, "MON-FRI", x[6], x[7], 3))
        if x[8] and x[9]:
            # saturday start/end times no1
            start, end = x[8], x[9]
            if x[10] and x[11] and x[10] == (end + 1) and x[23] == x[24]:
                end = x[11]
                sat2 = True
                if x[12] and x[13] and x[12] == (end + 1) and x[24] == x[25]:
                    end = x[13]
                    sat3 = True
            paid_rules.append(_dynrule(x, "SAT", start, end, 4))
        if x[10] and x[11] and not sat2:
            # saturday start/end times no2
            start, end = x[10], x[11]
            if x[12] and x[13] and x[12] == (end + 1) and x[24] == x[25]:
                end = x[13]
                sat3 = True
            paid_rules.append(_dynrule(x, "SAT", start, end, 5))
        if x[12] and x[13] and not sat3:
            # saturday start/end times no3
            paid_rules.append(_dynrule(x, "SAT", start, end, 6))
        if x[14] and x[15]:
            # sunday start/end times no1
            start, end = x[14], x[15]
            if x[16] and x[17] and x[16] == (end + 1) and x[26] == x[27]:
                end = x[17]
                sun2 = True
                if x[18] and x[19] and x[18] == (end + 1) and x[27] == x[28]:
                    end = x[19]
                    sun3 = True
            paid_rules.append(_dynrule(x, "SUN", start, end, 7))
        if x[16] and x[17] and not sun2:
            # sunday start/end times no2
            start, end = x[16], x[17]
            if x[18] and x[19] and x[18] == (end + 1) and x[27] == x[28]:
                end = x[19]
                sun3 = True
            paid_rules.append(_dynrule(x, "SUN", start, end, 8))
        if x[18] and x[19] and not sun3:
            # sunday start/end times no3
            paid_rules.append(_dynrule(x, "SUN", start, end, 9))
        if x[32]:
            # peak hour restriction
            insert_qry = "('{}', '{}', '{}'::jsonb, {}, ARRAY[{}]::varchar[], '{}', ARRAY{}::varchar[])"
            code, agenda = "SEA-PAID-{}-10".format(x[0]), {str(y): [] for y in range(1,8)}
            for z in x[32].split(" "):
                for y in range(1,6):
                    agenda[str(y)].append([tstr_to_float(z.split("-")[0] + z[-2:]),
                        tstr_to_float(z.split("-")[1])])
            desc = "PEAK HOUR NO PARKING WEEKDAYS {}".format(x[32])
            paid_rules.append(insert_qry.format(code, desc, json.dumps(agenda), "NULL",
                "'peak_hour'", "", x[1]))

    db.query("""
        INSERT INTO rules (code, description, agenda, time_max_parking, restrict_types, permit_no)
        SELECT code, description, agenda, time_max_parking, restrict_types, permit_no
        FROM (VALUES {}) AS d(code, description, agenda, time_max_parking, restrict_types, permit_no, ids)
    """.format(",".join([x for x in paid_rules])))
    db.query("""
        INSERT INTO seattle_sign_codes (code, signs)
        SELECT code, ids
        FROM (VALUES {}) AS d(code, description, agenda, time_max_parking, restrict_types,
            permit_no, ids)
    """.format(",".join([x for x in paid_rules])))


def _dynrule(x, per, start, end, count):
    insert_qry = "('{}', '{}', '{}'::jsonb, {}, ARRAY[{}]::varchar[], '{}', ARRAY{}::varchar[])"
    code, agenda = "SEA-PAID-{}-{}".format(x[0], count), {str(y): [] for y in range(1,8)}
    if per == "MON-FRI":
        for y in range(1,6):
            agenda[str(y)].append([float(start) / 60.0, round(float(end) / 60.0)])
    else:
        agenda["6" if per == "SAT" else "7"].append([float(start) / 60.0, round(float(end) / 60.0)])
    desc = "PAID PARKING {}-{} {} ${}/hr".format(pretty_time(start), pretty_time(end), per,
        "{0:.2f}".format(float(x[19 + count])))
    return insert_qry.format(code, desc, json.dumps(agenda), int(x[29]) if x[29] else "NULL",
        "'paid'" + (",'permit'" if x[30] else ""), x[31] if x[31] else "", x[1])
