# -*- coding: utf-8 -*-
from __future__ import print_function

from . import CONFIG
from .logger import Logger

import click
import datetime
import os
import subprocess


@click.group()
def main():
    pass


@click.command()
def export():
    """
    Export processed data tables to file
    """
    tables = ["montreal_slots", "quebec_slots", "newyork_slots", "seattle_slots", "cities", "city_assets", "parking_lots", "rules"]

    Logger.info('Exporting processed tables...')
    export_dir = os.path.join(os.path.dirname(os.environ["PRKNG_SETTINGS"]), 'export')
    file_name = 'prkng-data-{}.sql.gz'.format(datetime.datetime.now().strftime('%Y%m%d-%H%M'))
    if not os.path.exists(export_dir):
        os.mkdir(export_dir)
    subprocess.check_call('pg_dump -c {tbls} -U {PG_USERNAME} {PG_DATABASE} | gzip > {path}'.format(
        path=os.path.join(export_dir, file_name), PG_USERNAME=CONFIG["PG_USERNAME"], PG_DATABASE=CONFIG["PG_DATABASE"],
        tbls=" ".join(["-t '{}'".format(x) for x in tables])),
        shell=True)
    Logger.info('Table export created and stored as {}'.format(os.path.join(export_dir, file_name)))


@click.command()
@click.option('--city', default='all',
    help='A specific city to fetch data for (instead of all)')
def update(city):
    """
    Update data sources
    """
    from .downloaders import DataSource
    from .downloaders.cities import CitySources
    from .downloaders.zones import OsmLoader, ZoneLoader
    osm = OsmLoader()
    zl = ZoneLoader()
    zl.update()
    for source in CitySources():
        obj = source()
        if city != 'all' and obj.city != city:
            continue
        obj.download()
        obj.load()
        obj.load_rules()
        # download osm data related to data extent
        osm.download(obj.name, obj.get_extent())

    # load every osm files in one shot
    osm.load(city)


@click.command(name="update-areas")
def update_areas():
    """
    Create a new version of service area statics and upload to S3
    """
    from .downloaders.zones import ServiceAreasLoader
    sal = ServiceAreasLoader()
    sal.process_areas()


@click.command()
@click.option('--city', default='montreal,quebec,newyork',
    help='A specific city (or comma-separated list of cities) to process data for')
@click.option('--osm', default=True,
    help='Reprocess OSM roads/map data')
@click.option('--debug', default=False,
    help='Create debug slot tables and keep temp tables')
def process(city, osm, debug):
    """
    Process data and create the target tables
    """
    from . import pipeline
    pipeline.run(city.split(","), osm, debug)


main.add_command(export)
main.add_command(update)
main.add_command(update_areas)
main.add_command(process)
