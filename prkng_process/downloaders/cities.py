# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals

import csv
import os
import requests
import subprocess
import zipfile

from . import DataSource, script
from .. import CONFIG
from ..logger import Logger
from ..utils import download_progress


def CitySources():
    return [Montreal, Quebec, NewYork]


class Montreal(DataSource):
    """
    Download data from Montreal city
    """
    def __init__(self):
        super(Montreal, self).__init__()
        self.name = 'Montréal'
        self.city = 'montreal'
        # ckan API
        self.url_signs = "http://donnees.ville.montreal.qc.ca/api/3/action/package_show?id=stationnement-sur-rue-signalisation-courant"

        self.url_roads = "http://donnees.ville.montreal.qc.ca/api/3/action/package_show?id=geobase"

        self.resources = (
            'Ahuntsic-Cartierville',
            'Côte-des-Neiges-Notre-Dame-de-Grâce',
            'Mercier-Hochelaga-Maisonneuve',
            'Outremont',
            'Plateau-Mont-Royal',
            'Rosemont-La Petite-Patrie',
            'Saint-Laurent',
            'Le Sud-Ouest',
            'Ville-Marie',
            'Villeray-Saint-Michel-Parc-Extension',
            'signalisation-description-panneau'
        )

        self.jsonfiles = []
        self.paid_zone_shapefile = script('paid_montreal_zones.kml')

    def download(self):
        self.download_signs()
        self.download_roads()

    def download_roads(self):
        """
        Download roads (geobase) using CKAN API
        """
        json = requests.get(self.url_roads).json()
        url = ''

        for res in json['result']['resources']:
            if res['name'].lower() == 'géobase' and res['format'] == 'ZIP':
                url = res['url']

        Logger.info("Downloading Montreal Géobase")
        zfile = download_progress(
            url.replace('ckanprod', 'donnees.ville.montreal.qc.ca'),
            os.path.basename(url),
            CONFIG['DOWNLOAD_DIRECTORY']
        )

        Logger.info("Unzipping")
        with zipfile.ZipFile(zfile) as zip:
            self.road_shapefile = os.path.join(CONFIG['DOWNLOAD_DIRECTORY'], [
                name for name in zip.namelist()
                if name.lower().endswith('.shp')
            ][0])
            zip.extractall(CONFIG['DOWNLOAD_DIRECTORY'])

    def download_signs(self):
        """
        Download signs using CKAN API
        """
        json = requests.get(self.url_signs).json()
        subs = {}
        for res in json['result']['resources']:
            for sub in self.resources:
                if sub.lower() in res['name'].lower():
                    subs[res['name']] = res['url']

        for area, url in subs.iteritems():
            Logger.info("Downloading Montreal - {} ".format(area))
            zfile = download_progress(
                url.replace('ckanprod', 'donnees.ville.montreal.qc.ca'),
                os.path.basename(url),
                CONFIG['DOWNLOAD_DIRECTORY']
            )

            Logger.info("Unzipping")
            with zipfile.ZipFile(zfile) as zip:
                if 'description' not in zfile:
                    self.jsonfiles.append(os.path.join(CONFIG['DOWNLOAD_DIRECTORY'], [
                        name for name in zip.namelist()
                        if name.lower().endswith('.json')
                    ][0]))
                else:
                    self.csvfile = os.path.join(CONFIG['DOWNLOAD_DIRECTORY'], [
                        name for name in zip.namelist()
                        if name.lower().endswith('.csv')
                    ][0])

                zip.extractall(CONFIG['DOWNLOAD_DIRECTORY'])

    def load(self):
        """
        Loads geojson files
        """
        subprocess.check_call(
            'ogr2ogr -f "PostgreSQL" PG:"dbname=prkng user={PG_USERNAME}  '
            'password={PG_PASSWORD} port={PG_PORT} host={PG_HOST}" -overwrite '
            '-nlt point -s_srs EPSG:2145 -t_srs EPSG:3857 -lco GEOMETRY_NAME=geom  '
            '-nln montreal_poteaux {}'.format(self.jsonfiles[0], **CONFIG),
            shell=True
        )
        for jsondata in self.jsonfiles[1:]:
            subprocess.check_call(
                'ogr2ogr -f "PostgreSQL" PG:"dbname=prkng user={PG_USERNAME}  '
                'password={PG_PASSWORD} port={PG_PORT} host={PG_HOST}" '
                '-append -nlt point -s_srs EPSG:2145 -t_srs EPSG:3857 '
                '-nln montreal_poteaux {}'.format(jsondata, **CONFIG),
                shell=True
            )

        self.db.vacuum_analyze("public", "montreal_poteaux")

        subprocess.check_call(
            "shp2pgsql -d -g geom -s 2145:3857 -W LATIN1 -I {filename} montreal_geobase | "
            "psql -q -d {PG_DATABASE} -h {PG_HOST} -U {PG_USERNAME} -p {PG_PORT}"
            .format(filename=self.road_shapefile, **CONFIG),
            shell=True
        )
        self.db.vacuum_analyze("public", "montreal_geobase")

        subprocess.check_call(
            "shp2pgsql -d -g geom -s 2145:3857 -W LATIN1 -I {filename} montreal_bornes | "
            "psql -q -d {PG_DATABASE} -h {PG_HOST} -U {PG_USERNAME} -p {PG_PORT}"
            .format(filename=script('bornes_montreal.shp'), **CONFIG),
            shell=True
        )
        self.db.vacuum_analyze("public", "montreal_bornes")

        # loading csv data using script
        Logger.debug("loading file '%s' with script '%s'" %
                     (self.csvfile, script('montreal_load_panneau_descr.sql')))

        with open(script('montreal_load_panneau_descr.sql'), 'rb') as infile:
            self.db.query(infile.read().format(description_panneau=self.csvfile))
            self.db.vacuum_analyze("public", "montreal_descr_panneau")

    def load_rules(self):
        """
        load parking rules translation
        """
        filename = script("rules_montreal.csv")

        Logger.info("Loading parking rules for {}".format(self.name))
        Logger.debug("loading file '%s' with script '%s'" %
                     (filename, script('montreal_load_rules.sql')))

        with open(script('montreal_load_rules.sql'), 'rb') as infile:
            self.db.query(infile.read().format(filename))
            self.db.vacuum_analyze("public", "montreal_rules_translation")

    def get_extent(self):
        """
        get extent in the format latmin, longmin, latmax, longmax
        """
        res = self.db.query(
            """WITH tmp AS (
                SELECT st_transform(st_envelope(st_collect(geom)), 4326) as geom
                FROM montreal_poteaux
            ) select st_ymin(geom), st_xmin(geom), st_ymax(geom), st_xmax(geom) from tmp
            """)[0]
        return res


class Quebec(DataSource):
    """
    Download data from Quebec city
    """
    def __init__(self):
        super(Quebec, self).__init__()
        self.name = 'Quebec City'
        self.city = 'quebec'
        self.url = "http://donnees.ville.quebec.qc.ca/Handler.ashx?id=7&f=SHP"
        self.url_payant = "http://donnees.ville.quebec.qc.ca/Handler.ashx?id=8&f=SHP"

    def download(self):
        """
        Download and unzip file
        """
        Logger.info("Downloading {} parking data".format(self.name))
        zfile = download_progress(
            self.url,
            "quebec_latest.zip",
            CONFIG['DOWNLOAD_DIRECTORY']
        )

        Logger.info("Unzipping")
        with zipfile.ZipFile(zfile) as zip:
            self.filename = os.path.join(CONFIG['DOWNLOAD_DIRECTORY'], [
                name for name in zip.namelist()
                if name.lower().endswith('.shp')
            ][0])
            zip.extractall(CONFIG['DOWNLOAD_DIRECTORY'])

        Logger.info("Downloading {} paid parking data".format(self.name))
        zfile = download_progress(
            self.url_payant,
            "quebec_paid_latest.zip",
            CONFIG['DOWNLOAD_DIRECTORY']
        )

        Logger.info("Unzipping")
        with zipfile.ZipFile(zfile) as zip:
            self.filename_payant = os.path.join(CONFIG['DOWNLOAD_DIRECTORY'], [
                name for name in zip.namelist()
                if name.lower().endswith('.shp')
            ][0])
            zip.extractall(CONFIG['DOWNLOAD_DIRECTORY'])

    def load(self):
        Logger.info("Loading {} data".format(self.name))

        subprocess.check_call(
            "shp2pgsql -d -g geom -s 4326:3857 -W LATIN1 -I "
            "{filename} quebec_panneau | "
            "psql -q -d {PG_DATABASE} -h {PG_HOST} -U {PG_USERNAME} -p {PG_PORT}"
            .format(filename=self.filename, **CONFIG),
            shell=True
        )

        self.db.create_index('quebec_panneau', 'type_desc')
        self.db.create_index('quebec_panneau', 'nom_topog')
        self.db.create_index('quebec_panneau', 'id_voie_pu')
        self.db.create_index('quebec_panneau', 'lect_met')
        self.db.vacuum_analyze("public", "quebec_panneau")

        subprocess.check_call(
            "shp2pgsql -d -g geom -s 4326:3857 -W LATIN1 -I "
            "{filename} quebec_bornes | "
            "psql -q -d {PG_DATABASE} -h {PG_HOST} -U {PG_USERNAME} -p {PG_PORT}"
            .format(filename=self.filename_payant, **CONFIG),
            shell=True
        )

    def load_rules(self):
        """
        load parking rules translation
        """
        Logger.info("Loading parking rules for {}".format(self.name))

        filename = script("rules_quebec.csv")

        Logger.debug("loading file '%s' with script '%s'" %
                     (filename, script('quebec_load_rules.sql')))

        with open(script('quebec_load_rules.sql'), 'rb') as infile:
            self.db.query(infile.read().format(filename))
            self.db.vacuum_analyze("public", "quebec_rules_translation")

    def get_extent(self):
        """
        get extent in the format latmin, longmin, latmax, longmax
        """
        res = self.db.query(
            """WITH tmp AS (
                SELECT st_transform(st_envelope(st_collect(geom)), 4326) as geom
                FROM quebec_panneau
            ) select st_ymin(geom), st_xmin(geom), st_ymax(geom), st_xmax(geom) from tmp
            """)[0]
        return res


class NewYork(DataSource):
    """
    Download data from New York City
    """
    def __init__(self):
        super(NewYork, self).__init__()
        self.name = 'New York'
        self.city = 'newyork'
        self.url_signs = "http://a841-dotweb01.nyc.gov/datafeeds/ParkingReg/Parking_Regulation_Shapefile.zip"
        self.url_roads = "https://data.cityofnewyork.us/api/geospatial/exjm-f27b?method=export&format=Shapefile"
        self.url_snd = "http://www.nyc.gov/html/dcp/download/bytes/snd15c.zip"
        self.url_loc = "http://a841-dotweb01.nyc.gov/datafeeds/ParkingReg/signs.CSV"

    def download(self):
        self.download_signs()
        self.download_roads()
        self.download_snd()
        self.download_locations()

    def download_roads(self):
        """
        Download NYC Street Centerline (CSCL) shapefile
        """
        Logger.info("Downloading New York Centerlines")
        zfile = download_progress(
            self.url_roads,
            "nyc_cscl.zip",
            CONFIG['DOWNLOAD_DIRECTORY'],
            ua=True
        )

        Logger.info("Unzipping")
        with zipfile.ZipFile(zfile) as zip:
            self.road_shapefile = os.path.join(CONFIG['DOWNLOAD_DIRECTORY'], [
                name for name in zip.namelist()
                if name.lower().endswith('.shp')
            ][0])
            zip.extractall(CONFIG['DOWNLOAD_DIRECTORY'])

    def download_signs(self):
        """
        Download signs
        """
        Logger.info("Downloading New York sign data")
        zfile = download_progress(
            self.url_signs,
            os.path.basename(self.url_signs),
            CONFIG['DOWNLOAD_DIRECTORY']
        )

        Logger.info("Unzipping")
        with zipfile.ZipFile(zfile) as zip:
            self.sign_shapefile = os.path.join(CONFIG['DOWNLOAD_DIRECTORY'], [
                name for name in zip.namelist()
                if name.lower().endswith('.shp')
            ][0])
            zip.extractall(CONFIG['DOWNLOAD_DIRECTORY'])

    def download_snd(self):
        """
        Download Street Name Dictionary
        """
        Logger.info("Downloading New York Street Name Dictionary")
        zfile = download_progress(
            self.url_snd,
            "nyc_snd.zip",
            CONFIG['DOWNLOAD_DIRECTORY']
        )

        Logger.info("Unzipping")
        with zipfile.ZipFile(zfile) as zip:
            self.snd_file = os.path.join(CONFIG['DOWNLOAD_DIRECTORY'], [
                name for name in zip.namelist()
                if name.lower().endswith('.txt')
            ][0])
            self.snd_csv = os.path.join(CONFIG['DOWNLOAD_DIRECTORY'], self.snd_file.split(".txt")[0] + ".csv")
            zip.extractall(CONFIG['DOWNLOAD_DIRECTORY'])

    def download_locations(self):
        """
        Download Street Locations
        """
        Logger.info("Downloading New York Street Locations Index")
        csvfile = download_progress(
            self.url_loc,
            "nyc_loc.csv",
            CONFIG['DOWNLOAD_DIRECTORY']
        )
        self.loc_file = os.path.join(CONFIG['DOWNLOAD_DIRECTORY'], "nyc_loc.csv")

    def load(self):
        """
        Loads data into database
        """
        subprocess.check_call(
            'shp2pgsql -d -g geom -s 4326:3857 -W LATIN1 -I {filename} newyork_signs_raw | '
            'psql -q -d {PG_DATABASE} -h {PG_HOST} -U {PG_USERNAME} -p {PG_PORT}'
            .format(filename=self.sign_shapefile, **CONFIG),
            shell=True
        )

        self.db.vacuum_analyze("public", "newyork_signs_raw")

        subprocess.check_call(
            'shp2pgsql -d -g geom -t 2D -s 2263:3857 -W LATIN1 -I {filename} newyork_geobase | '
            'psql -q -d {PG_DATABASE} -h {PG_HOST} -U {PG_USERNAME} -p {PG_PORT}'
            .format(filename=self.road_shapefile, **CONFIG),
            shell=True
        )
        self.db.vacuum_analyze("public", "newyork_geobase")

        snd_lines = []
        with open(self.snd_file, "r") as f:
            for x in f.readlines():
                if not x.startswith("1") or x[50] not in [" ", "H", "M"] or x[34:36] not in ["PF", "VF", "VS"]:
                    continue
                snd_lines.append([x[1], x[2:34], x[36:42], x[36:44]])
        with open(self.snd_csv, "wb") as f:
            csvwriter = csv.writer(f)
            csvwriter.writerows(snd_lines)

        self.db.query("""
            DROP TABLE IF EXISTS newyork_snd;
            CREATE TABLE newyork_snd (
                id SERIAL PRIMARY KEY,
                boro smallint,
                stname_lab varchar,
                b5sc integer,
                b7sc integer
            );

            COPY newyork_snd (boro, stname_lab, b5sc, b7sc) FROM '{}' CSV;
        """.format(self.snd_csv))

        self.db.query("""
            DROP TABLE IF EXISTS newyork_roads_locations;
            CREATE TABLE newyork_roads_locations (
                id SERIAL PRIMARY KEY,
                boro varchar,
                order_no varchar,
                main_st varchar,
                from_st varchar,
                to_st varchar,
                sos varchar
            );

            COPY newyork_roads_locations (boro, order_no, main_st, from_st, to_st, sos) FROM '{}' CSV HEADER;
        """.format(self.loc_file))
        self.db.create_index("newyork_roads_locations", "main_st")

    def load_rules(self):
        """
        load parking rules translation
        """
        Logger.info("Loading parking rules for {}".format(self.name))

        filename = script("rules_newyork.csv")

        Logger.debug("loading file '%s' with script '%s'" %
                     (filename, script('newyork_load_rules.sql')))

        with open(script('newyork_load_rules.sql'), 'rb') as infile:
            self.db.query(infile.read().format(filename))
            self.db.vacuum_analyze("public", "newyork_rules_translation")

    def get_extent(self):
        """
        get extent in the format latmin, longmin, latmax, longmax
        """
        res = self.db.query(
            """WITH tmp AS (
                SELECT st_transform(st_envelope(st_collect(geom)), 4326) as geom
                FROM newyork_signs_raw
            ) select st_ymin(geom), st_xmin(geom), st_ymax(geom), st_xmax(geom) from tmp
            """)[0]
        return res
