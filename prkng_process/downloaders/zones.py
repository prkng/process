from __future__ import print_function, unicode_literals

import geojson
import gzip
import os
import StringIO
import subprocess
import zipfile

from boto.s3.key import Key
from boto.s3.connection import S3Connection

from . import DataSource, script
from .. import CONFIG
from ..logger import Logger


class OsmLoader(DataSource):
    """
    Load osm data according to bbox given
    """
    def __init__(self):
        # queue containing osm filenames
        super(OsmLoader, self).__init__()
        self.queue = []

    def download(self, name, extent):
        Logger.info("Getting OpenStreetMap ways for {}".format(name))
        Logger.debug("https://overpass-api.de/api/interpreter?data=(way({});>;);out;"
                     .format(','.join(map(str, extent))))
        osm_file = download_progress(
            "https://overpass-api.de/api/interpreter?data=(way({});>;);out;"
            .format(','.join(map(str, extent))),
            '{}.osm'.format(name.lower()),
            CONFIG['DOWNLOAD_DIRECTORY']
        )

        self.queue.append(osm_file)

    def load(self, city):
        """
        Load data using osm2pgsql
        """
        if city == 'all':
            process_file = os.path.join(CONFIG['DOWNLOAD_DIRECTORY'], 'merged.osm')

            # merge files before loading because osm2pgsql failed to load 2 osm files
            # at the same time
            subprocess.check_call("osmconvert {files} -o={merge}".format(
                files=' '.join(['"'+x+'"' for x in self.queue]),
                merge=process_file),
                shell=True)
        else:
            process_file = os.path.join(CONFIG['DOWNLOAD_DIRECTORY'], self.queue[0])

        subprocess.check_call(
            "osm2pgsql -E 3857 -d {PG_DATABASE} -H {PG_HOST} -U {PG_USERNAME} "
            "-P {PG_PORT} {osm_file}".format(
                osm_file=process_file,
                **CONFIG),
            shell=True
        )

        # add indexes on OSM lines
        self.db.create_index('planet_osm_line', 'way', index_type='gist')
        self.db.create_index('planet_osm_line', 'osm_id')
        self.db.create_index('planet_osm_line', 'name')
        self.db.create_index('planet_osm_line', 'highway')
        self.db.create_index('planet_osm_line', 'boundary')


class ZoneLoader(object):
    """
    Import assorted zone shapefiles
    """
    def update(self):
        Logger.info("Importing permit zone shapefiles")
        subprocess.check_call(
            "shp2pgsql -d -g geom -s 3857 -W LATIN1 -I {filename} permit_zones | "
            "psql -q -d {PG_DATABASE} -h {PG_HOST} -U {PG_USERNAME} -p {PG_PORT}"
            .format(filename=script('permit_zones.shp'), **CONFIG),
            shell=True
        )

        Logger.info("Importing metered rate zone shapefiles")
        subprocess.check_call(
            "shp2pgsql -d -g geom -s 3857 -W LATIN1 -I {filename} metered_rate_zones | "
            "psql -q -d {PG_DATABASE} -h {PG_HOST} -U {PG_USERNAME} -p {PG_PORT}"
            .format(filename=script('metered_rate_zones.shp'), **CONFIG),
            shell=True
        )


class ServiceAreasLoader(DataSource):
    """
    Import service area shapefiles, upload statics to S3
    """
    def __init__(self):
        super(ServiceAreasLoader, self).__init__()
        self.bucket = S3Connection(CONFIG["AWS_ACCESS_KEY"],
            CONFIG["AWS_SECRET_KEY"]).get_bucket('prkng-service-areas')
        self.areas_qry = """
            SELECT
                gid AS id,
                name,
                name_disp,
                ST_As{}(ST_Transform(geom, 4326)) AS geom
            FROM cities
        """
        self.mask_qry = """
            SELECT
                1,
                'world_mask',
                'world_mask',
                ST_As{}(ST_Transform(geom, 4326)) AS geom
            FROM cities_mask
        """

    def upload_kml(self, version, query, gz=False):
        kml_res = self.db.query(query.format("KML"))
        kml = ('<?xml version="1.0" encoding="utf-8"?>'
            '<kml xmlns="http://www.opengis.net/kml/2.2">'
                '{}'
            '</kml>').format(''.join(['<Placemark>'+x[3]+'</Placemark>' for x in kml_res]))

        strio = StringIO.StringIO()
        kml_file = gzip.GzipFile(fileobj=strio, mode='w')
        kml_file.write(kml)
        kml_file.close()
        strio.seek(0)
        key1 = self.bucket.new_key('{}.kml.gz'.format(version))
        key1.set_contents_from_file(strio, {"x-amz-acl": "public-read",
            "Content-Type": "application/gzip"})
        strio.seek(0)
        key2 = self.bucket.new_key('{}.kml'.format(version))
        key2.set_contents_from_file(strio, {"x-amz-acl": "public-read",
            "Content-Encoding": "gzip",
            "Content-Type": "application/xml"})
        return key1.generate_url(0)

    def upload_geojson(self, version, query):
        json_res = self.db.query(query.format("GeoJSON"))
        json = geojson.dumps(geojson.FeatureCollection([
            geojson.Feature(
                id=x[0],
                geometry=geojson.loads(x[3]),
                properties={"id": x[0], "name": x[1], "name_disp": x[2]}
            ) for x in json_res
        ]))

        strio = StringIO.StringIO()
        json_file = gzip.GzipFile(fileobj=strio, mode='w')
        json_file.write(json)
        json_file.close()
        strio.seek(0)
        key1 = self.bucket.new_key('{}.geojson.gz'.format(version))
        key1.set_contents_from_file(strio, {"x-amz-acl": "public-read",
            "Content-Type": "application/gzip"})
        strio.seek(0)
        key2 = self.bucket.new_key('{}.geojson'.format(version))
        key2.set_contents_from_file(strio, {"x-amz-acl": "public-read",
            "Content-Encoding": "gzip",
            "Content-Type": "application/json"})
        return key1.generate_url(0)

    def process_areas(self):
        """
        Reload service area statics from source and upload new version of statics to S3
        """
        self.db.query("""
            CREATE TABLE IF NOT EXISTS city_assets (
                id serial PRIMARY KEY,
                version integer,
                kml_addr varchar,
                kml_mask_addr varchar,
                geojson_addr varchar,
                geojson_mask_addr varchar
            )
        """)

        version_res = self.db.query("""
            SELECT version
            FROM city_assets
            ORDER BY version DESC
            LIMIT 1
        """)
        version = str((version_res[0][0] if version_res else 0) + 1)

        Logger.info("Importing service area shapefiles")
        subprocess.check_call(
            "shp2pgsql -d -g geom -s 3857 -I {filename} cities | "
            "psql -q -d {PG_DATABASE} -h {PG_HOST} -U {PG_USERNAME} -p {PG_PORT}"
            .format(filename=script('service_areas.shp'), **CONFIG),
            shell=True
        )
        subprocess.check_call(
            "shp2pgsql -d -g geom -s 3857 -I {filename} cities_mask | "
            "psql -q -d {PG_DATABASE} -h {PG_HOST} -U {PG_USERNAME} -p {PG_PORT}"
            .format(filename=script('service_areas_mask.shp'), **CONFIG),
            shell=True
        )
        self.db.query("""update cities
            set geom = st_makevalid(geom) where not st_isvalid(geom)""")
        self.db.create_index('cities', 'geom', index_type='gist')
        self.db.vacuum_analyze("public", "cities")

        Logger.info("Exporting new version of statics to S3")
        kml_url = self.upload_kml(version, self.areas_qry)
        kml_mask_url = self.upload_kml(version + ".mask", self.mask_qry)
        json_url = self.upload_geojson(version, self.areas_qry)
        json_mask_url = self.upload_geojson(version + ".mask", self.mask_qry)

        Logger.info("Saving metadata")
        self.db.query("""
            INSERT INTO city_assets
                (version, kml_addr, kml_mask_addr, geojson_addr, geojson_mask_addr)
            SELECT {}, '{}', '{}', '{}', '{}'
        """.format(version, kml_url.split('?')[0], kml_mask_url.split('?')[0],
            json_url.split('?')[0], json_mask_url.split('?')[0]))
