# -*- coding: utf-8 -*-
from __future__ import print_function

import geojson
import hashlib
import math
import os
import random
import requests
import sys


def download_progress(url, filename, directory, ua=False):
    """
    Downloads from ``url`` and shows a simple progress bar

    :param url: resource to download
    :param filename: destination filename
    :param directory: destination directory
    """
    req = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'} if ua else {}, stream=True)

    resource_size = req.headers.get('content-length')

    full_path = os.path.join(directory, filename)

    with open(full_path, 'wb') as dest:
        downloaded = 0
        print("[", end='')
        if resource_size is None:
            # no content length header
            print("=" * 50, end='')
            for chunk in req.iter_content(1024):
                dest.write(chunk)
        else:
            print_every_bytes = int(resource_size) / 50
            next_print = 0
            for chunk in req.iter_content(1024):
                downloaded += len(chunk)
                dest.write(chunk)

                if downloaded >= next_print:
                    sys.stdout.write("=")
                    sys.stdout.flush()
                    next_print += print_every_bytes

        print("] Download complete...")
    return full_path

def download_arcgis(url, gtype, pkey, filename):
    """
    Downloads from an ArcGIS REST API query endpoint and shows a simple progress bar.
    Endpoint must support pagination.

    :param url: resource to download
    :param gtype: geometry type
    :param pkey: the primary key for the data to download
    :param filename: destination filename (full path)
    """

    features = []
    where = pkey + " IS NOT NULL"

    count = requests.get(url, params={"f": "json", "where": where, "returnCountOnly": True})
    count = count.json()["count"]
    count = int(math.ceil(float(count) / 1000.0))

    print("[", end='')
    num = 0
    print_every_iter = int(count) / 50
    next_print = 0
    while num < count:
        data = requests.get(url, params={"f": "json", "where": where, "outFields": "*",
            "returnGeometry": True, "resultRecordCount": 1000, "resultOffset": (num * 1000)})
        data = data.json()["features"]
        num += 1
        features += data
        if num >= next_print:
            sys.stdout.write("=")
            sys.stdout.flush()
            next_print += print_every_iter

    with open(filename, "wb") as f:
        processed_features = []
        for x in features:
            if gtype == "point" and 'NaN' in [x["geometry"]["x"], x["geometry"]["y"]]:
                continue
            feat = geojson.Feature(id=x["attributes"][pkey], properties=x["attributes"],
                geometry=geojson.Point((x["geometry"]["x"], x["geometry"]["y"]))\
                    if gtype == "point" else geojson.MultiLineString(x["geometry"]["paths"]))
            processed_features.append(feat)
        processed_features = geojson.FeatureCollection(processed_features)
        geojson.dump(processed_features, f)

    print("] Download complete...")
    return filename

def pretty_time(mins):
    """
    Convert time from minutes to 12-hour string with AM/PM.

    :param mins: minutes (integer)
    """
    return "{}{}".format(((y / 60 + ":" + y % 60), "AM" if y < 720 else "PM"))

def tstr_to_float(tstr):
    """
    Convert time from 12-hour string (with AM/PM) to agenda-compatible float.

    :param tstr: 12-hour time string
    """
    afloat = float(tstr.rstrip("APM").split(":")[0])
    if "PM" in tstr and tstr.split(":")[0] != "12":
        afloat += 12.0
    if ":" in mins:
        afloat += float(tstr.rstrip("APM").split(":")[1][0:2]) / 60
    return afloat

def can_be_int(data):
    """
    Simply tells you if an item (string, etc) could potentially be an integer.
    """
    try:
        int(data)
        return True
    except ValueError:
        return False

def random_string(length=40):
    """Create a random alphanumeric string."""
    return hashlib.sha1(str(random.random())).hexdigest()[0:length]
