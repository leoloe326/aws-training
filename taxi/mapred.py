#!/usr/bin/env python

# Copyright 2017, Nan Dun <nan.dun@acm.org>
# All rights reserved.

import argparse
import sys
import os.path
import fileinput

from collections import defaultdict
from collections import Counter

from geo import NYCGeoPolygon
from raw2aws import RawReader

def parse_argv():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('--src', metavar='URI', type=str,
        default='s3://aws-nyc-taxi-data', help="data source directory")

    parser.add_argument("--dst", metavar='URI', type=str,
        default='file://', help="data destination directory")

    parser.add_argument('-m', "--map", dest='action', action='store_const',
        const='map', default=None, help="map phase")

    parser.add_argument('-r', "--reduce", dest='action', action='store_const',
        const='reduce', default=None, help="reduce phase")

    args = parser.parse_args()
    return args

class NYCTaxiStat:
    def __init__(self, opts):
        self.opts = opts
        self.cwd = os.path.dirname(__file__)

        # Load Boroughs and Community Districts information
        # self.boroughs = NYCGeoPolygon.load(self.get_fullpath('nyc_boroughs.geojson'))
        self.districts = NYCGeoPolygon.load(self.get_fullpath('nyc_community_districts.geojson'))

        # Statistics
        self.district_counter = defaultdict(Counter)

    def get_fullpath(self, filename):
        return os.path.join(self.cwd, filename)

if __name__ == '__main__':
    opts = parse_argv()
    p = NYCTaxiStat(opts)
    p.run()
