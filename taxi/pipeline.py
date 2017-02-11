#!/usr/bin/env python

# Copyright 2017, Nan Dun <nan.dun@acm.org>
# All rights reserved.


# NYC Taxi Data Format
# Yellow: http://www.nyc.gov/html/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf
# Green:  http://www.nyc.gov/html/tlc/downloads/pdf/data_dictionary_trip_records_green.pdf

import argparse
import sys
import os.path
import fileinput

from collections import defaultdict
from collections import Counter

from geo import NYCGeoPolygon

def parse_argv():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('-m', "--map", dest='action', action='store_const',
        const='map', default=None, help="map phase")

    parser.add_argument('-r', "--reduce", dest='action', action='store_const',
        const='reduce', default=None, help="reduce phase")

    args = parser.parse_args()
    return args

class Pipeline:
    def __init__(self, opts):
        self.opts = opts
        self.cwd = os.path.dirname(__file__)

        # Load Boroughs and Community Districts information
        self.boroughs = NYCGeoPolygon.load(self.get_fullpath('nyc_boroughs.geojson'))
        self.districts = NYCGeoPolygon.load(self.get_fullpath('nyc_community_districts.geojson'))
        self.district_counter = defaultdict(Counter)

    def get_fullpath(self, filename):
        return os.path.join(self.cwd, filename)

    def map(self, lineno, line):
        print len(line.split(','))
        vendor_id, pickup_datetime, dropoff_datetime, store_and_fwd, ratecode_id, \
        pickup_long, pickup_lat, dropoff_long, dropoff_lat, passenger_count, \
        trip_distance, fare_amount, extra, mta_tax, tip_amount, tolls_amount,\
        ehail_fee, improv_surcharge, total_amount, payment_type, trip_type = line.split(',')

        vendor_id,tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,trip_distance,pickup_longitude,pickup_latitude,rate_code_id,store_and_fwd_flag,dropoff_longitude,dropoff_latitude,payment_type,fare_amount,extra,mta_tax,tip_amount,tolls_amount,improvement_surcharge,total_amount

        pickup_long, pickup_lat = float(pickup_long), float(pickup_lat)
        pickup_borough, pickup_district = None, None
        for borough in self.boroughs:
            if (pickup_long, pickup_lat) in borough:
                pickup_borough = borough.index
                break
        for district in self.districts:
            if (pickup_long, pickup_lat) in district:
                pickup_district = district.index
                self.district_counter['pickup'][district.index] += 1
                break
        if pickup_borough is None or pickup_district is None:
            sys.stderr.write("warning: pickup(long=%s, lat=%s) not found." % (pickup_long, pickup_lat))
            return None

        dropoff_long, dropoff_lat = float(dropoff_long), float(dropoff_lat)
        dropoff_borough, dropoff_district = None, None
        for borough in self.boroughs:
            if (dropoff_long, dropoff_lat) in borough:
                dropoff_borough = borough.index
                break
        for district in self.districts:
            if (dropoff_long, dropoff_lat) in district:
                dropoff_district = district.index
                self.district_counter['dropoff'][district.index] += 1
                break
        if dropoff_borough is None or dropoff_district is None:
            sys.stderr.write("warning: dropoff(long=%s, lat=%s) not found." % (dropoff_long, dropoff_lat))
            return None

        return (vendor_id, pickup_datetime, dropoff_datetime, store_and_fwd, ratecode_id, \
            pickup_long, pickup_lat, dropoff_long, dropoff_lat, passenger_count, \
            trip_distance, fare_amount, extra, mta_tax, tip_amount, tolls_amount,\
            improv_surcharge, total_amount, payment_type, trip_type, \
            pickup_borough, pickup_district, dropoff_borough, dropoff_district)

    def reduce(self, counters):
        for counter in counters:
            self.district_counter['pickup'] += counter['pickup']
            self.district_counter['dropoff'] += counter['dropoff']

    def do_map(self):
        for line in fileinput.input('-'):
            if fileinput.isfirstline(): continue
            line = line.strip()
            if not line: continue # skip empty line
            self.map(fileinput.lineno(), line)
        sys.stdout.write(repr(self.district_counter))

    def do_reduce(self):
        counters = []
        for line in fileinput.input('-'):
            line = line.strip()
            line = line.replace("<class 'collections.Counter'>", 'Counter')
            counters.append(eval(line))
        self.reduce(counters)
        sys.stdout.write(repr(self.district_counter))

    def run(self):
        if self.opts.action == 'map':
            self.do_map()
        elif self.opts.action == 'reduce':
            self.do_reduce()

if __name__ == '__main__':
    opts = parse_argv()
    p = Pipeline(opts)
    p.run()
