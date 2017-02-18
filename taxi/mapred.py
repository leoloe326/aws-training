#!/usr/bin/env python

# Copyright 2017, Nan Dun <nan.dun@acm.org>
# All rights reserved.

import argparse
import datetime
import sys
import os.path
import time
import io
import fileinput
import multiprocessing

import boto3

from collections import defaultdict
from collections import Counter

from geo import NYC_BOROUGHS, NYCGeoPolygon
from raw2aws import RawReader, fatal, warning, info

NYC_DISTRICTS_JSON = 'nyc_community_districts.geojson'
NYC_BOROUGHS_JSON = 'nyc_boroughs.geojson'

def parse_argv():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('--src', metavar='URI', type=str,
        default='s3://aws-nyc-taxi-data', help="data source directory")

    parser.add_argument('-c', '--color',  metavar='yellow|green',
        type=str, default='green', help="color of record")

    parser.add_argument('-y', '--year',  metavar='YEAR',
        type=int, default=2016, help="year of record")

    parser.add_argument('-m', '--month',  metavar='MONTH',
        type=int, default=1, help="month of record")

    parser.add_argument('-s', '--start',  metavar='NUM', type=int,
        default=0, help="start record index")

    parser.add_argument('-e', '--end',  metavar='NUM', type=int,
        default=sys.maxint, help="end record index")

    parser.add_argument('-r', '--report', action='store_true',
        default=False, help="report results")

    parser.add_argument('-p', '--procs', type=int,
        default=1, help="number of concurrent processes")

    parser.add_argument('-n', '--n-records', dest='action', action='store_const',
        const='n_records', default=None, help="show the number of records")

    args = parser.parse_args()
    return args

class RecordReader(io.IOBase):
    MAX_RECORD_LENGTH = RawReader.MAX_RECORD_LENGTH
    DATA_STDIN = 1
    DATA_FILE = 2
    DATA_S3 = 3

    def __init__(self):
        self.data = None
        self.n_records = 0
        self.start = 0
        self.end = 0
        self.data_type = -1

        self.s3 = boto3.resource('s3')
        self.client = boto3.client('s3')

    def open(self, color, year, month, source,
             start=0, end=sys.maxint, nParts=1, nth=0):

        def create_range(path):
            if self.start < 0 or self.start > self.end:
                raise ValueError("invalid range [%d, %d] for %s (records=%d)" %\
                    (self.start, self.end, path, self.n_records))

            self.end = min(self.n_records, end)
            r = range(self.start, self.end+1, (self.end - self.start) / nParts)
            self.start, self.end = r[nth], r[nth+1]
            info("proc %02d read: %s [%d, %d]" % (nth, path, self.start, self.end))

        self.start = start
        self.end = end
        self.skip = None

        if source == '-':
            self.data_type = self.DATA_STDIN
            if self.start < 0 or self.start > self.end:
                fatal("invalid range [%d, %d] for stdin" % (self.start, self.end))

            self.skip = self.start
            self.data = fileinput.input('-')

        elif source.startswith('file://'):
            self.data_type = self.DATA_FILE
            self.data_type = 'file'
            directory = os.path.realpath(source[7:])
            if not os.path.isdir(directory):
                fatal("%s is not a directory." % directory)

            path = '%s/%s-%s-%02d.csv' % (directory, color, year, month)
            if not os.path.exists(path):
                raise OSError("%s does not exist." % path)
            if not os.path.isfile(path):
                raise OSError("%s is not a regular file." % path)

            self.n_records = os.path.getsize(path) / self.MAX_RECORD_LENGTH
            create_range('file://' + path)

            self.data = open(path, 'r')
            self.data.seek(self.MAX_RECORD_LENGTH * self.start)

        elif source.startswith('s3://'):
            self.data_type = self.DATA_S3
            bucket = self.s3.Bucket(source[5:])

            try:
                self.s3.meta.client.head_bucket(Bucket=bucket.name)
            except botocore.exceptions.ClientError as e:
                error_code = int(e.response['Error']['Code'])
                if error_code == 404:
                    fatal("%s does not exists" % self.opts.dst)

            # HOWTO: read object by range
            key = '%s-%s-%02d.csv' % (color, year, month)
            obj = bucket.Object(key)

            self.n_records = obj.content_length / self.MAX_RECORD_LENGTH
            create_range('s3://' + key)

            bytes_range = 'bytes=%d-%d' % \
                (self.start * self.MAX_RECORD_LENGTH, \
                 self.end * self.MAX_RECORD_LENGTH - 1)
            self.data = obj.get(Range=bytes_range)['Body']

        return self

    def readline(self):
        if self.data_type == self.DATA_S3:
            # HOWTO: fixed length makes read very easy
            return self.data.read(self.MAX_RECORD_LENGTH)
        return self.data.readline()

    def readlines(self):
        start = self.start
        skip = 0
        while start < self.end:
            line = self.readline()
            if skip < self.skip: skip += 1; continue # for stdin read
            start += 1
            if not line: break
            yield line

    def close(self):
        self.data.close()

class NYCTaxiStat:
    START_DATE = RawReader.START_DATE

    def __init__(self, opts, proc=0):
        self.opts = opts
        self.cwd = os.path.dirname(__file__)
        self.reader = RecordReader()
        self.proc = proc
        self.elapsed = 0

        # Load Boroughs and Community Districts information
        # self.boroughs = NYCGeoPolygon.load(self.get_fullpath('nyc_boroughs.geojson'))
        self.districts = NYCGeoPolygon.load(os.path.join(self.cwd, NYC_DISTRICTS_JSON))

        self.total = 0    # number of total records
        self.invalid = 0  # number of invalid records
        self.pickups = Counter()    # district -> # of pickups
        self.dropoffs = Counter()   # district -> # of dropoffs
        self.hour = Counter()       # pickup hour distriibution
        self.trip_time = Counter()  # trip time distribution
        self.distance = Counter()   # distance distribution
        self.fare = Counter()       # fare distribution

    def __add__(self, x):
        self.total += x.total
        self.invalid += x.invalid
        self.pickups += x.pickups
        self.dropoffs += x.dropoffs
        self.hour += x.hour
        self.trip_time += x.trip_time
        self.distance += x.distance
        self.fare += x.fare
        self.elapsed = max(self.elapsed, x.elapsed)
        return self

    def search(self, line):
        def delta_time(seconds):
            return self.START_DATE + datetime.timedelta(seconds=seconds)

        pickup_datetime, dropoff_datetime, \
        pickup_longitude, pickup_latitude, \
        dropoff_longitude, dropoff_latitude, \
        trip_distance, fare_amount, _ = line.strip().split(',')

        pickup_datetime = int(pickup_datetime)
        dropoff_datetime = int(dropoff_datetime)
        trip_time = dropoff_datetime - pickup_datetime # calculate trip time
        pickup_datetime = delta_time(pickup_datetime)
        dropoff_datetime = delta_time(dropoff_datetime)

        pickup_longitude = float(pickup_longitude)
        pickup_latitude = float(pickup_latitude)
        dropoff_longitude = float(dropoff_longitude)
        dropoff_latitude = float(dropoff_latitude)
        trip_distance = float(trip_distance)
        fare_amount = float(fare_amount)

        pickup_district, dropoff_district = None, None

        for district in self.districts:
            if (pickup_longitude, pickup_latitude) in district:
                pickup_district = district.index
            if (dropoff_longitude, dropoff_latitude) in district:
                dropoff_district = district.index
            if pickup_district and dropoff_district: break

        self.total += 1
        if pickup_district is None and dropoff_district is None:
            warning("cannot locate trip (%f, %f) => (%f, %f)" % \
                (pickup_longitude, pickup_latitude, \
                 dropoff_longitude, dropoff_latitude))
            self.invalid += 1
            return None

        if pickup_district:  self.pickups[pickup_district] += 1
        if dropoff_district: self.dropoffs[dropoff_district] += 1
        self.hour[pickup_datetime.hour] += 1

        if   trip_distance >= 20: self.distance[20] += 1
        elif trip_distance >= 10: self.distance[10] += 1
        elif trip_distance >= 5:  self.distance[5]  += 1
        elif trip_distance >= 2:  self.distance[2]  += 1
        elif trip_distance >= 1:  self.distance[1]  += 1
        else:                     self.distance[0]  += 1

        if   trip_time >= 3600:   self.trip_time[3600] += 1
        elif trip_time >= 2700:   self.trip_time[2700] += 1
        elif trip_time >= 1800:   self.trip_time[1800] += 1
        elif trip_time >= 900:    self.trip_time[900]  += 1
        elif trip_time >= 600:    self.trip_time[600]  += 1
        elif trip_time >= 300:    self.trip_time[300]  += 1
        else:                     self.trip_time[0]    += 1

        if   fare_amount >= 100:  self.fare[100] += 1
        elif fare_amount >= 50:   self.fare[50]  += 1
        elif fare_amount >= 25:   self.fare[25]  += 1
        elif fare_amount >= 10:   self.fare[10]  += 1
        elif fare_amount >= 5:    self.fare[5]   += 1
        else:                     self.fare[0]   += 1

    def report(self):
        width = 50
        report_date = datetime.datetime(self.opts.year, self.opts.month, 1)
        title = "NYC Taxi Statistics: %s Cab, %s" %\
            (self.opts.color.capitalize(), report_date.strftime('%B %Y'))
        print title.center(width, '=')

        # Aggregate Districts to Boroughs
        pickups = Counter()
        dropoffs = Counter()
        for district in self.districts:
            borough = district.index / 10000
            pickups[borough] += self.pickups[district.index]
            dropoffs[borough] += self.dropoffs[district.index]

        format_str = "%14s: %16s %16s"
        print format_str % ('Borough', 'Pickups', 'Dropoffs')
        for index, name in NYC_BOROUGHS.items():
            print format_str % (name, pickups[index], dropoffs[index])

        print "Pickup Time".center(width, '-')
        format_str = "%14s: %33s"
        #print format_str % ('Time', 'Pickups')
        for hour in range(24):
            if hour in self.hour:
                hour_str = '%d:00 ~ %d:59' % (hour, hour)
                print format_str % (hour_str, self.hour[hour])

        print "Trip Distance (miles)".center(width, '-')
        format_str = "%14s: %33s"
        #print format_str % ('Miles', 'Trips')
        print format_str % ('0 ~ 1',   self.distance[0])
        print format_str % ('1 ~ 2',   self.distance[1])
        print format_str % ('2 ~ 5',   self.distance[2])
        print format_str % ('5 ~ 10',  self.distance[5])
        print format_str % ('10 ~ 20', self.distance[10])
        print format_str % ('> 20',    self.distance[20])

        print "Trip Time (minutes)".center(width, '-')
        format_str = "%14s: %33s"
        #print format_str % ('Minutes', 'Trips')
        print format_str % ('0 ~ 5',   self.trip_time[0])
        print format_str % ('5 ~ 10',  self.trip_time[300])
        print format_str % ('10 ~ 15', self.trip_time[600])
        print format_str % ('15 ~ 30', self.trip_time[900])
        print format_str % ('30 ~ 45', self.trip_time[1800])
        print format_str % ('45 ~ 60', self.trip_time[2700])
        print format_str % ('> 60',    self.trip_time[3600])

        print "Fare (dollars)".center(width, '-')
        format_str = "%14s: %33s"
        #print format_str % ('Dollars', 'Trips')
        print format_str % ('0 ~ 5',    self.fare[0])
        print format_str % ('5 ~ 10',   self.fare[5])
        print format_str % ('10 ~ 25',  self.fare[10])
        print format_str % ('25 ~ 50',  self.fare[25])
        print format_str % ('50 ~ 100', self.fare[50])
        print format_str % ('> 100',    self.fare[100])

        print "Done, took %.2f seconds using %d processes." %\
            (self.elapsed, self.opts.procs)

    def run(self):
        self.elapsed = time.time()

        try:
            with self.reader.open(self.opts.color, self.opts.year, self.opts.month, \
                self.opts.src, self.opts.start, self.opts.end, \
                self.opts.procs, self.proc) as fin:
                for line in fin.readlines():
                    self.search(line)
        except KeyboardInterrupt as e:
            return

        self.elapsed = time.time() - self.elapsed

def start_process(args):
    opts, index = args
    p = NYCTaxiStat(opts, index)
    p.run()
    return p

if __name__ == '__main__':
    opts = parse_argv()
    tasks = []

    # map
    for i in range(opts.procs): tasks.append((opts, i))

    try:
        procs = multiprocessing.Pool(processes=opts.procs)
        results = procs.map(start_process, tasks)
    except Exception as e:
        fatal(e)

    # intermediate reduce
    master = results[0]
    for res in results[1:]: master += res
    if opts.report: master.report()
