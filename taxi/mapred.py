#!/usr/bin/env python

# Copyright 2017, Nan Dun <nan.dun@acm.org>
# All rights reserved.

import argparse
import datetime
import sys
import os.path
import io
import fileinput

import boto3

from collections import defaultdict
from collections import Counter

from geo import NYCGeoPolygon
from raw2aws import RawReader, fatal, warning, info

def parse_argv():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('--src', metavar='URI', type=str,
        default='s3://aws-nyc-taxi-data', help="data source directory")

    parser.add_argument('-s', '--start',  metavar='NUM', type=int,
        default=0, help="start record index")

    parser.add_argument('-e', '--end',  metavar='NUM', type=int,
        default=sys.maxint, help="end record index")

    parser.add_argument('-m', "--map", dest='action', action='store_const',
        const='map', default=None, help="map phase")

    parser.add_argument('-r', "--reduce", dest='action', action='store_const',
        const='reduce', default=None, help="reduce phase")

    args = parser.parse_args()
    return args

class RecordReader(io.IOBase):
    START_DATE = RawReader.START_DATE
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

    def open(self, color, year, month, source, start=0, end=sys.maxint):
        self.color = color
        self.year = year
        self.month = month
        self.source = source
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
                fatal("%s does not exist." % path)
            if not os.path.isfile(path):
                fatal("%s is not a regular file." % path)

            self.n_records = os.path.getsize(path) / self.MAX_RECORD_LENGTH

            if self.start < 0 or self.start > self.end:
                fatal("invalid range [%d, %d] for file://%s (records=%d)" %\
                    (self.start, self.end, path, self.n_records))

            self.end = min(self.n_records, end)
            self.data = open(path, 'r')
            self.data.seek(self.MAX_RECORD_LENGTH * self.start)
            info(" read: file://%s [%d, %d]" % (path, self.start, self.end))

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
            bytes_range = 'bytes=%d-%d' % \
                (self.start * self.MAX_RECORD_LENGTH, \
                 self.end * self.MAX_RECORD_LENGTH - 1)
            self.data = obj.get(Range=bytes_range)['Body']

    def readline(self):
        if self.data_type == self.DATA_S3:
            # HOWTO: fixed length makes read very easy
            return self.data.read(self.MAX_RECORD_LENGTH)
        return self.data.readline()

    def readlines(self):
        def delta_time(seconds):
            return self.START_DATE + datetime.timedelta(seconds=seconds)

        start = self.start
        skip = 0
        while start < self.end:
            line = self.readline()
            if skip < self.skip: skip += 1; continue # for stdin read
            start += 1
            if not line: break

            pickup_datetime, dropoff_datetime, \
            pickup_longitude, pickup_latitude, \
            dropoff_longitude, dropoff_latitude, \
            trip_distance, fare_amount = line.strip().split(',')[:-1] # drop padding field

            pickup_datetime = delta_time(int(pickup_datetime))
            dropoff_datetime = delta_time(int(dropoff_datetime))
            pickup_longitude = float(pickup_longitude)
            pickup_latitude = float(pickup_latitude)
            dropoff_longitude = float(dropoff_longitude)
            dropoff_latitude = float(dropoff_latitude)
            trip_distance = float(trip_distance)
            fare_amount = float(fare_amount)

            yield (pickup_datetime, dropoff_datetime, \
            pickup_longitude, pickup_latitude, \
            dropoff_longitude, dropoff_latitude, \
            trip_distance, fare_amount)

    def close(self):
        self.data.close()

class NYCTaxiStat:
    def __init__(self, opts):
        self.opts = opts
        self.cwd = os.path.dirname(__file__)
        self.reader = RecordReader()

        # Load Boroughs and Community Districts information
        # self.boroughs = NYCGeoPolygon.load(self.get_fullpath('nyc_boroughs.geojson'))
        self.districts = NYCGeoPolygon.load(self.get_fullpath('nyc_community_districts.geojson'))

        # Statistics
        self.district_counter = defaultdict(Counter)

    def get_fullpath(self, filename):
        return os.path.join(self.cwd, filename)

    def run(self):
        self.reader.open('green', 2016, 01, \
            self.opts.src, self.opts.start, self.opts.end)
        for line in self.reader.readlines():
            print line
        self.reader.close()

if __name__ == '__main__':
    opts = parse_argv()
    p = NYCTaxiStat(opts)
    p.run()
