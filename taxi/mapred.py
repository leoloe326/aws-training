#!/usr/bin/env python

# Copyright 2017, Nan Dun <nan.dun@acm.org>
# All rights reserved.

from __future__ import print_function

import argparse
import copy
import datetime
import decimal
import fileinput
import io
import json
import logging
import multiprocessing
import os.path
import sys
import time

import boto3
import botocore

from collections import Counter
from boto3.dynamodb.conditions import Key, Attr

from geo import NYCBorough, NYCGeoPolygon
from raw2aws import MIN_DATE, MAX_DATE, RawReader, fatal, warning, info
from tasks import TaskManager

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

    parser.add_argument('-p', '--procs', type=int, dest='nprocs',
        default=1, help="number of concurrent processes")

    parser.add_argument('-i', '--proc-idx', type=int,
        dest='proc_idx', default=0, help=argparse.SUPPRESS)

    parser.add_argument('-w', '--worker', action='store_true',
        default=False, help="worker mode")

    parser.add_argument('--sleep', type=int,
        default=10, help="worker sleep time if no task")

    parser.add_argument('-d', '--debug', action='store_true',
        default=False, help="debug mode")

    parser.add_argument('-v', '--verbose', type=int,
        default=logging.NOTSET, help='verbose level')

    args = parser.parse_args()

    # check arguments
    if args.color != 'yellow' and args.color != 'green':
        fatal('unknown color: %s' % args.color)

    data_date = datetime.datetime(args.year, args.month, 1)
    if not (data_date >= MIN_DATE[args.color] and \
            data_date <= MAX_DATE[args.color]):
        fatal('date range must be from %s to %s for %s data' % \
            (MIN_DATE[args.color].strftime('%Y-%m'),
             MAX_DATE[args.color].strftime('%Y-%m'),
             args.color))

    logging.basicConfig()

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
            self.start, self.end = TaskManager.cut(self.start, self.end, nParts, nth)
            info("proc %02d read: %s [%d, %d)" % (nth, path, self.start, self.end))

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
                    raise OSError("%s does not exists" % self.opts.dst)

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

class StatDB:
    def __init__(self, opts):
        self.opts = opts
        self.ddb = boto3.resource('dynamodb',
            region_name='us-west-2', endpoint_url="http://localhost:8000")
        self.table = self.ddb.Table('taxi')
        try:
            assert self.table.table_status == 'ACTIVE'
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                warning("table %s does not exist" % self.table.table_name)
            if opts.debug: self.create_table()

        # if self.opts.debug: self.table.delete(); self.create_table()

    def create_table(self):
        self.table = self.ddb.create_table(
            TableName='taxi',
            KeySchema=[
                {
                    'AttributeName': 'color',
                    'KeyType': 'HASH'   # partition key
                },
                {
                    'AttributeName': 'date',
                    'KeyType': 'RANGE'  # sort key
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'color',
                    'AttributeType': 'S'
                },
                {
                    'AttributeName': 'date',
                    'AttributeType': 'N'
                },

            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 10,
                'WriteCapacityUnits': 10
            }
        )

    def append(self, stat):
        def add_values(counter, prefix):
            for key, count in counter.items():
                values[':%s%s' % (prefix, key)] = count

        values = {}

        # use one letter to save bytes, thus write/read units
        # must not overlap with 'color' and 'date'
        values[':l'] = stat.total
        values[':i'] = stat.invalid
        add_values(stat.pickups,   'p')
        add_values(stat.dropoffs,  'r')
        add_values(stat.hour,      'h')
        add_values(stat.trip_time, 't')
        add_values(stat.distance,  's')
        add_values(stat.fare,      'f')
        add_values(stat.borough_pickups,  'k')
        add_values(stat.borough_dropoffs, 'o')

        # HOWTO: contrurct update expression
        expr = ','.join([k[1:] + k for k in values.keys()])

        self.table.update_item(
            Key={'color': stat.color, 'date': stat.year * 100 + stat.month},
            UpdateExpression='add ' + expr,
            ExpressionAttributeValues=values
        )

    def get(self, color, year, month):
        def add_stat(counter, prefix):
            for key, val in values.items():
                if key.startswith(prefix):
                    counter[int(key[1:])] = int(val)

        try:
            response = self.table.get_item(
                Key={
                    'color': color,
                    'date': year * 100 + month
                }
            )
        except botocore.exceptions.ClientError as e:
            print(e.response['Error']['Message'])
            return None

        values = response['Item']
        stat = TaxiStat(color, year, month)

        stat.total = values['l']
        stat.invalid = values['i']
        add_stat(stat.pickups,   'p')
        add_stat(stat.dropoffs,  'r')
        add_stat(stat.hour,      'h')
        add_stat(stat.trip_time, 't')
        add_stat(stat.distance,  's')
        add_stat(stat.fare,      'f')
        add_stat(stat.borough_pickups,  'k')
        add_stat(stat.borough_dropoffs, 'o')

        return stat

class TaxiStat(object):
    def __init__(self, color=None, year=0, month=0):
        self.color = color
        self.year = year
        self.month = month
        self.total = 0                      # number of total records
        self.invalid = 0                    # number of invalid records
        self.pickups = Counter()            # district -> # of pickups
        self.dropoffs = Counter()           # district -> # of dropoffs
        self.hour = Counter()               # pickup hour distriibution
        self.trip_time = Counter()          # trip time distribution
        self.distance = Counter()           # distance distribution
        self.fare = Counter()               # fare distribution
        self.borough_pickups  = Counter()   # borough -> # of pickups
        self.borough_dropoffs = Counter()   # borough -> # of dropoffs

class NYCTaxiStat(TaxiStat):
    START_DATE = RawReader.START_DATE
    def __init__(self, opts):
        super(NYCTaxiStat, self).__init__(opts.color, opts.year, opts.month)
        self.opts = opts
        self.reader = RecordReader()
        self.elapsed = 0
        self.districts = NYCGeoPolygon.load_districts()

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
        trip_time = dropoff_datetime - pickup_datetime
        pickup_hour = delta_time(pickup_datetime).hour
        # We don't need dropoff time
        # dropoff_datetime = delta_time(dropoff_datetime)

        pickup_longitude = float(pickup_longitude)
        pickup_latitude = float(pickup_latitude)
        dropoff_longitude = float(dropoff_longitude)
        dropoff_latitude = float(dropoff_latitude)
        trip_distance = float(trip_distance)
        fare_amount = float(fare_amount)

        pickup_district, dropoff_district = None, None

        # Note: district in particular order, see geo.py
        for district in self.districts:
            if pickup_district is None and \
               (pickup_longitude, pickup_latitude) in district:
                pickup_district = district.index
            if dropoff_district is None and \
               (dropoff_longitude, dropoff_latitude) in district:
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
        self.hour[pickup_hour] += 1

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
        title = " NYC %s Cab, %s " %\
            (self.opts.color.capitalize(), report_date.strftime('%B %Y'))
        print(title.center(width, '='))

        format_str = "%14s: %16s %16s"
        print(format_str % ('Borough', 'Pickups', 'Dropoffs'))
        for index, name in NYCBorough.BOROUGHS.items():
            print(format_str % (name,
                                self.borough_pickups[index],
                                self.borough_dropoffs[index]))

        print(" Pickup Time ".center(width, '-'))
        format_str = "%14s: %33s"
        for hour in range(24):
            if hour in self.hour:
                hour_str = '%d:00 ~ %d:59' % (hour, hour)
                print(format_str % (hour_str, self.hour[hour]))

        print(" Trip Distance (miles) ".center(width, '-'))
        format_str = "%14s: %33s"
        print(format_str % ('0 ~ 1',   self.distance[0]))
        print(format_str % ('1 ~ 2',   self.distance[1]))
        print(format_str % ('2 ~ 5',   self.distance[2]))
        print(format_str % ('5 ~ 10',  self.distance[5]))
        print(format_str % ('10 ~ 20', self.distance[10]))
        print(format_str % ('> 20',    self.distance[20]))

        print(" Trip Time (minutes) ".center(width, '-'))
        format_str = "%14s: %33s"
        print(format_str % ('0 ~ 5',   self.trip_time[0]))
        print(format_str % ('5 ~ 10',  self.trip_time[300]))
        print(format_str % ('10 ~ 15', self.trip_time[600]))
        print(format_str % ('15 ~ 30', self.trip_time[900]))
        print(format_str % ('30 ~ 45', self.trip_time[1800]))
        print(format_str % ('45 ~ 60', self.trip_time[2700]))
        print(format_str % ('> 60',    self.trip_time[3600]))

        print(" Fare (dollars) ".center(width, '-'))
        format_str = "%14s: %33s"
        print(format_str % ('0 ~ 5',    self.fare[0]))
        print(format_str % ('5 ~ 10',   self.fare[5]))
        print(format_str % ('10 ~ 25',  self.fare[10]))
        print(format_str % ('25 ~ 50',  self.fare[25]))
        print(format_str % ('50 ~ 100', self.fare[50]))
        print(format_str % ('> 100',    self.fare[100]))

        print(''.center(width, '='))
        print("Done, took %.2f seconds using %d processes." %\
            (self.elapsed, self.opts.nprocs))

    def run(self):
        self.elapsed = time.time()

        try:
            with self.reader.open(\
                self.opts.color, self.opts.year, self.opts.month, \
                self.opts.src, self.opts.start, self.opts.end, \
                self.opts.nprocs, self.opts.proc_idx) as fin:
                for line in fin.readlines(): self.search(line)
        except KeyboardInterrupt as e:
            return

        # aggregate boroughs' pickups and dropoffs
        for index, count in self.pickups.items():
            self.borough_pickups[index/10000] += count
        for index, count in self.dropoffs.items():
            self.borough_dropoffs[index/10000] += count

        self.elapsed = time.time() - self.elapsed

def start_process(opts):
    p = NYCTaxiStat(opts)
    p.run()
    return p

def start_multiprocess(opts):
    db = StatDB(opts)

    tasks = []
    for i in range(opts.nprocs):
        opts_copy = copy.deepcopy(opts)
        opts_copy.proc_idx = i
        tasks.append(opts_copy)

    try:
        procs = multiprocessing.Pool(processes=opts.nprocs)
        results = procs.map(start_process, tasks)
    except Exception as e:
        fatal(e)

    master = results[0]
    for res in results[1:]: master += res
    db.append(master)

    if opts.report: master.report()

    return True

def start_worker(opts):
    task_manager = TaskManager()
    logger = logging.getLogger(NYCTaxiStat.__name__)
    logger.setLevel(opts.verbose)

    opts.nprocs = multiprocessing.cpu_count()

    while True:
        task = task_manager.retrieve_task()
        if task:
            logger.info('get task %r' % task)
            opts.color = task.color
            opts.year = task.year
            opts.month = task.month
            opts.start = task.start
            opts.end = task.end
            if start_multiprocess(opts):
                logger.info("task %r succeeded" % task)
                task_manager.delete_task(task)
        else:
            logger.info("no task, wait for 10 seconds...")
            time.sleep(10)

def main(opts):
    if opts.worker: start_worker(opts)
    else: start_multiprocess(opts)

if __name__ == '__main__':
    main(parse_argv())
