#!/usr/bin/env python

# Copyright 2017, Nan Dun <nan.dun@acm.org>
# All rights reserved.

# Tasks Management and Queuing

import logging
import sys
import time

import boto3
import botocore

from raw2aws import RawReader

logging.basicConfig()

class Task:
    def __init__(self, color, year, month, start, end,
            timeout=3600, sqs_id=None, sqs_handle=None):
        self.color = color
        self.year = year
        self.month = month
        self.start = start
        self.end = end
        self.timeout = timeout  # If not succeeded in 3600 seconds, expires
        self.status  = None

        # for task retry
        self.sqs_id = sqs_id          # SQS message ID
        self.sqs_handle = sqs_handle  # SQS message handle

    def encode(self):
        return self.__str__()

    @classmethod
    def decode(cls, message):
        color, year, month, start, end, timeout = message.body.split(',')

        return Task(color, int(year), int(month), int(start), int(end),
            int(timeout), message.message_id, message.receipt_handle)

    def __repr__(self):
        return "%(color)s:%(year)s:%(month)s:[%(start)d,%(end)d):%(timeout)d" % \
            (self.__dict__)

    def __str__(self):
        return "%(color)s,%(year)d,%(month)d,%(start)d,%(end)d,%(timeout)d" % \
            (self.__dict__)

class TaskManager:
    MAX_RECORD_LENGTH = RawReader.MAX_RECORD_LENGTH
    DEFAULT_QUEUE = 'https://sqs.us-west-2.amazonaws.com/026979347307/taxi'

    def __init__(self, url=None):
        self.sqs = boto3.resource('sqs')
        if url is None: url = self.DEFAULT_QUEUE
        self.queue = self.sqs.Queue(url)

        self.s3 = boto3.resource('s3')
        self.bucket = self.s3.Bucket('aws-nyc-taxi-data')

        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.DEBUG)

    def fatal(self, message):
        self.logger.critical(message)
        sys.exit(1)

    @classmethod
    def cut(cls, start, end, N, nth=None):
        """Cut a range [start, end] into N parts [nth, nth+1)"""
        parts = range(start, end + 1, (end - start) / N)
        parts[-1] = end + 1
        if nth is None: return [[parts[i], parts[i+1]] for i in range(N)]
        return [parts[nth], parts[nth+1]]

    def create_tasks(self, color, year, month, n_tasks):
        try:
            self.s3.meta.client.head_bucket(Bucket=self.bucket.name)
        except botocore.exceptions.ClientError as e:
            error_code = int(e.response['Error']['Code'])
            if error_code == 404:
                self.fatal('s3://%s does not exists' % self.bucket.name)

        key = '%s-%s-%02d.csv' % (color, year, month)
        obj = self.bucket.Object(key)
        n_records = obj.content_length / self.MAX_RECORD_LENGTH

        self.logger.debug('create tasks for s3://%s/%s (%d)' % \
            (self.bucket.name, key, n_records))

        for r in self.cut(0, n_records, n_tasks):
            task = Task(color, year, month, r[0], r[1])
            self.logger.debug('%r => create' % task)
            self.queue.send_message(MessageBody=task.encode())

    def retrieve_task(self, hold=True):
        """Retrieve one task"""
        try:
            message = self.queue.receive_messages(
                MaxNumberOfMessages=1, WaitTimeSeconds=1)[0]
        except IndexError as e:
            self.logger.debug("no more task")
            return None

        task = Task.decode(message)
        self.logger.debug('%r => retreive' % task)

        # change task visiblity in case of failure and retry
        if hold:
            self.logger.debug('%r (%s) => hold' % (task, task.sqs_id))
            message.change_visibility(VisibilityTimeout=task.timeout)
        else:
            self.logger.debug('%r (%s) => delete' % (task, task.sqs_id))
            message.delete()

        return task

    def delete_task(self, task):
        self.logger.debug('%r (%s) => delete' % (task, task.sqs_id))
        self.queue.delete_messages(
            Entries = [{'Id': task.sqs_id, 'ReceiptHandle': task.sqs_handle}])

if __name__ == '__main__':
    tm = TaskManager()
    tm.create_tasks('green', 2016, 1, 1000)
    #task = tm.retrieve_task()
    #time.sleep(10)
    #tm.delete_task(task)
