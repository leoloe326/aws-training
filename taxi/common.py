#!/usr/bin/env python

# Copyright 2017, Nan Dun <nan.dun@acm.org>
# All rights reserved.

from __future__ import print_function

import argparse
import logging
import sys
import ConfigParser

__all__ = ['RECORD_LENGTH', 'fatal', 'error', 'Options']

RECORD_LENGTH = 80

def fatal(message):
    sys.stderr.write('fatal: %s\n' % message)
    sys.stderr.flush()
    sys.exit(1)

def error(message):
    sys.stderr.write('error: %s\n' % message)
    sys.stderr.flush()

class Options:
    def __init__(self):
        self.parser = argparse.ArgumentParser(
            formatter_class=argparse.ArgumentDefaultsHelpFormatter)

        self.parser.add_argument('-c', '--color', metavar='yellow|green',
            type=str, default='green', help="color of record")

        self.parser.add_argument('-y', '--year', metavar='YEAR',
            type=int, default=2016, help="year of record")

        self.parser.add_argument('-m', '--month', metavar='MONTH',
            type=int, default=1, help="month of record")

        self.parser.add_argument('-d', '--debug', action='store_true',
            default=False, help="debug mode")

        self.parser.add_argument('--config', type=str,
            default='config.ini', help="configuration file")

        class VAction(argparse.Action):
            def __call__(self, parser, args, values, option_string=None):
                if values is None: values = logging.INFO
                try:
                    values = int(values)
                except ValueError:
                    values = logging.INFO - values.count('v') * 10
                setattr(args, self.dest, values)

        self.parser.add_argument('-v', nargs='?', action=VAction,
            metavar='vv..|NUM',
            dest='verbose', default=logging.WARNING, help='verbose level')

        self.parser.add_argument('--dryrun', action='store_true',
            default=False, help="dryrun")

    def add(self, *args, **kwargs):
        self.parser.add_argument(*args, **kwargs)

    def load(self):
        self.opts = self.parser.parse_args()
        self. _validate()

        # load configurations
        p = ConfigParser.SafeConfigParser()
        p.read(self.opts.config)
        profile = 'debug' if self.opts.debug else 'default'
        for name, value in p.items(profile):
            setattr(self.opts, name, value)

        return self.opts

    def _validate(self):
        if self.opts.color not in ['yellow', 'green']:
            fatal('unknown color: %s' % self.opts.color)

    def __str__(self):
        return '\n'.join(['%16s: %s' % (attr, value) for attr, value in
                self.opts.__dict__.iteritems()])

    @classmethod
    def parse_argv(cls):
        return Options().load()

if __name__ == '__main__':
    o = Options()
    o.load()
    print(o)
