#!/usr/bin/env python

# Copyright 2017, Nan Dun <nan.dun@acm.org>
# All rights reserved.

# Geo Location Processing

import json
import shapely.geometry
from collections import OrderedDict

NYC_BOROUGHS = OrderedDict({
    1: 'Manhattan',
    2: 'Bronx',
    3: 'Brooklyn',
    4: 'Queens',
    5: 'Staten Island'
})

class NYCGeoPolygon:
    def __init__(self, index, name, polygon):
        self.index = index
        self.name = name
        self.polygon = shapely.geometry.shape(polygon)

    def __contains__(self, point):
        return self.polygon.contains(shapely.geometry.Point(point))

    def __str__(self):
        return '{index}: {name}'.format(**self.__dict__)

    def xy(self):
        x, y = self.polygon.exterior.coords.xy
        return list(x), list(y)

    @classmethod
    def load(cls, filename):
        polygons = []
        with open(filename, 'r') as f:
            for feature in json.load(f)['features']:
                properties = feature['properties']
                if 'boro_name' in properties:
                    # Boroughs
                    name = properties['boro_name']
                    index = int(properties['boro_code']) * 10000
                else:
                    # Community districts
                    name = 'Community District %s' % properties['boro_cd']
                    index = int(properties['boro_cd']) * 100

                # Flatten multipolygon
                # see structure at http://terraformer.io/glossary/
                geometry = feature['geometry']
                if geometry['type'].lower() == 'polygon':
                    raise NotImplementedError
                # print '%s has %d patches' % (name, len(geometry['coordinates']))
                for i, coords in enumerate(geometry['coordinates']):
                    polygon = {'type': 'Polygon', 'coordinates': coords}
                    polygons.append(NYCGeoPolygon(index + i + 1, name, polygon))

        polygons.sort(key=lambda x: x.index)
        return polygons
