#!/usr/bin/env python

# Copyright 2017, Nan Dun <nan.dun@acm.org>
# All rights reserved.

from __future__ import print_function

import argparse
import math
import os.path
import random
import time

from bokeh.charts import Area, Line, Histogram, Bar
from bokeh.plotting import figure
#from bokeh.palettes import Viridis6 as palette
from bokeh.palettes import GnBu6 as palette
from bokeh.layouts import row, column, layout, widgetbox
from bokeh.models import ColumnDataSource, HoverTool, Div, LogColorMapper, FixedTicker, FuncTickFormatter
from bokeh.models.widgets import Dropdown, RangeSlider, RadioButtonGroup, Slider, Toggle
from bokeh.models.renderers import GlyphRenderer
from bokeh.io import curdoc
from bokeh.sampledata.autompg import autompg as df

from geo import NYCBorough,  NYCGeoPolygon
from mapred import StatDB

def parse_argv():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('-d', '--debug', action='store_true',
        default=False, help="debug mode")

    args = parser.parse_args()

    return args

class InteractivePlot:
    def __init__(self, opts):
        self.opts = opts

        # data query
        self.db = StatDB(opts)
        self.data = None
        self.last_query = {
            'timestamp': 0.0,
                'color': '',
                 'year': 0,
                'month': 0
        }

        self.boroughs = {v: k for k, v in NYCBorough.BOROUGHS.items()}

        self.districts = None
        self.districts_xs = []
        self.districts_ys = []
        self.districts_names = []

        # figure elements
        self.selects = {
            'color': 'green',
            'year': 2016,
            'month': 1,
            'type': 'pickup',
            'borough': 0
        }

    def query(self, color, year, month):
        # prevent polling too often to save cost
        if color == self.last_query['color'] and \
           year == self.last_query['year'] and \
           month == self.last_query['month']: return
        if time.time() - self.last_query['timestamp'] < 2: time.sleep(2)

        self.data = self.db.get(color, year, month)
        self.last_query['color'] = color
        self.last_query['year'] = year
        self.last_query['month'] = month
        self.last_query['timestamp'] = time.time()

    def hot_map_init(self, width=700, height=700, webgl=True):
        self.districts = NYCGeoPolygon.load_districts()

        rates = []
        for district in self.districts:
            x, y = district.xy()
            self.districts_xs.append(x)
            self.districts_ys.append(y)
            self.districts_names.append(district.name)
            rates.append(self.data.pickups[district.index]) # default uses pickups

        self.hot_map_source = ColumnDataSource(data=dict(
            x=self.districts_xs,
            y=self.districts_ys,
            name=self.districts_names,
            rate=rates,
        ))

        palette.reverse()
        color_mapper = LogColorMapper(palette=palette)

        self.hot_map = figure(webgl=webgl,
            plot_height=height, plot_width=width,
            tools='pan,wheel_zoom,box_zoom,reset,hover,save',
            x_axis_location=None, y_axis_location=None
        )
        self.hot_map.grid.grid_line_color = None

        self.hot_map.patches('x', 'y', source=self.hot_map_source,
            fill_color={'field': 'rate', 'transform': color_mapper},
            fill_alpha=0.7, line_color="white", line_width=0.5)

        hover = self.hot_map.select_one(HoverTool)
        hover.point_policy = "follow_mouse"
        hover.tooltips = [
            ("District", "@name"),
            ("Trips", "@rate"),
            ("Coordinates", "($x, $y)"),
        ]

    def hot_map_update(self, label):
        rates = []
        for district in self.districts:
            rate = 0
            borough = self.selects['borough']
            if borough == 0 or borough == district.region:
                if self.selects['type'] == 'pickup':
                    rate = self.data.pickups[district.index]
                else:
                    rate = self.data.dropoffs[district.index]
            rates.append(rate)

        self.hot_map_source.data=dict(
            x=self.districts_xs,
            y=self.districts_ys,
            name=self.districts_names,
            rate=rates,
        )

    def trip_hour_init(self, width=620, height=350, webgl=True):
        self.trip_hour = figure(webgl=webgl,
            toolbar_location=None,
            width=width, height=height, title='Hour')
        self.trip_hour_source = ColumnDataSource(data=dict(
            x=[i + 0.5 for i in range(24)],
            top=[self.data.hour[i] for i in range(24)]
        ))
        vbar = self.trip_hour.vbar(width=0.6, bottom=0, x='x', top='top',
            source=self.trip_hour_source, fill_alpha=0.7,
            line_color="white", color='#D35400')
        self.trip_hour.y_range.start = 0
        self.trip_hour.xaxis.major_tick_line_color = None
        self.trip_hour.xaxis.minor_tick_line_color = None
        self.trip_hour.xaxis.ticker=FixedTicker(ticks=range(24))

        self.trip_hour.select(dict(type=GlyphRenderer))
        self.trip_hour.add_tools(HoverTool(renderers=[vbar],
            tooltips=[("Trips", "@top")]))

    def trip_hour_update(self):
        data = [self.data.hour[i] + 1000 * random.randint(1, 5) for i in range(24)]
        average = sum(data) / 24
        self.trip_hour_source.data=dict(
            x=range(24),
            top=[self.data.hour[i] + 1000 * random.randint(1, 5) for i in range(24)]
        )
        self.trip_hour.title.text = "Hour: %d trips in average" % average

    def trip_distance_init(self, width=310, height=350, webgl=True):
        def ticker():
            labels = {0: '0~1', 1: '1~2', 2: '2~5', 3: '5~10', 4: '10~20', 5: '>20'}
            return labels[tick]

        self.trip_distance = figure(webgl=webgl,
            toolbar_location=None,
            width=width, height=height, title='Distance')
        x = range(6)
        self.trip_distance_source = ColumnDataSource(data=dict(
            x=x,
            top=[self.data.distance[i] for i in x]
        ))
        vbar = self.trip_distance.vbar(width=1, bottom=0, x='x', top='top',
            source=self.trip_distance_source, fill_alpha=0.7,
            line_color="white", color='#588c7e')
        self.trip_distance.y_range.start = 0
        self.trip_distance.xaxis.major_tick_line_color = None
        self.trip_distance.xaxis.minor_tick_line_color = None
        self.trip_distance.xaxis.formatter=FuncTickFormatter.from_py_func(ticker)

        self.trip_distance.select(dict(type=GlyphRenderer))
        self.trip_distance.add_tools(HoverTool(renderers=[vbar],
            tooltips=[("Trips", "@top")]))

    def trip_distance_update(self):
        data = [self.data.distance[i] + 100 * random.randint(1, 5) for i in range(6)]
        average = sum(data) / 6
        self.trip_distance_source.data=dict(
            x=range(6),
            top=data
        )
        self.trip_distance.title.text = "Distance: %d miles in average" % average

    def trip_fare_init(self, width=310, height=350, webgl=True):
        def ticker():
            labels = {0: '0~5', 1: '5~10', 2: '10~25', 3: '25~50', 4: '50~100', 5: '>100'}
            return labels[tick]

        self.trip_fare = figure(webgl=webgl,
            toolbar_location=None,
            width=width, height=height, title='fare')
        x = range(6)
        self.trip_fare_source = ColumnDataSource(data=dict(
            x=x,
            top=[self.data.fare[i] for i in x]
        ))
        vbar = self.trip_fare.vbar(width=1, bottom=0, x='x', top='top',
            source=self.trip_fare_source, fill_alpha=0.7,
            line_color="white", color='#ffcc5c')
        self.trip_fare.y_range.start = 0
        self.trip_fare.xaxis.major_tick_line_color = None
        self.trip_fare.xaxis.minor_tick_line_color = None
        #self.trip_fare.xaxis.major_label_orientation = math.pi/4
        self.trip_fare.xaxis.formatter=FuncTickFormatter.from_py_func(ticker)

        self.trip_fare.select(dict(type=GlyphRenderer))
        self.trip_fare.add_tools(HoverTool(renderers=[vbar],
            tooltips=[("Trips", "@top")]))

    def trip_fare_update(self):
        data = [self.data.fare[i] + 100 * random.randint(1, 5) for i in range(6)]
        average = sum(data) / 6
        self.trip_fare_source.data=dict(
            x=range(6),
            top=data
        )
        self.trip_fare.title.text = "Fare: $%d in average" % average

    def plot(self):
        def update():
            self.query('green', 2016, 1)

            if pickup_or_dropoff.active:
                self.selects['type'] = 'pickup'
                pickup_or_dropoff.label = 'Pickups'
            else:
                self.selects['type'] = 'dropoff'
                pickup_or_dropoff.label = 'Dropoffs'

            self.selects['borough'] = self.boroughs[borough.value]

            self.hot_map_update(pickup_or_dropoff.label)
            self.trip_hour_update()
            self.trip_distance_update()
            self.trip_fare_update()

            min_year, max_year = year_range.range
            if min_year == max_year:
                self.hot_map.title.text = "%s %d, %s" % \
                    (pickup_or_dropoff.label, min_year, borough.value)
            else:
                self.hot_map.title.text = "%s, %d - %d, %s" % \
                    (pickup_or_dropoff.label, min_year, max_year, borough.value)

            borough.label = borough.value

        cwd = os.path.dirname(__file__)
        desc = Div(text=open(
            os.path.join(cwd, "description.html")).read(), width=1000)

        # Create input controls
        taxi_type = RadioButtonGroup(labels=["Yellow", "Green"], active=0)
        taxi_type.on_change('active', lambda attr, old, new: update())

        pickup_or_dropoff = Toggle(label="Pickup", button_type="primary")
        pickup_or_dropoff.on_change('active', lambda attr, old, new: update())

        # BUG: Dropdown menu value cannot be integer, i.e., ('Mahattan', '1')
        borough_menu = [('All Boroughs', 'All Boroughs'), None,
            ('Manhattan', 'Manhattan'), ('Bronx', 'Bronx'), ('Brooklyn', 'Brooklyn'),
            ('Queens', 'Queens'), ('Staten Island', 'Staten Island')]
        # https://github.com/bokeh/bokeh/issues/4915
        borough = Dropdown(label="Boroughs", button_type="warning",
            menu=borough_menu, value='All Boroughs')
        borough.on_change('value', lambda attr, old, new: update())

        year_range = RangeSlider(title="Year", start=2009, end=2016, step=1, range=(2009, 2009))
        year_range.on_change('range', lambda attr, old, new: update())

        month_range = RangeSlider(title="Month", start=1, end=12, step=1, range=(1, 12))
        month_range.on_change('range', lambda attr, old, new: update())

        emr_size = Slider(title="Nodes", start=1, end=10, value=1, step=1)
        emr_size.on_change('value', lambda attr, old, new: update())

        controls = [taxi_type, pickup_or_dropoff, borough, year_range, month_range, emr_size]

        # Pickup/Dropoff Map
        self.query('green', 2016, 1)

        self.hot_map_init()
        self.trip_hour_init()
        self.trip_distance_init()
        self.trip_fare_init()

        usage_data = dict(
            CPU=[2, 3, 7, 5, 26, 221, 44, 233, 254, 265, 266, 267, 120, 111],
            MEM=[12, 33, 47, 15, 126, 121, 144, 233, 254, 225, 226, 267, 110, 130],
        )
        resource_usage = Line(usage_data, plot_width=1500, plot_height=120,
            toolbar_location=None, color=["firebrick", "seagreen"],
            title=None, xlabel="Time", ylabel="%", legend='top_right')
        resource_usage.xgrid.visible = False
        resource_usage.ygrid.visible = False
        resource_usage.x_range.start = 0
        resource_usage.y_range.start = 0

        rightdown_row = row([self.trip_distance, self.trip_fare])
        right_column = column([self.trip_hour, rightdown_row])
        inputs = widgetbox(*controls, width=140, sizing_mode="fixed")
        l = layout([
            [desc],
            [inputs, self.hot_map, right_column],
            [resource_usage],
        ], sizing_mode="fixed")

        update()  # initial load of the data

        curdoc().add_root(l)
        curdoc().title = "NYC Taxi Data Explorer"

if __name__ == "__main__":
    print("usage: bokeh serve --show %s --args [ARGS]" % os.path.dirname(__file__))
else:
    p = InteractivePlot(parse_argv())
    p.plot()
