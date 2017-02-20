#!/usr/bin/env python

# Copyright 2017, Nan Dun <nan.dun@acm.org>
# All rights reserved.

from __future__ import print_function

import os.path

import numpy
import pandas.io.sql as psql
import sqlite3

from bokeh.charts import Area, Line, Histogram
from bokeh.plotting import figure
#from bokeh.palettes import Viridis6 as palette
from bokeh.palettes import GnBu6 as palette
from bokeh.layouts import column, layout, widgetbox
from bokeh.models import ColumnDataSource, HoverTool, Div, LogColorMapper
from bokeh.models.widgets import Dropdown, RangeSlider, RadioButtonGroup, Slider, Toggle
from bokeh.io import curdoc
from bokeh.sampledata.autompg import autompg as df

from geo import NYCGeoPolygon

class InteractivePlot:
    def __init__(self):
        self.cwd = os.path.dirname(__file__)

    def get_fullpath(self, filename):
        return os.path.join(self.cwd, filename)

    def in_borough(self, borough, district):
        if borough == 'All Boroughs': return True

        borough_map = {'Manhattan' : 1, 'Bronx' : 2, 'Brooklyn' : 3,
                       'Queens' : 4, 'Staten Island' : 5}
        if district.index / 10000 == borough_map[borough]: return True
        return False

    def plot(self):
        def update():
            pickup_or_dropoff.label = 'Pickups' if pickup_or_dropoff.active else 'Dropoffs'

            xs = []
            ys = []
            names = []
            rates = []
            for i, district in enumerate(districts):
                x, y = district.xy()
                xs.append(x)
                ys.append(y)
                names.append(district.name)
                if self.in_borough(borough.value, district): rates.append(i)
                else: rates.append(0)

            source.data=dict(
                x=xs,
                y=ys,
                name=names,
                rate=rates,
            )

            min_year, max_year = year_range.range
            if min_year == max_year:
                pickups_map.title.text = "%s %d, %s" % \
                    (pickup_or_dropoff.label, min_year, borough.value)
            else:
                pickups_map.title.text = "%s, %d - %d, %s" % \
                    (pickup_or_dropoff.label, min_year, max_year, borough.value)

            trip_distance.title.text = "Average Trip Distance: 10 mile"
            trip_fare.title.text = "Average Trip Fare: $20"

        desc = Div(text=open(self.get_fullpath("description.html")).read(), width=800)

        #
        # Create input controls
        #
        taxi_type = RadioButtonGroup(labels=["Yellow", "Green", "FHV", "  All  "], active=0)
        taxi_type.on_change('active', lambda attr, old, new: update())

        pickup_or_dropoff = Toggle(label="Pickup", button_type="primary")
        pickup_or_dropoff.on_change('active', lambda attr, old, new: update())

        # BUG: Dropdown menu value cannot be integer, i.e., ('Mahattan', '1')
        borough_menu = [('All Boroughs', 'All Boroughs'), None,
            ('Manhattan', 'Manhattan'), ('Bronx', 'Bronx'), ('Brooklyn', 'Brooklyn'),
            ('Queens', 'Queens'), ('Staten Island', 'Staten Island')]
        # https://github.com/bokeh/bokeh/issues/4915
        borough = Dropdown(label="NYC Boroughs", button_type="warning",
            menu=borough_menu, value='All Boroughs')
        borough.on_change('value', lambda attr, old, new: update())

        year_range = RangeSlider(title="Year", start=2009, end=2016, step=1, range=(2009, 2009))
        year_range.on_change('range', lambda attr, old, new: update())

        month_range = RangeSlider(title="Month", start=1, end=12, step=1, range=(1, 12))
        month_range.on_change('range', lambda attr, old, new: update())

        emr_size = Slider(title="Nodes", start=1, end=10, value=1, step=1)
        emr_size.on_change('value', lambda attr, old, new: update())

        controls = [taxi_type, pickup_or_dropoff, borough, year_range, month_range, emr_size]

        # Plot charts
        palette.reverse()
        color_mapper = LogColorMapper(palette=palette)

        # Pickup/Dropoff Map
        districts = NYCGeoPolygon.load(self.get_fullpath('nyc_community_districts.geojson'))
        xs = []
        ys = []
        names = []
        rates = []
        for i, district in enumerate(districts):
            x, y = district.xy()
            xs.append(x)
            ys.append(y)
            names.append(district.name)
            rates.append(i)

        source = ColumnDataSource(data=dict(
            x=xs,
            y=ys,
            name=names,
            rate=rates,
        ))

        TOOLS = "pan,wheel_zoom,box_zoom,reset,hover,save"

        pickups_map = figure(
            plot_height=700, plot_width=700,
            tools=TOOLS,
            x_axis_location=None, y_axis_location=None
        )
        pickups_map.grid.grid_line_color = None

        pickups_map.patches('x', 'y', source=source,
            fill_color={'field': 'rate', 'transform': color_mapper},
            fill_alpha=0.7, line_color="white", line_width=0.5)

        hover = pickups_map.select_one(HoverTool)
        hover.point_policy = "follow_mouse"
        hover.tooltips = [
            ("Name", "@name"),
            ("Pickups", "@rate"),
            ("Long, Lat", "($x, $y)"),
        ]

        trip_distance = Histogram(df, plot_width=570, plot_height=350,
            values='mpg', xlabel='miles', ylabel='count', title="Average Trip Distance",
            color='firebrick')

        trip_fare = figure(plot_width=570, plot_height=350)
        trip_fare.circle([1, 2, 3, 4, 5], [6, 7, 2, 4, 5], size=15, line_color="navy",
                         fill_color="orange", fill_alpha=0.5)

        usage_data = dict(
            CPU=[2, 3, 7, 5, 26, 221, 44, 233, 254, 265, 266, 267, 120, 111],
            MEM=[12, 33, 47, 15, 126, 121, 144, 233, 254, 225, 226, 267, 110, 130],
        )
        resource_usage = Line(usage_data, plot_width=1500, plot_height=120, toolbar_location=None,
            title=None, xlabel="Time", ylabel="%", legend='top_right')
        resource_usage.xgrid.visible = False
        resource_usage.ygrid.visible = False

        right_column = column([trip_distance, trip_fare])
        inputs = widgetbox(*controls, width=226, sizing_mode="fixed")
        l = layout([
            [desc],
            [inputs, pickups_map, right_column],
            [resource_usage],
        ], sizing_mode="fixed")

        update()  # initial load of the data

        curdoc().add_root(l)
        curdoc().title = "NYC Taxi Data Explorer"

if __name__ == "__main__":
    print("usage: bokeh serve --show %s" % os.path.dirname(__file__))
else:
    p = InteractivePlot()
    p.plot()
