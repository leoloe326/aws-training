#!/usr/bin/env python

# Copyright 2017, Nan Dun <nan.dun@acm.org>
# All rights reserved.

from __future__ import print_function

import argparse
import os.path
import time

from bokeh.charts import Area, Line, Histogram, Bar
from bokeh.plotting import figure
#from bokeh.palettes import Viridis6 as palette
from bokeh.palettes import GnBu6 as palette
from bokeh.layouts import row, column, layout, widgetbox
from bokeh.models import ColumnDataSource, HoverTool, Div, LogColorMapper
from bokeh.models.widgets import Dropdown, RangeSlider, RadioButtonGroup, Slider, Toggle
from bokeh.io import curdoc
from bokeh.sampledata.autompg import autompg as df

from geo import NYCGeoPolygon
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
        self.db = StatDB(opts)
        self.last_refresh = time.time()

    def in_borough(self, borough, district):
        if borough == 'All Boroughs': return True

        borough_map = {'Manhattan' : 1, 'Bronx' : 2, 'Brooklyn' : 3,
                       'Queens' : 4, 'Staten Island' : 5}
        if district.region == borough_map[borough]: return True
        return False

    def plot(self):
        def update():
            # prevent DynamoDB from polling too often
            if time.time() - self.last_refresh < 2: time.sleep(2)
            data = self.db.get('green', 2016, 1)

            pickup_or_dropoff.label = 'Pickups' if pickup_or_dropoff.active else 'Dropoffs'
            xs = []
            ys = []
            names = []
            rates = []
            for district in districts:
                x, y = district.xy()
                xs.append(x)
                ys.append(y)
                names.append(district.name)
                if self.in_borough(borough.value, district):
                    if pickup_or_dropoff.label == 'Pickups':
                        rates.append(data.pickups[district.index])
                    else:
                        rates.append(data.dropoffs[district.index])
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

            trip_hour_data = {'hour': [data.hour[i] + 1000 for i in range(24)]}
            trip_hour = Bar(data=trip_hour_data,
                plot_width=620, plot_height=350, values='hour',
                xlabel='miles', ylabel='count', title="Hour",
                color='firebrick', legend=None)

            borough.label = borough.value
            trip_distance.title.text = "Distance: 10 mile"
            trip_fare.title.text = "Fare: $20"

            self.last_refresh = time.time()

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

        # Plot charts
        palette.reverse()
        color_mapper = LogColorMapper(palette=palette)

        # Pickup/Dropoff Map
        data = self.db.get('green', 2016, 1)
        districts = NYCGeoPolygon.load_districts()
        xs = []
        ys = []
        names = []
        rates = []
        for district in districts:
            x, y = district.xy()
            xs.append(x)
            ys.append(y)
            names.append(district.name)
            if pickup_or_dropoff.label == "Pickup":
                rates.append(data.pickups[district.index])
            else:
                rates.append(data.dropoffs[district.index])

        source = ColumnDataSource(data=dict(
            x=xs,
            y=ys,
            name=names,
            rate=rates,
        ))

        TOOLS = "pan,wheel_zoom,box_zoom,reset,hover,save"

        pickups_map = figure(webgl=True,
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

        trip_hour_data = {'hour': [data.hour[i] for i in range(24)]}
        trip_hour = Bar(data=trip_hour_data,
            plot_width=620, plot_height=350, values='hour',
            xlabel='miles', ylabel='count', title="Hour",
            color='firebrick', legend=None)
        trip_hour.xaxis.major_tick_line_color = None
        trip_hour.xaxis.minor_tick_line_color = None

        trip_distance = Histogram(df, plot_width=310, plot_height=350,
            values='mpg', xlabel='miles', ylabel='count', title="Distance",
            color='firebrick')

        trip_fare = figure(plot_width=310, plot_height=350)
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

        rightdown_row = row([trip_distance, trip_fare])
        right_column = column([trip_hour, rightdown_row])
        inputs = widgetbox(*controls, width=140, sizing_mode="fixed")
        l = layout([
            [desc],
            [inputs, pickups_map, right_column],
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
