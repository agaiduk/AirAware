import googlemaps
from flask import Flask
from flask import render_template, request, flash, redirect, url_for, send_file
from flask import stream_with_context, Response
from flask_sqlalchemy import SQLAlchemy
from flask_bootstrap import Bootstrap
from sqlalchemy.sql import text
from flask_cassandra import CassandraCluster
import os
import models
from forms import AddressForm
import pprint
from datetime import datetime
import json
import csv
from collections import OrderedDict
#from tempfile import NamedTemporaryFile
from io import StringIO


app = Flask(__name__)
Bootstrap(app)
app.config.from_object('config.DevelopmentConfig')
db = SQLAlchemy(app)
GoogleMapsKey = app.config["GOOGLEMAPSKEY"]
GoogleMapsJSKey = app.config["GOOGLEMAPSJSKEY"]
gmaps = googlemaps.Client(key=GoogleMapsKey)
API_url = "https://maps.googleapis.com/maps/api/js?key=" + GoogleMapsJSKey + "&callback=initMap"
cassandra = CassandraCluster()


@app.route('/download', methods=['GET', 'POST'])
def download():

    def make_csv(grid_id):
        session = cassandra.connect()
        session.set_keyspace("air")
        cql_ozone = "SELECT * FROM air.measurements_hourly WHERE grid_id = {} AND parameter = {}".format(grid_id, '44201')
        cql_pm = "SELECT * FROM air.measurements_hourly WHERE grid_id = {} AND parameter = {}".format(grid_id, '88101')
        cql_pm_nof = "SELECT * FROM air.measurements_hourly WHERE grid_id = {} AND parameter = {}".format(grid_id, '88502')
        records_ozone = list(session.execute(cql_ozone))
        records_pm = list(session.execute(cql_pm))
        records_pm_nof = list(session.execute(cql_pm_nof))
        data = dict()

        for record in records_ozone:
            time = record.time.strftime('%Y-%m-%d %H:%M')
            if not data.get(time):
                data[time] = dict()
            data[time][44201] = record.c

        for record in records_pm:
            time = record.time.strftime('%Y-%m-%d %H:%M')
            if not data.get(time):
                data[time] = dict()
            data[time][88101] = record.c

        for record in records_pm_nof:
            time = record.time.strftime('%Y-%m-%d %H:%M')
            if not data.get(time):
                data[time] = dict()
            data[time][88502] = record.c

        data_sorted = OrderedDict(sorted(data.items(), key=lambda t: t[0]))

        # f = csv.writer(open("data.csv", "w"))
        #file_tmp = NamedTemporaryFile(delete=False)
        yield ",".join(["timestamp", "ozone", "pm2.5_frm", "pm2.5_nfrm"]) + '\n'
        for key, values in data_sorted.items():
            yield ",".join([key, str(values.get(44201, '')), str(values.get(88101, '')), str(values.get(88502, ''))]) + '\n'

    if request.method == 'GET':
        return redirect('/')

    elif request.method == 'POST':
        grid_id = request.form['grid_id']
        return Response(stream_with_context(make_csv(grid_id)), mimetype='text/csv',
        headers={"Content-Disposition": "attachment; filename=data_grid_{}.csv".format(grid_id)})
        # return send_file(f_IO, as_attachment=True, attachment_filename='data_grid_{}.csv'.format(grid_id))
        # return 'records_pm_nof = {}'.format(len(records_ozone), len(records_pm), len(records_pm_nof))


@app.route('/test', methods=['GET', 'POST'])
def test(chartID='chart_ID', chart_type='line', chart_height=350):
    dates = ["04/01/2014", "05/01/2014", "06/01/2014", "07/01/2014"]
    measurements = [5, 6, 7, 8]
    data = list(map(list, list(zip([1000*int(datetime.strptime(date, '%m/%d/%Y').strftime('%s')) for date in dates], measurements))))
    print(data)
    chart = {"renderTo": chartID, "type": chart_type, "height": chart_height}
    series = [{'pointInterval': 30 * 24 * 3600 * 1000, "name": 'Ozone', "data": data}]
    #series = [{'pointInterval': 30 * 24 * 3600 * 1000, "name": 'Ozone', "data": [[1396310400000, 5], [1398902400000, 6], [1401580800000, 7], [1404172800000, 8]]}]
    title = {"text": 'Pollution for a given grid point'}
    xAxis = {'type': 'datetime', "title": {'text': 'Date'}}
    yAxis = {"title": {"text": 'Concentration'}}
    return render_template('index.html', chartID=chartID, chart=chart, series=series, title=title, xAxis=xAxis, yAxis=yAxis)


@app.route('/', methods=['GET', 'POST'])
def hello():

    if request.method == 'GET':
        return render_template('dashboard.html')

    elif request.method == 'POST':

        address = request.form['address']

        geocode_result = gmaps.geocode(address)
        coordinates = geocode_result[0]["geometry"]["location"]

        #pprint.pprint(geocode_result)

        latitude = coordinates["lat"]
        longitude = coordinates["lng"]

        sql = text(
            """
            SELECT ST_Distance(location, 'POINT({longitude} {latitude})'::geography) as d, grid_id, longitude, latitude
            FROM grid ORDER BY location <-> 'POINT({longitude} {latitude})'::geography limit 10000;
            """.format(**locals())
        )
        nearest_grid_points = db.engine.execute(sql).fetchall()
        for i in range(0, len(nearest_grid_points)):
            grid_id = nearest_grid_points[i][1]
            history_measurements = models.measurements_monthly.query.filter_by(grid_id=grid_id).order_by(models.measurements_monthly.time.desc()).all()
            if not history_measurements:
                continue
            else:
                ozone = [x for x in history_measurements if x.parameter == 44201]
                ozone_data = [[1000*int(x.time.strftime('%s')), round(1000*x.c,2)] for x in ozone]

                pm = [x for x in history_measurements if x.parameter == 88101]
                pm_data = [[1000*int(x.time.strftime('%s')), round(x.c,2)] for x in pm]

                # Setup charts
                chart_type='line'
                chart_height=350
                chart_ozone = {"renderTo": 'chart_ozone', "type": chart_type, "height": chart_height}
                chart_pm = {"renderTo": 'chart_pm', "type": chart_type, "height": chart_height}
                series_ozone = [{'pointInterval': 30 * 24 * 3600 * 1000, "name": 'Ozone', "data": ozone_data}]
                series_pm = [{'pointInterval': 30 * 24 * 3600 * 1000, "name": 'PM2.5', "data": pm_data}]
                title = {"text": ''}
                xAxis = {'type': 'datetime', "title": {'text': 'Date'}}
                yAxis = {"title": {"text": 'Concentration'}}

                distance = nearest_grid_points[i][0]/(1600.)  # Delete by 1600 to get miles from meters
                grid_longitude = nearest_grid_points[i][2]
                grid_latitude = nearest_grid_points[i][3]
                current = history_measurements[0]
                time = current.time.strftime("%b %d, %Y at %I:%M %p")
                parameter = current.parameter
                c = current.c
                print('Reporting readings at the grid point {}, {} miles away from the tagret address'.format(grid_id, distance))
                break
        #return render_template('base.html', chart_ozone=chart_ozone, chart_pm=chart_pm, series_ozone=series_ozone, series_pm=series_pm)
        return render_template('dashboard.html', chart_ozone=chart_ozone, chart_pm=chart_pm, series_ozone=series_ozone, series_pm=series_pm,
                               ozone_current=ozone_data[0][1], pm_current=pm_data[0][1],
                               lat=latitude, lon=longitude, grid_id=grid_id, API_url=API_url)


if __name__ == '__main__':
    app.debug = True
    app.run(host='0.0.0.0')
