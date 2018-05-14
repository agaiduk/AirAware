import googlemaps
from flask import Flask
from flask import render_template, request, redirect
from flask import stream_with_context, Response
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.sql import text
from flask_cassandra import CassandraCluster
import models
from datetime import datetime
from collections import OrderedDict


app = Flask(__name__)
app.config.from_object('config.DevelopmentConfig')
GoogleMapsKey = app.config["GOOGLEMAPSKEY"]
GoogleMapsJSKey = app.config["GOOGLEMAPSJSKEY"]

db = SQLAlchemy(app)
cassandra = CassandraCluster()
gmaps = googlemaps.Client(key=GoogleMapsKey)
API_url = "https://maps.googleapis.com/maps/api/js?key="\
        + GoogleMapsJSKey + "&callback=initMap"

# Parameter codes for asthma-causing pollutants
ozone_code = 44201
pm_frm_code = 88101  # Federal reference method
pm_code = 88502  # Non-federal reference method


def make_csv(grid_id):
    session = cassandra.connect()
    session.set_keyspace("air")
    cql_ozone = "SELECT * FROM air.measurements_hourly WHERE grid_id = {} AND parameter = {}".format(grid_id, ozone_code)
    cql_pm = "SELECT * FROM air.measurements_hourly WHERE grid_id = {} AND parameter = {}".format(grid_id, pm_frm_code)
    cql_pm_nof = "SELECT * FROM air.measurements_hourly WHERE grid_id = {} AND parameter = {}".format(grid_id, pm_code)
    records_ozone = list(session.execute(cql_ozone))
    records_pm = list(session.execute(cql_pm))
    records_pm_nof = list(session.execute(cql_pm_nof))
    data = dict()

    for record in records_ozone:
        time = record.time.strftime('%Y-%m-%d %H:%M')
        if not data.get(time):
            data[time] = dict()
        data[time][ozone_code] = record.c

    for record in records_pm:
        time = record.time.strftime('%Y-%m-%d %H:%M')
        if not data.get(time):
            data[time] = dict()
        data[time][pm_frm_code] = record.c

    for record in records_pm_nof:
        time = record.time.strftime('%Y-%m-%d %H:%M')
        if not data.get(time):
            data[time] = dict()
        data[time][pm_code] = record.c

    data_sorted = OrderedDict(sorted(data.items(), key=lambda t: t[0]))

    yield ",".join(["timestamp", "ozone", "pm2.5_frm", "pm2.5_nfrm"]) + '\n'
    for key, values in data_sorted.items():
        yield ",".join([key, str(values.get(ozone_code, '')), str(values.get(pm_frm_code, '')), str(values.get(pm_code, ''))]) + '\n'


@app.route('/download', methods=['GET', 'POST'])
def download():

    if request.method == 'GET':
        return redirect('/')

    elif request.method == 'POST':
        grid_id = request.form['grid_id']
        return Response(
            stream_with_context(make_csv(grid_id)),
            mimetype='text/csv',
            headers={
                "Content-Disposition":
                "attachment; filename=data_grid_{}.csv".format(grid_id)
            }
        )


@app.route('/', methods=['GET', 'POST'])
def dashboard():

    def request_from_location(latitude, longitude):
        '''
        This function prepares Http request object,
        based on user's location input
        '''
        sql = text(
            """
            SELECT ST_Distance(location, 'POINT({longitude} {latitude})'::geography) as d, grid_id, longitude, latitude
            FROM grid ORDER BY location <-> 'POINT({longitude} {latitude})'::geography limit 10000;
            """.format(**locals())
        )
        nearest_grid_points = db.engine.execute(sql).fetchall()

        for i in range(0, len(nearest_grid_points)):
            grid_id = nearest_grid_points[i][1]
            history_measurements = models.measurements_monthly\
                                   .query.filter_by(grid_id=grid_id)\
                                   .order_by(models.measurements_monthly\
                                   .time.asc()).all()

            if not history_measurements:
                # If the grid point we found does not contain any historical data
                # (for example, it is far from any air control station)
                continue

            else:
                ozone = [x for x in history_measurements if x.parameter == ozone_code]
                ozone_data = [[1000*int(x.time.strftime('%s')), round(1000*x.c,2)] for x in ozone]

                pm = [x for x in history_measurements if x.parameter == pm_frm_code]
                pm_data = [[1000*int(x.time.strftime('%s')), round(x.c,2)] for x in pm]

                # Setup charts
                chart_type = 'line'
                chart_height = 350
                chart_ozone = {"renderTo": 'chart_ozone', "type": chart_type, "height": chart_height}
                chart_pm = {"renderTo": 'chart_pm', "type": chart_type, "height": chart_height}
                series_ozone = [{'pointInterval': 30 * 24 * 3600 * 1000, "name": 'Ozone', "data": ozone_data}]
                series_pm = [{'pointInterval': 30 * 24 * 3600 * 1000, "name": 'PM2.5', "data": pm_data}]
                break

        return render_template(
            'dashboard.html', chart_ozone=chart_ozone, chart_pm=chart_pm,
            series_ozone=series_ozone, series_pm=series_pm,
            ozone_current=ozone_data[-1][1], pm_current=pm_data[-1][1],
            lat=latitude, lon=longitude, grid_id=grid_id, API_url=API_url
        )

    if request.method == 'GET':
        # Default coordinates in San Francisco downtown
        rendered_webpage = request_from_location(37.7749295, -122.4194155)
        return rendered_webpage

    elif request.method == 'POST':

        # Get address entered by the user
        address = request.form['address']

        # Obtain geolocation results from Google Maps
        geocode_result = gmaps.geocode(address)
        coordinates = geocode_result[0]["geometry"]["location"]

        latitude = coordinates["lat"]
        longitude = coordinates["lng"]

        rendered_webpage = request_from_location(latitude, longitude)
        return rendered_webpage


if __name__ == '__main__':
    app.debug = True
    app.run(host='0.0.0.0')
