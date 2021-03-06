import googlemaps
from flask import Flask
from flask import render_template, request, redirect
from flask import stream_with_context, Response
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.sql import text
from flask_cassandra import CassandraCluster
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
pm_frm_code = 88101  # Federal reference methods
pm_code = 88502  # Non-federal reference methods

# SF coordinates as a default
sf = dict()
sf['lat'] = 41.8781136
sf['lon'] = -87.6297982

import models


def get_pollutant_records(session, data, grid_id, parameter):
    '''
    Add full historical pollution data for pollutant code (parameter)
    at grid_id to a dictionary data
    '''
    cql = "SELECT * FROM air.measurements_hourly WHERE grid_id = {} AND parameter = {}"
    cql_command = cql.format(grid_id, parameter)
    records = list(session.execute(cql_command))

    for record in records:
        time = record.time.strftime('%Y-%m-%d %H:%M')
        if not data.get(time):
            data[time] = dict()
        data[time][parameter] = record.c


def get_pollution_data(grid_id):
    # Connect to Cassandra database and obtain pollution data
    session = cassandra.connect()
    session.set_keyspace("air")

    data = dict()
    get_pollutant_records(session, data, grid_id, ozone_code)
    get_pollutant_records(session, data, grid_id, pm_frm_code)
    get_pollutant_records(session, data, grid_id, pm_code)

    return OrderedDict(sorted(data.items(), key=lambda t: t[0]))


def get_ozone_and_pm(record):
    '''
    Given record containing pollution data, return streamlined record
    '''
    ozone = record.get(ozone_code, None)
    pm_frm = record.get(pm_frm_code, None)
    pm = record.get(pm_code, None)

    # Compute PM2.5 pollution level based on measurements available
    # If we have a zero, report pollution level as 0, not None
    n_pm = 0  # Counter of valid pm measurements
    pm_average = 0.
    if pm_frm is not None:
        n_pm += 1
        pm_average += pm_frm
    if pm is not None:
        n_pm += 1
        pm_average += pm

    if n_pm:
        pm_average /= n_pm
    else:
        pm_average = None

    return ['{:.2f}'.format(1000*ozone) if ozone is not None else '',
            '{:.2f}'.format(pm_average) if pm_average is not None else '']


def make_csv(grid_id):
    '''
    This function makes csv file with the full air pollution history for a given grid point
    '''
    # OrderedDict assembled from Cassandra
    data = get_pollution_data(grid_id)

    yield ",".join(["Timestamp", "Ozone [ppb]", "PM2.5 [mcg/m3]"]) + '\n'
    for timestep, record in data.items():
        yield ","\
            .join([item for sublist in [[timestep], get_ozone_and_pm(record)]
                  for item in sublist]) + '\n'


def get_coordinates_from_address(address_request):
    '''
    This function converts address to coordinates using Google Maps API call
    '''
    geocode_result = gmaps.geocode(address_request)

    # Some defaults
    error_message = 'Please enter a valid U.S. address'
    formatted_address = None
    coordinates = None
    latitude, longitude = None, None

    if len(geocode_result) == 0:
        return sf['lat'], sf['lon'], error_message

    address = geocode_result[0]
    # Check if the address is in the U.S.
    try:
        formatted_address = address['formatted_address'].lower()
    except (TypeError, KeyError):
        return sf['lat'], sf['lon'], error_message

    if 'usa' in formatted_address or 'puerto rico' in formatted_address:
        try:
            coordinates = address["geometry"]["location"]
        except (TypeError, KeyError):
            return sf['lat'], sf['lon'], error_message

    if coordinates:
        latitude = coordinates["lat"]
        longitude = coordinates["lng"]
    else:
        return sf['lat'], sf['lon'], error_message

    return latitude, longitude, ''


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


def pollution_level(c, moderate, bad):
    '''
    Determine pollution level based on thresholds
    '''
    if c < moderate:
        result = 'good'
    elif c > moderate and c < bad:
        result = 'moderate'
    elif c > bad:
        result = 'bad'

    return result


@app.route('/', methods=['GET', 'POST'])
def dashboard():

    def request_from_location(latitude, longitude, error_message=''):
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
                .order_by(models.measurements_monthly.time.asc()).all()

            if not history_measurements:
                # The grid point we found does not contain any historical
                # data (for example, it is far from any air quality station)
                continue

            else:
                ozone = [x for x in history_measurements if x.parameter == ozone_code]
                ozone_data = [[1000*int(x.time.strftime('%s')), round(1000*x.c,2)] for x in ozone]

                pm = [x for x in history_measurements if x.parameter == pm_frm_code]
                pm_data = [[1000*int(x.time.strftime('%s')), round(x.c,2)] for x in pm]

                if len(ozone_data) == 0 or len(pm_data) == 0:
                    rendered_webpage = request_from_location(
                            sf['lat'],
                            sf['lon'],
                            'Location you entered is too far from air quality monitors'
                        )
                    return rendered_webpage

                ozone_current = ozone_data[-1][1]
                pm_current = pm_data[-1][1]
                ozone_level = pollution_level(ozone_current, 30, 50)
                pm_level = pollution_level(pm_current, 12, 30)

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
            ozone_level=ozone_level, pm_level=pm_level,
            lat=latitude, lon=longitude, grid_id=grid_id, API_url=API_url,
            error_message=error_message
        )

    if request.method == 'GET':
        # Default coordinates in San Francisco downtown
        rendered_webpage = request_from_location(sf['lat'], sf['lon'])
        return rendered_webpage

    elif request.method == 'POST':

        # Get address entered by the user
        address_request = request.form['address']

        # Obtain geolocation results from Google Maps
        latitude, longitude, error_message =\
            get_coordinates_from_address(address_request)

        rendered_webpage =\
            request_from_location(latitude, longitude, error_message=error_message)

        return rendered_webpage


@app.route('/about', methods=['GET'])
def about():
    return redirect("https://github.com/agaiduk/AirAware")


@app.route('/slides', methods=['GET'])
def slides():
    return redirect("https://docs.google.com/presentation/d/1BWLKoafapgM5VxpgU_nHYCeJLk38wu1VACwECdN8RIo")


@app.route('/github', methods=['GET'])
def github():
    return redirect("https://github.com/agaiduk")


if __name__ == '__main__':
    app.debug = True
    app.run(host='0.0.0.0')
