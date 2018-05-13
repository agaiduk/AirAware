import googlemaps
from flask import Flask
from flask import render_template, request, flash, redirect, url_for
from flask_sqlalchemy import SQLAlchemy
from flask_bootstrap import Bootstrap
from sqlalchemy.sql import text
import os
import models
from forms import AddressForm
import pprint
from datetime import datetime


app = Flask(__name__)
Bootstrap(app)
app.config.from_object('config.DevelopmentConfig')
db = SQLAlchemy(app)
GoogleMapsKey = app.config["GOOGLEMAPSKEY"]
GoogleMapsJSKey = app.config["GOOGLEMAPSJSKEY"]
gmaps = googlemaps.Client(key=GoogleMapsKey)
API_url = "https://maps.googleapis.com/maps/api/js?key=" + GoogleMapsJSKey + "&callback=initMap"


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
                pm_data = [[1000*int(x.time.strftime('%s')), x.c] for x in pm]

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
                               lat=latitude, lon=longitude, API_url=API_url)


if __name__ == '__main__':
    app.debug = True
    app.run(host='0.0.0.0')
