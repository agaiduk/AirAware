import googlemaps
from flask import Flask
from flask import render_template, request, flash, redirect, url_for
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.sql import text
import os
import models
from forms import AddressForm
import pprint
from datetime import datetime


app = Flask(__name__)
app.config.from_object('config.DevelopmentConfig')
db = SQLAlchemy(app)
gmaps = googlemaps.Client(key=app.config["GOOGLEMAPSAPIKEY"])


@app.route('/', methods=['GET', 'POST'])
def hello():

    form = AddressForm(request.form)

    if request.method == 'POST':
        address=request.form['address']

        geocode_result = gmaps.geocode(address)
        coordinates = geocode_result[0]["geometry"]["location"]

        #latitude = 37.4263253
        #longitude = -122.1409849
        latitude = coordinates["lat"]
        longitude = coordinates["lng"]

        sql = text(
        """
        SELECT ST_Distance(location, 'POINT({longitude} {latitude})'::geography) as d, grid_id, longitude, latitude
        FROM grid ORDER BY location <-> 'POINT({longitude} {latitude})'::geography limit 100;
        """.format(**locals())
        )
        nearest_grid_point = db.engine.execute(sql).fetchall()
        distance = nearest_grid_point[0][0]/(1600.)  # Delete by 1600 to get miles from meters
        grid_id = nearest_grid_point[0][1]
        grid_longitude = nearest_grid_point[0][2]
        grid_latitude = nearest_grid_point[0][3]
        #print("grid_id: {} at {} longitude and {} latitude".format(grid_id, grid_longitude, grid_latitude))

        # measurement = models.hourly.query.order_by(models.hourly.time).join(models.coordinates).filter_by(site_id=998).first()
        history_measurements = models.measurements.query.filter_by(grid_id=grid_id).order_by(models.measurements.time.desc()).all()
        current = history_measurements[0]
        c = current.c
        time = current.time.strftime("%b %d, %Y at %I:%M %p")
        flash("Pollution level of nitrous oxides {:.2f} miles from your address was {} ppb at {}".format(distance, c, time))
        flash("History of measurements:")
        for event in history_measurements:
            flash("{} {}".format(event.time.strftime("%Y-%m-%d %H:%M GMT"), event.c))
        #flash("The address of the grid point nearest to your location is ... Pollution level of nitrous oxides here are {}, {}, {} is {}".format(measurement.grid.grid_id, measurement.grid.latitude, measurement.grid.longitude, measurement.c))

    return render_template('form.html', form=form)


if __name__ == '__main__':
    app.debug = True
    app.run(host='0.0.0.0')
