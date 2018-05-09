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
        FROM grid ORDER BY location <-> 'POINT({longitude} {latitude})'::geography limit 100000;
        """.format(**locals())
        )
        nearest_grid_points = db.engine.execute(sql).fetchall()
        for i in range(0, len(nearest_grid_points)):
            grid_id = nearest_grid_points[i][1]
            history_measurements = models.measurements.query.filter_by(grid_id=grid_id).order_by(models.measurements.time.desc()).all()
            if not history_measurements:
                continue
            else:
                distance = nearest_grid_points[i][0]/(1600.)  # Delete by 1600 to get miles from meters
                grid_longitude = nearest_grid_points[i][2]
                grid_latitude = nearest_grid_points[i][3]
                current = history_measurements[0]
                c = current.c
                time = current.time.strftime("%b %d, %Y at %I:%M %p")
                if i == 0:
                    flash("Pollution level of nitrous oxides {:.2f} miles from your address was {} ppb at {}".format(distance, c, time))
                else:
                    flash("There are no grid points in the vicinity. Pollution level of nitrous oxides at the closest site {:.2f} miles from your address was {} ppb at {}".format(distance, c, time))
                flash("History of measurements:")
                for event in history_measurements:
                    flash("{} {}".format(event.time.strftime("%Y-%m-%d %H:%M GMT"), event.c))
                break

            #print("grid_id: {} at {} longitude and {} latitude {} miles from the address".format(grid_id, grid_longitude, grid_latitude, distance))

            # measurement = models.hourly.query.order_by(models.hourly.time).join(models.coordinates).filter_by(site_id=998).first()

    return render_template('form.html', form=form)


if __name__ == '__main__':
    app.debug = True
    app.run(host='0.0.0.0')
