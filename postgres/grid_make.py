import configparser
import numpy
import json
import psycopg2
import sys
import os


def insert_records(commands):
    '''
    Insert grid points into the database
    '''
    # Read in configuration file

    config = configparser.ConfigParser()
    config.read('../setup.cfg')

    postgres_url = 'postgresql://'\
                   + config["postgres"]["user"] + ':' + config["postgres"]["password"]\
                   + '@localhost:' + config["postgres"]["port"] + '/' + config["postgres"]["db"]

    conn = None
    try:
        # connect to the PostgreSQL server
        conn = psycopg2.connect(postgres_url)
        cur = conn.cursor()
        # create table one by one
        print("got connection")
        for command in commands:
            cur.execute(command)
            print("executed command")
        # close communication with the PostgreSQL database server
        cur.close()
        print("closed the cursor")
        # commit the changes
        conn.commit()
        print("committed the connection")
    except (Exception) as error:
        print(error)
        raise error
    finally:
        if conn is not None:
            conn.close()
            print("closed the connection")


def main():

    # File where to store json
    fname = 'grid.json'

    # Number of samples
    N = 1000
    
    # Precision of the grid points
    precision = 3
    
    # Generate arrays for latitude and longitude
    # The points are generated between latitudes 35 and 49, and longitudes -120 and -80
    # to cover major portion of the continental US
    longitudes = numpy.random.uniform(-120,-80,N)
    latitudes = numpy.random.uniform(35,49,N)
    
    # Now go over these arrays and combine them into a 2d array
    
    grid_id = 0
    grid = []
    commands = []

    for i in range(0,N):
        grid_id += 1
        longitude = round(longitudes[i], precision)
        latitude = round(latitudes[i], precision)
        grid.append({'id': grid_id, 'lon': longitude, 'lat': latitude})
        commands.append(
        """
        INSERT INTO grid (grid_id, longitude, latitude, location) VALUES ({grid_id}, {longitude}, {latitude}, ST_GeogFromText('POINT({longitude} {latitude})') );
        """.format(**locals())
        )
    insert_records(commands)
    
    with open(fname, 'w') as f:
        json.dump(grid, f)


if __name__ == '__main__':
    main()
