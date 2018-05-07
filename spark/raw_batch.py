from __future__ import print_function

import csv
import json
from datetime import datetime
from StringIO import StringIO
from math import radians, sin, cos, sqrt, asin
import configparser

from pyspark import SparkContext
from pyspark.sql import SparkSession, SQLContext, DataFrameReader, Row
from pyspark.sql.types import (StructType, StructField, FloatType, DateType,
                               TimestampType, IntegerType, StringType)


def get_grid_from_file(filename):
    '''
    Load a text file with one line of JSON containing all grid points

    Parameters
    ----------
    filename : str
                    Name of the json file

    Returns
    -------
    dict
                    Dictionary of grid points with lon and lat
    '''
    with open(filename) as f:
        raw_json = f.readline()
    return json.loads(raw_json)


def header(line):
    '''
    Filter out the header
    '''
    # This is header
    if 'State Code' in line:
        return False
    # This is not
    else:
        return True


def raw_to_station(line):
    '''
    This function maps each measurement to a digestable format
    '''
    if line[0] == 'State Code':
        return None
    lat = float(line[5])
    lon = float(line[6])
    compound = line[8]
    date_local = line[9]
    time_local = line[10]
    timestamp_local = datetime.strptime(date_local + time_local, '%Y-%m-%d%H:%M')
    date_gmt = line[11]
    time_gmt = line[12]
    timestamp_gmt = datetime.strptime(date_gmt + time_gmt, '%Y-%m-%d%H:%M')
    C = float(line[13])
    mdl = float(line[15])
    # Measured concentration is below detection limit
    if C < mdl:
        C = 0
    units = line[14]
    state_code = line[0]
    county_code = line[1]
    site_code = line[2]
    state_name = line[21]
    county_name = line[22]
    return (timestamp_gmt, (lat, lon, state_code, county_code, compound, C, units))


def station_to_grid(rdd):
    '''
    Takes RDD with air quality stations' readings and and returns
    RDDs for readings transformed to nearest grid points

    Parameters
    ----------
    rdd : RDD
                RDD of air monitors readings

    Returns
    -------
    RDD
                RDDs of air monitors reading transformed to nearest grid points
    '''
    time = rdd[0]
    station_lat = rdd[1][0]
    station_lon = rdd[1][1]
    state_code = rdd[1][2]
    county_code = rdd[1][3]
    compound = rdd[1][4]
    C = rdd[1][5]
    units = rdd[1][6]
    measurements = []
    for grid in GRID:
        grid_id = grid.get("id", None)
        grid_lon = grid.get("lon", None)
        grid_lat = grid.get("lat", None)
        distance = calc_distance(station_lat, station_lon, grid_lat, grid_lon)
        weight = 1. / (float(distance) ** 2)
        weight_C_prod = C * weight
        measurements.append(((time, grid_lat, grid_lon, grid_id),
                            (weight_C_prod, weight)))
    return measurements


def sum_weight_and_prods(val1, val2):
    '''
    Little custom map function to compute weighted averages

    Parameters
    ----------
    val1: RDD
                Value of first RDD
    val2: RDD
                Value of second RDD

    Returns
    -------
    RDD
                RDD with tuples reduced based on weights and weight*C
    '''
    return (val1[0] + val2[0], val1[1] + val2[1])


def split_line(line):
    '''
    This function splits line efficiently using csv package

    Input
    -----
    line : str
                One line containing air monitor reading

    Returns
    -------
    list
                List of fields in the air monitor reading
    '''
    f = StringIO(line)
    reader = csv.reader(f, delimiter=',')
    return reader.next()


def calc_distance(lat1, lon1, lat2, lon2):
    '''
    Compute distance between two geographical points
    Source: https://rosettacode.org/wiki/Haversine_formula#Python

    Parameters
    ----------
    lat1 : float
    lon1 : float
            Latitude and longitude of the first geographical point

    lat2 : float
    lon2 : float
            Latitude and longitude of the second geographical point


    Returns
    -------
    float
            Distance between two points in kilometers
    '''
    R = 6372.8  # Earth's radius in kilometers
    delta_lat = radians(lat2 - lat1)
    delta_lon = radians(lon2 - lon1)
    lat1 = radians(lat1)
    lat2 = radians(lat2)
    a = sin(delta_lat / 2.0) ** 2 + cos(lat1) * cos(lat2) * sin(delta_lon / 2.0) ** 2
    c = 2 * asin(sqrt(a))
    return R * c


def map_to_distance(grid_lat, grid_lon, measurement):
    '''
    Obsolete - delete at next commit
    '''
    if measurement is None:
        return None
    station_lat = measurement[0]
    station_lon = measurement[1]
    timestamp_gmt = measurement[5]
    C = measurement[6]
    distance = calc_distance(grid_lat, grid_lon, station_lat, station_lon)
    return ((grid_lat, grid_lon, timestamp_gmt), C / distance**2)


def calc_weighted_average_grid(rdd):
    '''
    Compute the weighted average over the entire grid

    Parameters
    ----------
    rdd: RDD
            RDD containing weights and weighted pollution levels

    Returns
    -------
    RDD
            RDD with value as a weighted average pollution level
    '''
    weighted_avg = rdd[1][0] / float(rdd[1][1])
    time = rdd[0][0]
    lat = rdd[0][1]
    lon = rdd[0][2]
    grid_id = rdd[0][3]
    return (time, lat, lon, grid_id, weighted_avg)


def main():

    # Read in data from the configuration file

    config = configparser.ConfigParser()
    config.read('../setup.cfg')

    s3 = 's3a://' + config["s3"]["bucket"] + '/'
    spark_url = 'spark://' + config["spark"]["dns"]
    postgres_url = 'jdbc:postgresql://' + config["postgres"]["dns"] + '/'\
                   + config["postgres"]["db"]
    postgres_table_hourly = config["postgres"]["table_hourly"]
    postgres_credentials = {
        'user': config["postgres"]["user"],
        'password': config["postgres"]["password"]
    }

    # Create Spark context & session

    sc = SparkContext(spark_url, "Batch")
    spark = SparkSession(sc)
    sqlContext = SQLContext(sc)

    # Schemas for converting RDDs to DataFrames & writing to DBs

    measurement_schema = StructType([
        StructField("grid_id", IntegerType(), False),
        StructField("time", TimestampType(), False),
        StructField("c", FloatType(), False)
    ])

    # Start processing data files

    data_file = 'day'
    raw = s3 + data_file

    data_rdd = sc.textFile(raw)

    # Global variable GRID to store the grid points & distances to each station

    global GRID
    GRID = get_grid_from_file("grid.json")

    # grid_df = sc.parallelize(GRID).toDF()
    # grid_df.write.jdbc(url=postgres_url, table='coordinates', mode='append', properties=postgres_properties)

    # Read query - not planning to have those currently
    # df = sqlContext.read.jdbc(url=postgres_url, table='coordinates', properties=postgres_properties)

    # df = sc.parallelize([Row(grid_id=1, time=datetime.now(), c=11)]).toDF()
    # df.write.jdbc(url=postgres_url, table='hourly', mode='append', properties=postgres_properties)

    # Do the thing
    data = data_rdd.filter(header)\
                   .map(split_line)\
                   .map(raw_to_station)\
                   .flatMap(station_to_grid)\
                   .reduceByKey(sum_weight_and_prods)\
                   .map(calc_weighted_average_grid)\
                   .map(lambda line: (line[3], line[0], line[4]))\
                   .persist()
                 # .filter(lambda line: line[0] != 'State Code')\
    # data.collect()

    data_df = spark.createDataFrame(data, measurement_schema)
    data_df.write.jdbc(url=postgres_url, table=postgres_table_hourly,
                       mode='append', properties=postgres_credentials)

    data_df.show()

if __name__ == '__main__':
    main()

