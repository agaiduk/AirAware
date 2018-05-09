from __future__ import print_function

import csv
import json
from datetime import datetime
from StringIO import StringIO
import configparser

from pyspark import SparkContext, SparkConf
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


def convert_to_int(string):
    '''
    Returns an integer if it can or returns None otherwise

    Parameters
    ----------
    string : string, required
                String representing the property value

    Returns
    -------
    int
            If string can be represented as a valid integer
    None
            Otherwise
    '''
    try:
        number = int(string)
        return number
    except ValueError:
        return None


def convert_to_float(string):
    '''
    Returns a float if it can or returns None otherwise

    Parameters
    ----------
    string : string, required
                String representing the property value

    Returns
    -------
    float
            If string can be represented as a valid nonzero float
    None
            Otherwise
    '''
    try:
        number = float(string)
        return number
    except ValueError:
        return None


def parse_measurement_record(measurement_record):
    '''
    This function ...

    Input
    -----
    line : str
                One line containing air monitor reading

    Returns
    -------
    list
                List of fields in the air monitor reading
    '''
    f = StringIO(measurement_record.encode('ascii', 'ignore'))
    reader = csv.reader(f, delimiter=',')
    record = reader.next()

    parameter = convert_to_int(record[3])
    if parameter not in [44201, 88101]:
        # Ignore records other than ozone (44201) or PM2.5 (88101)
        return None

    state_id = record[0]
    # Filter out header, Canada, Mexico, US Virgin Islands, or Guam
    if state_id in ['State Code', 'CC', '80', '78', '66']:
        return None

    county_id = record[1]
    site_number = record[2]
    site_id = '|'.join([state_id, county_id, site_number])

    # Carve out the GMT timestamp
    date = record[11]
    time = record[12]
    timestamp = datetime.strptime(date + time, '%Y-%m-%d%H:%M')

    C = convert_to_float(record[13])
    mdl = convert_to_float(record[15])
    if not C or not mdl:
        return None

    # Measured concentration is below detection limit
    if C < mdl:
        C = 0.

    return (site_id, parameter, C, timestamp)


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
    site_id = rdd[0]
    parameter = rdd[1]
    C = rdd[2]
    timestamp = rdd[3]
    grid = STATIONS[site_id]
    measurements = []
    for grid_id in grid:
        distance = grid[grid_id]
        weight = 1. / (distance ** 2)
        weight_C_prod = C * weight
        measurements.append(((int(grid_id), timestamp, parameter),
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
    grid_id = rdd[0][0]
    timestamp = rdd[0][1]
    parameter = rdd[0][2]
    weighted_avg = rdd[1][0] / float(rdd[1][1])
    return (grid_id, parameter, timestamp, weighted_avg)


def group_by_month(rdd):
    '''
    Given an rdd containing weighted average air pollution level at grid point,
    determine the month and year for it, and prepare for averaging
    '''
    grid_id = rdd[0]
    parameter = rdd[1]
    timestamp = rdd[2]
    C = rdd[3]
    month_year = datetime.strftime(timestamp, '%m%Y')
    return ((grid_id, month_year, parameter), (C, 1))


def average_over_month(rdd):
    '''
    Given rdd containing sum of air pollution level at grid point over month,
    compute the average pollution in a month for a given compound
    '''
    grid_id = rdd[0][0]
    month_year = rdd[0][1]
    timestamp = datetime.strptime(month_year, '%m%Y')
    parameter = rdd[0][2]
    C = rdd[1][0] / float(rdd[1][1])
    return (grid_id, timestamp, parameter, C)


def main():

    # Read in data from the configuration file

    config = configparser.ConfigParser()
    config.read('../setup.cfg')

    s3 = 's3a://' + config["s3"]["bucket"] + '/'
    spark_url = 'spark://' + config["spark"]["dns"]
    postgres_url = 'jdbc:postgresql://' + config["postgres"]["dns"] + '/'\
                   + config["postgres"]["db"]
    table_hourly = 'measurements_hourly'
    table_monthly = "measurements_monthly"
    postgres_credentials = {
        'user': config["postgres"]["user"],
        'password': config["postgres"]["password"]
    }
    cassandra_url = config["cassandra"]["dns"]
    cassandra_username = config["cassandra"]["user"]
    cassandra_password = config["cassandra"]["password"]

    # Create Spark context & session

    conf = SparkConf().set("spark.cassandra.connection.host", cassandra_url)\
                      .set("spark.cassandra.auth.username", cassandra_username)\
                      .set("spark.cassandra.auth.password", cassandra_password)

    sc = SparkContext(spark_url, "Batch", conf=conf)
    spark = SparkSession(sc)
    sqlContext = SQLContext(sc)

    # Schemas for converting RDDs to DataFrames & writing to DBs

    schema_hourly = StructType([
        StructField("grid_id", IntegerType(), False),
        StructField("parameter", IntegerType(), False),
        StructField("time", TimestampType(), False),
        StructField("c", FloatType(), False)
    ])

    schema_monthly = StructType([
        StructField("grid_id", IntegerType(), False),
        StructField("time", TimestampType(), False),
        StructField("parameter", IntegerType(), False),
        StructField("c", FloatType(), False)
    ])

    # Read the most recent data from Postgres to get the most recent timestamp
    # df = sqlContext.read.jdbc(url=postgres_url, table='coordinates', properties=postgres_properties)

    # Start processing data files

    # data_file = 'hourly_NONOxNOy_2010.csv'
    # data_file = 'sample'
    data_file = 'day'
    raw = s3 + data_file

    data_rdd = sc.textFile(raw)

    # Global variable STATIONS to store distances from stations to grid points
    global STATIONS
    STATIONS = get_grid_from_file("stations.json")

    # grid_df = sc.parallelize(GRID).toDF()
    # grid_df.write.jdbc(url=postgres_url, table='coordinates', mode='append', properties=postgres_properties)

    # Read query - not planning to have those currently
    # df = sqlContext.read.jdbc(url=postgres_url, table='coordinates', properties=postgres_properties)

    # df = sc.parallelize([Row(grid_id=1, time=datetime.now(), c=11)]).toDF()
    # df.write.jdbc(url=postgres_url, table='hourly', mode='append', properties=postgres_properties)

    # Compute pollution levels on the grid
    data_hourly = data_rdd\
        .map(parse_measurement_record)\
        .filter(lambda line: line is not None)\
        .flatMap(station_to_grid)\
        .reduceByKey(sum_weight_and_prods)\
        .map(calc_weighted_average_grid)\
        .persist()

    # Write them to Cassandra database
    data_hourly_df = spark\
        .createDataFrame(data_hourly, schema_hourly)\
        .sort("grid_id")\
        .write\
        .format("org.apache.spark.sql.cassandra")\
        .mode('append')\
        .options(table=table_hourly, keyspace="air")\
        .save()

    # Average pollution levels for each month
    data_monthly = data_hourly\
        .map(group_by_month)\
        .reduceByKey(sum_weight_and_prods)\
        .map(average_over_month)\
        .persist()

    # Write monthly data to Postgres database

    data_monthly_df = spark.createDataFrame(data_monthly, schema_monthly)
    data_monthly_df.write.jdbc(
        url=postgres_url, table=table_monthly,
        mode='overwrite', properties=postgres_credentials
    )

    # print(data_monthly.collect())
    # data_monthly_df.show()


if __name__ == '__main__':
    main()
