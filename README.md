# AirAware

The goal of my Insight Data Engineering project was to develop a data pipeline and Web platform for monitoring air quality at arbitrary United States locations. My data application could be used by analytics teams to understand the effect of pollution on health, with the specific focus on asthma and preventing its attacks.

Screencast: https://youtu.be/82MFrfjmKKA

## Table of contents
1. [Introduction](README.md#introduction)
2. [Practical significance](README.md#practical-significance)
3. [Data pipeline](README.md#data-pipeline)
    * [Data extraction](README.md#data-extraction)
    * [Data transformation](README.md#data-transformation)
    * [Data loading](README.md#data-loading)
4. [Challenges](README.md#challenges)
    * [Computation](README.md#computation)
    * [Storage](README.md#storage)
    * [Complex task automation](README.md#complex-task-automation)
5. [Technologies](README.md#technologies)

## Introduction

Air pollution is a serious health hazard. It causes conditions such as asthma, lung cancer, and heart diseases, and is responsible for 1.3 million deaths worldwide [[link](http://www.who.int/ceh/risks/cehair/en/)]. In the Los Angeles Basin and San Joaquin Valley of Southern California alone, considerably more people die prematurely from air pollution than from auto collisions [[link](http://calstate.fullerton.edu/news/2008/091-air-pollution-study.html)].

To minimize the effects of air pollution on health, more information is needed about the level of pollutants such as ozone, sulfur oxide, and solid particles in the air, at various locations. Environmental Protection Agency (EPA) routinely measures concentrations of various pollutants in the atmosphere, and openly releases these data. The air quality data are accumulated at air control stations that aren't usually located in residential areas. This project will use the EPA datasets to estimate the content of pollutants in the atmosphere at arbitrary locations, by computing average pollution levels between different stations.

The author's vision for the product is the application that would show the air quality at an arbitrary location, as well as let user download cleaned up historical data for this location. The first step, accomplished in this Insight project, implements this functionality for the United States.

## Practical significance

The result of the project will be a Web application showing pollution levels at an arbitrary U.S. address. It could be used for monitoring local air quality and providing warnings when air quality is unacceptable. The broader goal of this project is to build a platform for data analytics teams to help develop new predictive models of air quality.

## Data pipeline

### Data extraction

The complete measurement data is available free of charge on [EPA website](https://aqs.epa.gov/aqsweb/airdata/download_files.html#Raw) for the years 1980-2017, measured every hour at all locations across US. The amount of data is ~10 Gb/year for years after 2000. The data is constantly updated. Extraction step consists in downloading data into an Amazon S3 storage bucket, then loading it into Spark's RDD object and cleaning it up.

### Data transformation

The main data transformation in this project is calculation of air quality levels at arbitrary grid points. The grid constructed in this work contains 100,000 points, ensuring sufficiently fine mesh, with any arbitrary U.S. address being at most ~15 miles away from one of the grid points. Computing pollution levels at grid points is a spacial inter(extra)polation problem, with several common ways to address it [[link](http://www.integrated-assessment.eu/eu/guidebook/spatial_interpolation_and_extrapolation_methods.html)]. Since the focus of this project is on the data pipelines rather than models, I decided to use more common inverse distance weighting to estimate pollution levels on the grid.

After the calculations, several metrics are computed, including averages and percentiles, to provide more detail about the air quality in the area.

### Data loading

Cleaned data, as well as various pollution metrics are loaded into the PostgreSQL database with PostGIS extension for an easier location-based search. Detailed data in the hourly resolution is also loaded into Cassandra database, to be used for historical data retrieval.

## Challenges

### Computation

Interpolation geospatial data is expensive---for each time step, around 50 million unique combinations of stations/grid points need to be considered, for each pollutant. In addition, each of these calculations involve computing distance between two geographical points, using 5 trigonometric function estimations. Doing these calculations brute-force would result in unacceptably long waiting times. To mitigate the computational cost, I precomputed distances between the grid points and stations, and used this information in my calculations.

### Storage

Once the computation for each given moment of time is complete, the resulting map overlay needs to be stored in the database for all the points on the map, as a time series. This means storing ~100,000 points for every hour in the day, for almost 40 years of observation history. Within this vast amount of data, my application needs to have a way to efficiently locate a grid point which is close to a given address.

Given these unique data organization challenges, a two-database storage scheme seems to be appropriate. A PostgreSQL database with PostGIS extension is used for quick location look-up and presenting rough aggregated historical data, and Cassandra distributed database is used for full historical data look-up, at hourly resolution.

### Complex task automation

EPA updates pollution data on hourly basis. Building automatic tools for ingesting the data, processing it, and updating tables in databases is a challenge that has to be addressed in my applications.

## Technologies

Taking into account the unique challenges of my project and technical trade-offs that had to be made when working on it, I designed the following data pipeline for my application:

1. Store raw sensory data obtained from EPA (Amazon S3)
2. Load EPA data into a Spark batch job; compute the pollution map, averages over long periods of time, and various analytic quantities;
3. Store computed map and aggregated quantities in a PostgreSQL database with PostGIS extension for quick location look-up; store full historical data at hourly resolution in a Cassandra database;
4. In a Flask application, use Google Maps API to find latitude and longitude of a given U.S. address; locate a grid point closest to it, and retrieve data for this grid point.

![Project's pipeline](./pipeline.png)
