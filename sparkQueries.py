
from pyspark.sql.functions import date_format
from pyspark.sql.types import *
from pyspark.sql import SQLContext
from pyspark import SparkConf, SparkContext

def get_all_brooklyn_rides(spark):
    df = spark.sql("select * from Fare_prediction where pickup_longitude between -74.01 and -73.96 and pickup_latitude between 40.62 and 40.70 ")
    return df

def get_all_bronx_rides(spark):
    df = spark.sql("select * from Fare_prediction where pickup_longitude between -73.935 and -73.88 and pickup_latitude between 40.79  and 40.89 ")
    return df

def get_all_jfk_rides(spark):
    df = spark.sql("select * from Fare_prediction where pickup_longitude between -73.81 and -73.77 and pickup_latitude between 40.63 and 40.67 ")
    return df

def get_all_manhattan_rides(spark):
    df = spark.sql("select * from Fare_prediction where pickup_longitude between -74.02 and -73.93 and pickup_latitude between 40.70 and 40.85 ")
    return df

def get_all_staten_island_rides(spark):
    df = spark.sql("select * from Fare_prediction where pickup_longitude between -74.20 and -74.10 and pickup_latitude between 40.50 and 40.62 ")
    return df

def get_all_rides_outside_newyork(spark):
    df = spark.sql("select * from Fare_prediction where pickup_longitude between -73.935 and -73.88 and pickup_latitude between 40.79  and 40.89 ")
    return df

def get_all_rides_inside_newyork(spark):
    df = spark.sql("select * from Fare_prediction where pickup_longitude between -74.5 and -72.8 and pickup_latitude between 40.573143 and 41.709555 and dropoff_longitude between -74.252193 and -72.986532 and dropoff_latitude between 40.5 and 41.8")
    return df




def get_rush_hour_rides(df):
    df = df.select('*').where(df.hour.between(16,20))
    return df
def get_mid_night_rides(df):
    df = df.select('*').where(~df.hour.between(7,22))
    return df
def get_snow_season_rides(df):
    df = df.select('*').where(df.month.between(1,2))
    return df

def get_average_fare_rides_for_each_months(df):
    df = df.groupBy("month").avg("fare_amount")
    return df

def get_average_fare_rides_for_each_hour(df):
    df = df.groupBy("hour").avg("fare_amount")
    return df

def get_average_fare_rides_for_each_weekday(df):
    df = df.groupBy("day_of_week").avg("fare_amount")
    return df

def get_average_fare_rides_for_each_year(df):
    df = df.groupBy("year").avg("fare_amount")
    return df