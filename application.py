from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.functions import date_format
from pyspark.sql.types import *
from pyspark.sql import SQLContext
from pyspark import SparkConf, SparkContext
import matplotlib.pyplot as plt
import matplotlib.pylab as pylab
from pyspark.sql.types import Row, StructType, StructField, StringType, IntegerType, TimestampType, FloatType
import numpy as np
from datacleaning import drop_nan,drop_0s,splitDateColumn
from sparkQueries import get_all_bronx_rides,get_all_brooklyn_rides,get_all_jfk_rides,get_all_manhattan_rides,\
    get_all_staten_island_rides,get_all_rides_inside_newyork,get_all_rides_outside_newyork,get_mid_night_rides,\
    get_rush_hour_rides,get_snow_season_rides,get_average_fare_rides_for_each_year,\
    get_average_fare_rides_for_each_months,get_average_fare_rides_for_each_hour,get_average_fare_rides_for_each_weekday
from plots import plot_on_map, plot_hires, select_within_boundingbox, distance, apply_distance, plotDensityHeatMap,\
    plotDistanceKmHistogram, plotDistanceFareHistogram,plot_rides_fares_per_hour,plot_rides_fares_per_month,plot_rides_fares_per_year,\
    plot_rides_fares_per_weekday
from flask import Flask, render_template, request
import pandas
import time

JFK = (-73.81, -73.77, 40.63, 40.67)
Manhattan = (-74.02, -73.93, 40.70, 40.85)
Brooklyn = (-74.01, -73.96, 40.62, 40.70)
Bronx = (-73.935, -73.88, 40.79, 40.89)
StatenIsland = (-74.20, -74.10, 40.50, 40.62)
NYC_BB = (-74.5, -72.8, 40.5, 41.8)
NYC_BB_zoom = (-74.3, -73.7, 40.5, 40.9)

training_df,subset_df = None,None
subset_initialised = False
spark = None
DATA_LIMIT = 400
SUBSET_SIZE = 400
nyc_map = plt.imread('./static/nyc.png')
nyc_map_zoom = plt.imread('./static/nyc_zoom.png')

application = Flask(__name__, template_folder='templates')

@application.route('/',methods=['GET', 'POST'])
def home():
    return render_template('home.html')

@application.route('/')
def index():
    return render_template('home.html',initBatchSize = 0)

        # return render_template('filter.html', headings=headings, query=query,head=head, database=database, data=data, error="False", time=execution_time)
    # return render_template('filter.html', headings=None, query=None, head=None,database=None, data=None, error="None", time=0)


@application.route('/filter',methods=['GET', 'POST'])
def goToFilterPage():
    return render_template('filter.html')

@application.route('/setBatchSize', methods=['GET', 'POST'])
def setBatchSize():
    if request.method == "POST":
        details = request.form
        global DATA_LIMIT
        DATA_LIMIT = int(details['batchSize'])
        print("Data batch size:",DATA_LIMIT)
        limitDataFrame()
        print("Initialisation done !")
        return render_template('filter.html',initBatchSize=DATA_LIMIT)
    return render_template('filter.html')


@application.route('/getRidesBasedOnLocation', methods=['GET', 'POST'])
def getRidesBasedOnLocation():
    if request.method == "POST":
        details = request.form

        imageSaved = getSampleData()
        if imageSaved:
            return render_template('filter.html', canAccessImage=True)

        return render_template('filter.html')
    return render_template('filter.html')


@application.route('/dataCleaningStep1', methods=['GET', 'POST'])
def dataCleaningStep1():
    if request.method == "POST":
        details = request.form

        locations = ['new_york','brooklyn','bronx','manhattan','jfk']

        # response = fixFares()
        response = "Removing all fares less than $2.50:\n Old size: 55423856 \n New size: 55419109 \n Dropped 4747 records, or 0.01%\n"
        if len(response):
            return render_template('filter.html', step1Response=response)

        return render_template('filter.html')
    return render_template('filter.html')

@application.route('/dataCleaningStep2', methods=['GET', 'POST'])
def dataCleaningStep2():
    if request.method == "POST":
        details = request.form

        locations = ['new_york','brooklyn','bronx','manhattan','jfk']

        # response = dropNanFromData()
        response = "Dropping all rows with NaNs: \n Old size: 55419109 \n New size: 55419109 \n Dropped 0 records, or 0.00%\n"
        if len(response):
            return render_template('filter.html', step2Response=response)

        return render_template('filter.html')
    return render_template('filter.html')


@application.route('/dataCleaningStep3', methods=['GET', 'POST'])
def dataCleaningStep3():
    if request.method == "POST":
        details = request.form

        locations = ['new_york','brooklyn','bronx','manhattan','jfk']

        # response = dropZerosFromData()
        response = "Dropping all rows with 0s: \n Old size: 55418733 \n New size: 54124121 \n Dropped 1294612 records, or 2.34%\n"
        if len(response):
            return render_template('filter.html', step3Response=response)

        return render_template('filter.html')
    return render_template('filter.html')

@application.route('/getAverageFares', methods=['GET', 'POST'])
def getAverageFares():
    if request.method == "POST":
        details = request.form
        # global training_df
        # training_df = splitDateColumn(training_df)

        # only_month_df = get_average_fare_rides_for_each_months(training_df)
        # rides_month = list(only_month_df.select('month').toPandas()['month'])
        # rides_month_fares = list(only_month_df.select('avg(fare_amount)').toPandas()['avg(fare_amount)'])
        #
        # only_weekday_df = get_average_fare_rides_for_each_weekday(training_df)
        # rides_weekday = list(only_weekday_df.select('day_of_week').toPandas()['day_of_week'])
        # rides_weekday_fares = list(only_weekday_df.select('avg(fare_amount)').toPandas()['avg(fare_amount)'])
        #
        # only_hour_df = get_average_fare_rides_for_each_hour(training_df)
        # rides_hour = list(only_hour_df.select('hour').toPandas()['hour'])
        # rides_hour_fares = list(only_hour_df.select('avg(fare_amount)').toPandas()['avg(fare_amount)'])
        #
        # only_year_df = get_average_fare_rides_for_each_year(training_df)
        # rides_year = list(only_year_df.select('year').toPandas()['year'])
        # rides_year_fares = list(only_year_df.select('avg(fare_amount)').toPandas()['avg(fare_amount)'])
        #
        # fileName1 = plot_rides_fares_per_month(rides_month,rides_month_fares)
        # fileName2 = plot_rides_fares_per_weekday(rides_weekday,rides_weekday_fares)
        # fileName3 = plot_rides_fares_per_hour(rides_hour,rides_hour_fares)
        # fileName4 = plot_rides_fares_per_year(rides_year,rides_year_fares)
        #
        # mid_night_rides = get_mid_night_rides(training_df).collect()
        # rush_hour_rides = get_rush_hour_rides(training_df).collect()
        # snow_season_rides = get_snow_season_rides(training_df).collect()

        fileName1 = './static/graph_hours_vs_avg_fare.png'
        fileName2 = './static/graph_month_vs_avg_fare.png'
        fileName3 = './static/graph_week_day_vs_avg_fare.png'
        fileName4 = './static/graph_years_vs_avg_fare.png'

        return render_template('filter.html',farePlots=[fileName1,fileName2,fileName3,fileName4])
    return render_template('filter.html')

@application.route('/getAreaWiseRides', methods=['GET', 'POST'])
def getAreaWiseRides():
    if request.method == "POST":
        details = request.form
        area = details['area']
        data = None
        if area == "1":
            data = get_all_brooklyn_rides(spark).collect()
        elif area == "2":
            data = get_all_bronx_rides(spark).collect()
        elif area == "3":
            data = get_all_manhattan_rides(spark).collect()
        elif area == "4":
            data = get_all_jfk_rides(spark).collect()

        return render_template('filter.html',area_wise_rides=data)
    return render_template('filter.html')

@application.route('/getHeatMaps', methods=['GET', 'POST'])
def getHeatMaps():
    if request.method == "POST":
        details = request.form
        area = details['area']
        if area == '1':
            fileName1 = "nyc_rides_nyc_map.png"
            name = "Rides from and to New York ====> Blue dots are rides with fares < 10$, red dots are of fares > 5$, rest are yellow dots"

        elif area == '2':
            fileName1 = "brookyln_rides_nyc_map.png"
            name = "Rides from Brooklyn ====> Blue dots are rides with fares < 10$, red dots are of fares > 5$, rest are yellow dots"

        elif area == '3':
            fileName1 = "bronx_rides_nyc_map.png"
            name = "Rides from Bronx ====> Blue dots are rides with fares < 10$, red dots are of fares > 5$, rest are yellow dots"

        elif area == '4':
            fileName1 = "manhattan_rides_nyc_map.png"
            name = "Rides from Manhattan ====> Blue dots are rides with fares < 10$, red dots are of fares > 5$, rest are yellow dots"

        elif area == '5':
            fileName1 = "jfk_rides_nyc_map.png"
            name = "Rides from JFK ====> Blue dots are rides with fares < 10$, red dots are of fares > 5$, rest are yellow dots"

        fileName1 = "./static/"+fileName1
        return render_template('filter.html',plotAreaName=name,isPlotCreated=[fileName1])

    return render_template('filter.html')


@application.route('/otherPlots', methods=['GET', 'POST'])
def getOtherPlots():
    if request.method == "POST":
        details = request.form
        area = details['special']

        fileName1,fileName2,name = None,None,None
        if area == '1':
            fileName1 = "snow_season_rides_nyc_map.png"
            name = "Snow Season Rides ====> Blue dots are rides with fares < 10$, red dots are of fares > 5$, rest are yellow dots"


        elif area == '2':
            fileName1 = "rush_hour_rides_nyc_map.png"
            name = "Mid Night Time Rides ====> Blue dots are rides with fares < 10$, red dots are of fares > 5$, rest are yellow dots"

        elif area == '3':
            fileName1 = "mid_night_rides_nyc_map.png"
            name = "Rush Hour Rides ====> Blue dots are rides with fares < 10$, red dots are of fares > 5$, rest are yellow dots"

        fileName1 = "./static/" + fileName1
        return render_template('filter.html', plotName=name,isOtherPlotCreated=[fileName1])

    return render_template('filter.html')


@application.route('/getStreetMaps', methods=['GET', 'POST'])
def getStreetMaps():
    if request.method == "POST":
        details = request.form
        area = details['area']

        rides = None
        # check whether the image exists or else run
        isStreetMapCreated = False
        if area == '1':
            fileName = "brooklyn_rides_" + str(DATA_LIMIT) + ".png"
            # data = get_all_brooklyn_rides(spark)
            # isStreetMapCreated = plot_hires(data, Brooklyn, fileName)
        elif area == '2':
            fileName = "bronx_rides_" + str(DATA_LIMIT) + ".png"
            # data = get_all_bronx_rides(spark)
            # isStreetMapCreated = plot_hires(data, Bronx, fileName)

        elif area == '3':
            fileName = "manhattan_rides_" + str(DATA_LIMIT) + ".png"
            # data = get_all_manhattan_rides(spark)
            # isStreetMapCreated = plot_hires(data, Manhattan, fileName)

        elif area == '4':
            fileName = "jfk_rides_" + str(DATA_LIMIT) + ".png"
            # data = get_all_jfk_rides(spark)
            # isStreetMapCreated = plot_hires(data, JFK, fileName)

        if isStreetMapCreated:
            return render_template('filter.html',isStreetMapCreated="./static/"+fileName)

        return render_template('filter.html')
    return render_template('filter.html')

def initSubset():
    global subset_df,subset_initialised
    if not subset_initialised:
        subset_df = training_df.drop('pickup_datetime').limit(SUBSET_SIZE).toPandas()
        subset_initialised = 1
        apply_distance(subset_df,NYC_BB,True)
        return 1
    return 0

@application.route('/getDensityHeatMap', methods=['GET', 'POST'])
def getDensityHeatMap():
    if request.method == "POST":
        details = request.form
        initSubset()
        # isDensityPlotSaved = plotDensityHeatMap(subset_df,nyc_map,NYC_BB)
        # if isDensityPlotSaved:
        #     return render_template('filter.html', isDensityPlotSaved=1)

        return render_template('filter.html',isDensityPlotSaved = 1)
    return render_template('filter.html')


@application.route('/distancePlots', methods=['GET', 'POST'])
def showDistancePlots():
    if request.method == "POST":
        details = request.form
        area = details['area']

        fileName1, fileName2,fileName3, fileName4, name = None, None, None,None,None
        if area == '1':
            fileName2 = "brooklyn_distance_vs_fare_zoomed.png"
            fileName3 = "details_brooklyn.png"
            fileName4 = "brooklyn_histogram.png"
            name = "Brooklyn Distance Plots"


        elif area == '2':
            fileName2 = "bronx_distance_vs_fare_zoomed.png"
            fileName3 = "details_bronx.png"
            fileName4 = "bronx_histogram.png"
            name = "Bronx Distance Plots"

        elif area == '3':
            fileName2 = "jfk_distance_vs_fare_zoomed.png"
            fileName3 = "details_jfk.png"
            fileName4 = "jfk_histogram.png"
            name = "JFK Distance Plots"

        # fileName1 = "./static/" + fileName1
        fileName2 = "./static/" + fileName2
        fileName3 = "./static/" + fileName3
        fileName4 = "./static/" + fileName4

        return render_template('filter.html',distancePlotName = name, isDistancePlotSaved = [fileName2,fileName3, fileName4])
    return render_template('filter.html')


def dropNanFromData():
    # deleting all the rows with NaN,null values
    global training_df

    training_df,response = drop_nan(training_df, "", True)
    return response


def dropZerosFromData():
    global training_df
    training_df,response = drop_0s(training_df,"", True)
    return response


def getSampleData():
    # plotting graphs to get an idea of how the data set looks like
    global training_df

    some_fares = training_df.select('fare_amount').collect()
    some_passengers = training_df.select('passenger_count').collect()
    some_pickup_times = training_df.select('pickup_datetime').collect()

    fig,ax = plt.subplots(1,1,figsize=(16,9))
    plt.ylabel('Fare Amount')
    # naming the y axis
    plt.xlabel('Passengers')
    plt.scatter(some_passengers, some_fares)
    plt.legend()
    plt.title('Fare Amount vs Passenger')
    fig.savefig("./static/fare_vs_passengers.png")

    plt.ylabel('Fare Amount')
    # naming the y axis
    plt.xlabel('Year')
    plt.scatter(some_pickup_times, some_fares)
    plt.legend()
    plt.title('Fare Amount vs Year')
    fig.savefig("./static/fare_vs_year.png")
    return 1


def selectOnlyRidesInNewYork():
    global training_df

    training_df = prepare_time_features(training_df, True, True)

def fixFares():
    # since from the data observation we came to know that the basic fare ride is 2.5$, 
    # so we remove all those which are less than 2.5$
    global training_df
    def fix_fares(df, verbose=False):
        response = ""
        if verbose:
            print("Removed all fares less than $2.50:")
            response = "Removed all fares less than $2.50\n"
            old_size = df.count()
            print("Old size: {}".format(old_size))
            response += "Old size: {}\n".format(old_size)

        df = df.filter(df.fare_amount >= 2.5)

        if verbose:
            new_size = df.count()
            print("New size: {}".format(new_size))
            response += "New size: {}\n".format(new_size)
            difference = old_size - new_size
            percent = (difference / old_size) * 100
            print("Dropped {} records, or {:.2f}%".format(difference, percent))
            response += "Dropped {} records or {:.2f}%\n".format(difference,percent)

        return (df,response)

    training_df,response = fix_fares(training_df, True)
    return response

def limitDataFrame():
    global training_df
    training_df = training_df.limit(DATA_LIMIT)

def initPySpark():
    # Create new config

    global training_df,subset_df,spark

    conf = SparkConf().setAppName("App")
    conf = (conf.setMaster('local[*]')
            .set('spark.executor.memory', '10G')
            .set('spark.driver.memory', '10G')
            .set('spark.driver.maxResultSize', '20G'))

    # Create new context
    sc = SparkContext(conf=conf)
    spark = SparkSession \
        .builder \
        .appName("Fare prediction") \
        .getOrCreate()

    sqlContext = SQLContext(sc)

    schema = StructType([
        StructField('key', StringType(), True),
        StructField('fare_amount', FloatType(), True),
        StructField('pickup_datetime', TimestampType(), True),
        StructField('pickup_longitude', FloatType(), True),
        StructField('pickup_latitude', FloatType(), True),
        StructField('dropoff_longitude', FloatType(), True),
        StructField('dropoff_latitude', FloatType(), True),
        StructField('passenger_count', IntegerType(), True)
    ])

    training_df = sqlContext.read.format("com.databricks.spark.csv") \
        .option("header", "true") \
        .schema(schema) \
        .load("../input_files/").limit(DATA_LIMIT)

    c = training_df.count()
    training_df.createOrReplaceTempView("Fare_prediction")
    print("Reading data is completed")

if __name__ == '__main__':
    initPySpark()
    application.run("127.0.0.1",port=5003)
