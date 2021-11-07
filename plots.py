import matplotlib.pyplot as plt
import numpy as np
from pyspark.sql.functions import date_format
from pyspark.sql.types import *
from pyspark.sql import SQLContext
from pyspark import SparkConf, SparkContext

density_pickup,density_dropoff = None,None

def plot_on_map(df, BB, nyc_map,fileName, s=10, alpha=0.2):
    fig, axs = plt.subplots(1, 2, figsize=(16, 10))

    p1 = df.select('pickup_longitude').collect()
    p2 = df.select('pickup_latitude').collect()
    p3 = df.select('dropoff_longitude').collect()
    p4 = df.select('dropoff_latitude').collect()

    axs[0].scatter(p1, p2, zorder=1, alpha=alpha, c='r', s=s)
    axs[0].set_xlim((BB[0], BB[1]))
    axs[0].set_ylim((BB[2], BB[3]))
    axs[0].set_title('Pickup locations')
    axs[0].imshow(nyc_map, zorder=0, extent=BB)

    axs[1].scatter(p3, p4, zorder=1, alpha=alpha, c='r', s=s)
    axs[1].set_xlim((BB[0], BB[1]))
    axs[1].set_ylim((BB[2], BB[3]))
    axs[1].set_title('Dropoff locations')
    axs[1].imshow(nyc_map, zorder=0, extent=BB)
    fig.savefig('./static/'+fileName)
    return 1


def plot_hires(df, BB,fileName, figsize=(12, 12), ax=None, c=('r', 'b')):
    if ax == None:
        fig, ax = plt.subplots(1, 1, figsize=figsize)

    f1 = df.select('pickup_longitude').collect()
    f2 = df.select('pickup_latitude').collect()
    f3 = df.select('dropoff_longitude').collect()
    f4 = df.select('dropoff_latitude').collect()

    ax.scatter(f1, f2, c=c[0], s=0.01, alpha=0.5)
    ax.scatter(f3, f4, c=c[1], s=0.01, alpha=0.5)
    fig.savefig('./static/'+fileName)
    return 1



def select_within_boundingbox(df, BB):
    filter_df = df.filter((df['pickup_longitude'] >= BB[0]) & (df['pickup_longitude'] <= BB[1]) & \
                          (df['pickup_latitude'] >= BB[2]) & (df['pickup_latitude'] <= BB[3]) & \
                          (df['dropoff_longitude'] >= BB[0]) & (df['dropoff_longitude'] <= BB[1]) & \
                          (df['dropoff_latitude'] >= BB[2]) & (df['dropoff_latitude'] <= BB[3]))

    return filter_df


def distance(lat1, lon1, lat2, lon2):
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)
    """
    ## Convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(np.radians, [lon1, lat1, lon2, lat2])

    ## Haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = np.sin(dlat / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2) ** 2
    c = 2 * np.arcsin(np.sqrt(a))
    r = 6371  # Radius of earth in kilometers. Use 3956 for miles
    return c * r


def apply_distance(df, NYC_BB,verbose=False):
    df['distance_km'] = distance(df.pickup_latitude, df.pickup_longitude,
                                 df.dropoff_latitude, df.dropoff_longitude)
    # if verbose:
    #     display(df.distance_km.describe())

    global density_pickup,density_dropoff
    ## First calculate two arrays with datapoint density per sq km
    n_lon, n_lat = 200, 200  # number of grid bins per longitude, latitude dimension
    density_pickup, density_dropoff = np.zeros((n_lat, n_lon)), np.zeros((n_lat, n_lon))  # Prepare arrays

    """
    To calculate the number of datapoints in a grid area, the numpy.digitize() function is used. 
    This function needs an array with the (location) bins for counting the number of datapoints per bin.
    """
    bins_lon = np.zeros(n_lon + 1)  # bin
    bins_lat = np.zeros(n_lat + 1)  # bin

    delta_lon = (NYC_BB[1] - NYC_BB[0]) / n_lon  # bin longutide width
    delta_lat = (NYC_BB[3] - NYC_BB[2]) / n_lat  # bin latitude height

    bin_width_kms = distance(NYC_BB[2], NYC_BB[1], NYC_BB[2], NYC_BB[0]) / n_lon  # Bin width in kms
    bin_height_kms = distance(NYC_BB[3], NYC_BB[0], NYC_BB[2], NYC_BB[0]) / n_lat  # Bin height in kms

    for i in range(n_lon + 1):
        bins_lon[i] = NYC_BB[0] + i * delta_lon

    for j in range(n_lat + 1):
        bins_lat[j] = NYC_BB[2] + j * delta_lat

    ## Digitize per longitude, latitude dimension
    inds_pickup_lon = np.digitize(df.pickup_longitude, bins_lon)
    inds_pickup_lat = np.digitize(df.pickup_latitude, bins_lat)
    inds_dropoff_lon = np.digitize(df.dropoff_longitude, bins_lon)
    inds_dropoff_lat = np.digitize(df.dropoff_latitude, bins_lat)

    """
    Count per grid bin
    note: as the density_pickup will be displayed as image, the first index is the y-direction, 
          the second index is the x-direction. Also, the y-direction needs to be reversed for
          properly displaying (therefore the (n_lat-j) term)
    """
    dxdy = bin_width_kms * bin_height_kms

    for i in range(n_lon):
        for j in range(n_lat):
            density_pickup[j, i] = np.sum((inds_pickup_lon == i + 1) & (inds_pickup_lat == (n_lat - j))) / dxdy
            density_dropoff[j, i] = np.sum((inds_dropoff_lon == i + 1) & (inds_dropoff_lat == (n_lat - j))) / dxdy
    return df

def plotDensityHeatMap(subset_df,nyc_map,NYC_BB):
    ## Plot the density arrays
    fig, axs = plt.subplots(2, 1, figsize=(18, 24))
    # apply_distance(subset_df, True)
    print("********************",density_pickup,density_dropoff)
    axs[0].imshow(nyc_map, zorder=0, extent=NYC_BB);
    im = axs[0].imshow(np.log1p(density_pickup), zorder=1, extent=NYC_BB, alpha=0.6, cmap='plasma')
    axs[0].set_title('Pickup density [datapoints per sq km]')
    cbar = fig.colorbar(im, ax=axs[0])
    cbar.set_label('log(1 + # datapoints per sq km)', rotation=270)

    axs[1].imshow(nyc_map, zorder=0, extent=NYC_BB);
    im = axs[1].imshow(np.log1p(density_dropoff), zorder=1, extent=NYC_BB, alpha=0.6, cmap='plasma')
    axs[1].set_title('Dropoff density [datapoints per sq km]')
    cbar = fig.colorbar(im, ax=axs[1])
    cbar.set_label('log(1 + # datapoints per sq km)', rotation=270)
    fig.savefig('./static/density_heat_map.png')
    return 1


def plotDistanceKmHistogram(subset_df):
    ## Add new column to dataframe with distance in kms
    # subset_df = apply_distance(subset_df, True)
    fig,ax = plt.subplots(1, 1, figsize=(16, 8))
    subset_df.distance_km.hist(bins=50, figsize=(16, 8))
    plt.xlabel('Distance in km')
    plt.xlabel('Count of Rides')
    plt.title('Histogram of rides vs distances in km')
    fig.savefig('./static/distance_vs_histogram.png')
    meanValue = subset_df.groupby('passenger_count')['distance_km', 'fare_amount'].mean()
    response = "Mean of distance in km and fare amount grouped by passenger count"+str(meanValue)+"\n"
    print("Average $USD/km : {:0.2f}".format(subset_df.fare_amount.sum() / subset_df.distance_km.sum()))
    response +="Average $USD/km : {:0.2f}\n".format(subset_df.fare_amount.sum() / subset_df.distance_km.sum())
    return response

def plotDistanceFareHistogram(subset_df):
    ## Scatter plot distance - fare
    fig, axs = plt.subplots(1, 2, figsize=(16,9))
    axs[0].scatter(subset_df.distance_km, subset_df.fare_amount, alpha=0.2)
    axs[0].set_xlabel('Distance in km')
    axs[0].set_ylabel('Fare in $USD')
    axs[0].set_title('All data')

    ## Zoom in on part of data
    idx = (subset_df.distance_km < 30) & (subset_df.fare_amount < 100)
    axs[1].scatter(subset_df[idx].distance_km, subset_df[idx].fare_amount, alpha=0.2)
    axs[1].set_xlabel('Distance in km')
    axs[1].set_ylabel('Fare in $USD')
    axs[1].set_title('Zoom in on distance < 30 km, fare < $100')
    fig.savefig('./static/distance_vs_fare.png')
    return 1


def plot_rides_fares_per_month(x,y):
    # x_axis = [float(val) for val in x]
    y_axis = [float(val) for val in y]
    print(x,y_axis)
    y_axis = [float(val) for val in y]
    print(x,y_axis)
    path = './static/avg_fare_per_month.png'
    fig, axs = plt.subplots(1, 1, figsize=(12, 8))
    axs.scatter(x, y_axis, alpha=1)
    axs.set_xlabel('Month')
    axs.set_ylabel('Average Fare in $USD')
    axs.set_title('Average Fare per month')
    fig.savefig(path)
    return path

def plot_rides_fares_per_weekday(x,y):
    y_axis = [float(val) for val in y]
    print(x,y_axis)
    path = './static/avg_fare_per_weekday.png'

    fig, axs = plt.subplots(1, 1, figsize=(12, 8))
    axs.scatter(x, y_axis, alpha=1)
    axs.set_xlabel('Week Day')
    axs.set_ylabel('Average Fare in $USD')
    axs.set_title('Average Fare per week day')
    fig.savefig(path)
    return path

def plot_rides_fares_per_hour(x,y):
    y_axis = [float(val) for val in y]
    print(x,y_axis)
    path = './static/avg_fare_per_hour.png'
    fig, axs = plt.subplots(1, 1, figsize=(12, 8))
    axs.scatter(x, y_axis, alpha=1)
    axs.set_xlabel('Hour')
    axs.set_ylabel('Average Fare in $USD')
    axs.set_title('Average Fare per hour')
    fig.savefig(path)
    return path

def plot_rides_fares_per_year(x,y):
    y_axis = [float(val) for val in y]
    print(x,y_axis)
    path = './static/avg_fare_per_year.png'
    fig, axs = plt.subplots(1, 1, figsize=(12, 8))
    axs.scatter(x, y_axis, alpha=1)
    axs.set_xlabel('Year')
    axs.set_ylabel('Average Fare in $USD')
    axs.set_title('Average Fare per year')
    fig.savefig(path)
    return path