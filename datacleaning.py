# dropping all the rows with zeroes in their columns
from pyspark.sql.functions import date_format

isDateColumnSplitDone = 0

def splitDateColumn(df):
    global isDateColumnSplitDone
    if not isDateColumnSplitDone:
        df = prepare_time_features(df)
        isDateColumnSplitDone = 1
    return df

def prepare_time_features(df, verbose=False, drop=False):
    df = df.withColumn('year', date_format('pickup_datetime', 'y')).withColumn('month',
                                                                               date_format('pickup_datetime', 'M')) \
        .withColumn('day_of_week', date_format('pickup_datetime', 'E')).withColumn('hour',
                                                                                   date_format('pickup_datetime', 'HH')) \
        .withColumn('time', date_format('pickup_datetime', 'HH:mm:ss'))

    if drop:
        df = df.drop("pickup_datetime")
        df.show(2)

    return df

def drop_0s(df, response, verbose=False):
    if verbose:
        print("Dropping all rows with 0s:")
        response += "Dropping all rows with 0s:\n"
        old_size = df.count()
        print("Old size: {}".format(old_size))
        response += "Old size: {}\n".format(old_size)

    df = df.filter(
        (df["pickup_longitude"] != 0.0) & (df["dropoff_longitude"] != 0.0) & (df["dropoff_latitude"] != 0.0) \
        & (df["pickup_latitude"] != 0.0) & (df["passenger_count"] != 0.0))

    if verbose:
        new_size = df.count()
        print("New size: {}".format(new_size))
        response += "New size: {}\n".format(new_size)
        difference = old_size - new_size
        percent = (difference / old_size) * 100
        print("Dropped {} records, or {:.2f}%".format(difference, percent))
        response += "Dropped {} records, or {:.2f}%\n".format(difference, percent)

    return df, response


def drop_nan(df, response, verbose=False):
    if verbose:
        print("Dropping all rows with NaNs:")
        response+="Dropping all rows with NaNs\n"
        old_size = df.count()
        print("Old size: {}\n".format(old_size))

    df.dropna()

    if verbose:
        new_size = df.count()
        print("New size: {}".format(new_size))
        response+="New size: {}\n".format(new_size)
        difference = old_size - new_size
        percent = (difference / old_size) * 100
        print("Dropped {} records, or {:.2f}%".format(difference, percent))
        response+="Dropped {} records, or {:.2f}%\n".format(difference, percent)
    return df.na.drop(),response