import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth
from pyspark.sql.functions import hour, weekofyear, date_format
from pyspark.sql.types import StructType as R, StructField as Fld
from pyspark.sql.types import DoubleType as Dbl, LongType as Long
from pyspark.sql.types import StringType as Str, IntegerType as Int
from pyspark.sql.types import TimestampType as Timestamp

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    '''
    Load song_data from S3 and saves it back to S3 after transformations
    Parameters:
    input_data: S3 Bucket where song_data json files are stored
    output_data: S3 Bucket where parquet will be stored
    '''
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/'
    # Define song_schema
    song_schema = R([
        Fld('artist_id', Str()),
        Fld('artist_latitude', Dbl()),
        Fld('artist_location', Str()),
        Fld('artist_longitude', Dbl()),
        Fld('artist_name', Str()),
        Fld('duration', Dbl()),
        Fld('num_songs', Int()),
        Fld('song_id', Str()),
        Fld('title', Str()),
        Fld('year', Int())
    ])
    # read song data file
    print('Reading Song_data....')
    df = spark.read.json(song_data, schema=song_schema)
    # extract columns to create songs table
    songs_table = df.select(["song_id",
                             "title",
                             "artist_id",
                             "year",
                             "duration"]).dropDuplicates(["song_id"])
    print('Created songs_table')
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(output_data+'songs/',
                              mode="overwrite",
                              partitionBy=["year", "artist_id"])
    # extract columns to create artists table
    artists_table = df.select(["artist_id",
                               "artist_name",
                               "artist_location",
                               "artist_latitude",
                               "artist_longitude"]) \
        .dropDuplicates(["artist_id"])
    print('Created artists_table')
    # write artists table to parquet files
    artists_table.write.parquet(output_data+'artists/', mode="overwrite")


def process_log_data(spark, input_data, output_data):
    '''
    Load song_data from S3 and saves it back to S3 after transformations
    Parameters:
    input_data: S3 Bucket where song_data json files are stored
    output_data: S3 Bucket where parquet will be stored
    '''
    # get filepath to log data file
    print('Reading log_data ....')
    log_data = input_data + "log_data/"
    # Define log schema
    log_schema = R([
        Fld('artist', Str()),
        Fld('auth', Str()),
        Fld('firstName', Str()),
        Fld('gender', Str()),
        Fld('itemInSession', Str()),
        Fld('lastName', Str()),
        Fld('length', Dbl()),
        Fld('level', Str()),
        Fld('location', Str()),
        Fld('method', Str()),
        Fld('page', Str()),
        Fld('registration', Dbl()),
        Fld('sessionId', Str()),
        Fld('song', Str()),
        Fld('status', Str()),
        Fld('ts', Long()),
        Fld('userAgent', Str()),
        Fld('userId', Str())
    ])
    # read log data file
    df = spark.read.json(log_data, schema=log_schema)
    # filter by actions for song plays
    df = df.where('page="NextSong"')
    # extract columns for users table
    users_table = df.select(["userId",
                             "firstName",
                             "lastName",
                             "gender",
                             "level"]).dropDuplicates(["userId"])
    print('Created users_table')
    # write users table to parquet files
    users_table.write.parquet(output_data+'users/', mode="overwrite")
    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp((x / 1000)),
                        Timestamp())
    df = df.withColumn("timestamp",
                       get_timestamp(col("ts")))
    # extract columns to create time table
    time_table = df.select(
        col('timestamp').alias('start_time'),
        hour('timestamp').alias('hour'),
        dayofmonth('timestamp').alias('day'),
        weekofyear('timestamp').alias('week'),
        month('timestamp').alias('month'),
        year('timestamp').alias('year'),
        date_format(col("timestamp"), "E").alias("weekday")
    ).dropDuplicates(["start_time"])
    print('Created time_table')
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(output_data+'time/',
                             mode="overwrite",
                             partitionBy=["year", "month"])
    # read in song data to use for songplays table
    song_df = spark.read.json(input_data+'song_data/*/*/*/*.json')
    song_df.createOrReplaceTempView("song_data")
    df.createOrReplaceTempView("log_data")
    # extract columns from joined song and log datasets
    # to create songplays table
    songplays_table = spark.sql("""
                            SELECT
                            monotonically_increasing_id() as songplay_id,
                                timestamp as start_time,
                                userId as user_id,
                                level,
                                song_id,
                                artist_id,
                                sessionId as session_id,
                                location,
                                userAgent as user_agent,
                                year(timestamp) as year,
                                month(timestamp) as month
                            FROM log_data log
                            JOIN song_data song
                            ON (log.song = song.title
                            AND log.length = song.duration
                            AND log.artist = song.artist_name)
                            """)
    print('Create songplays_table')
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(output_data+'songplays/',
                                  mode="overwrite",
                                  partitionBy=['year', 'month'])


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://spark-and-data-lake/output/"
#     input_data = 'data/'
#     output_data = 'output/'
    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
