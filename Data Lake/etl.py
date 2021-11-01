import configparser
from datetime import datetime
import os
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import *


config = configparser.ConfigParser()
config.read('dl.cfg') # Read config file for connecting to aws services

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    '''
    - Description : Create or get spark session
    - Returns : spark session
    '''
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    '''
    Description : 
        - Create song_data spark object from all the song dataset files(.json format) present in S3
        - Create spark dataframe from all files
        - Extract columns from dataframe to create songs_table
        - Write songs_table to parquet file partitioned by year and artist in S3
        - Extract columns from dataframe to create artists_table
        - Write artists_table to parquet file in S3
    Arguments : 
        - spark : spark session
        - input_data : source file path
        - output_data : output file path
    Returns : 
        - None
    '''
    # get filepath to song data file
    song_data =os.path.join(input_data, "song_data/*/*/*/*.json")
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select('song_id','title','artist_id','year','duration').where(col('song_id').isNotNull()).dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year","artist_id").mode('overwrite').parquet("{}/songs/songs_table.parquet".format(output_data))

    # extract columns to create artists table
    artists_table = df.select('artist_id',
                          col('artist_name').alias('name'),
                          col('artist_location').alias('location'),
                          col('artist_latitude').alias('latitude'),
                          col('artist_longitude').alias('longitude')).where(col('artist_id').isNotNull()).dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet("{}/songs/artists_table.parquet".format(output_data))


def process_log_data(spark, input_data, output_data):
    '''
    Description : 
        - Create log_data spark object from all the user log dataset files(.json format) present in S3
        - Create spark dataframe from all files
        - Filter the dataframe by 'NextPage' page
        - Extract columns for users_table from dataframe and write to parquet file in S3
        - Create timestamp and datetime columns from original timestamp column from source
        - Extract columns to create time_table and write to parquet file format partitioned by year and month in S3
        - Create song_df dataframe from song dataset to get song and artist informations for the song being listened by user
        - Extract user information from user log dataframe and join with song dataframe to create songplays_table.
        - Write songplay_table file to parquet file format partitioned by year and month in S3
    
    Arguments : 
        - spark : spark session 
        - input_data :  source file path
        - output_data : output file path
    Returns : 
        - None 
    '''
    # get filepath to log data file
    log_data =os.path.join(input_data, "log_data/*/*/*.json")

    # read log data file
    df = spark.read.json(log_data)
    df = df.withColumn('user_id', df.userId.cast(IntegerType()))
    
    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")

    # extract columns for users table    
    users_table = df.select('user_id',
                         col('firstName').alias('first_name'),
                         col('lastName').alias('last_name'),
                         col('gender').alias('gender'),
                         'level').where(col('user_id').isNotNull()).dropDuplicates()
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet("{}/songs/users_table.parquet".format(output_data))

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x : datetime.utcfromtimestamp(int(x)/1000.0), TimestampType())
    df = df.withColumn('timestamp', get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.utcfromtimestamp(int(x)/1000.0),DateType())
    df = df.withColumn('date', get_datetime(df.ts))
    
    # extract columns to create time table
    time_table = df.select(col('timestamp').alias('start_time'),
                           hour('timestamp').alias('hour'),
                           dayofmonth('date').alias('day'),
                           weekofyear('date').alias('week'),
                           month('date').alias('month'),
                           year('date').alias('year'),
                           date_format('date','E').alias('weekday')).dropDuplicates()                           
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year","month").mode('overwrite').parquet("{}/songs/time_table.parquet".format(output_data))

    # read in song data to use for songplays table
    song_df = spark.read.json(os.path.join(input_data, 'song_data', '*', '*', '*'))

    # extract columns from joined song and log datasets to create songplays table 
    df = df.orderBy('timestamp')
    df = df.withColumn('songplay_id',F.monotonically_increasing_id()) #generating serial id for songplay table
    songplays_table = df.join(song_df, 
                              (df['song'] == song_df['title']) & 
                              (df['length'] == song_df['duration']) & 
                              (df['artist'] == song_df['artist_name']),'left_outer').select(
    df.songplay_id,
    df.timestamp.alias('start_time'),
    df.user_id,
    df.level,
    song_df.song_id,
    song_df.artist_id,
    df.sessionId.alias('session_id'),
    df.location,
    df.userAgent.alias('user_agent'),
    year('date').alias('year'),
    month('date').alias('month'))

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year","month").mode('overwrite').parquet("{}/songs/songplays_table.parquet".format(output_data))


def main():
    '''
    Description : 
        - Call function to create spark session
        - Call process_song_data,process_log_data functions to do ETL process to load into S3 bucket
    Arguments :
        - None
    Returns :
        - None
    '''
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://sparkify-out/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
