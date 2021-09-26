import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import IntegerType,TimestampType


config = configparser.ConfigParser()
config.read('./dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_CREDENTIALS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_CREDENTIALS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    '''
    Creates a spark session using hadoop 2.7.0.
    '''
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """Creates songs table and artists table in parquet format based on the song_data json input files.

    Args:
        spark (object): Spark sesssion object.
        input_data (string): The song data json file path. 
                            Wild card can be used to assign a folder with multiples files.
        output_data (string): The folder where the parquet files will be stored.
    """

    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(['song_id', 'title', 'duration',
                            'year', 'artist_name','artist_id']).dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(output_data + 'songs_table.parquet', mode='overwrite')

    # extract columns to create artists table
    artists_table = df.select(['artist_id', 'artist_name', 'artist_location',
                                'artist_latitude', 'artist_longitude']).dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + 'artists_table.parquet', mode='overwrite')


def process_log_data(spark, input_data, output_data):
    """Creates users and songplays table in parquet format based on the log_data json input files.

    Args:
        spark (object): Spark sesssion object.
        input_data (string): The log_data json file path. 
                            Wild card can be used to assign a folder with multiples files.
        output_data (string): The folder where the parquet files will be stored.
    """
    # get filepath to log data file
    log_data = input_data + 'log-data/*/*/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.where(df.page == 'NextSong')

    # extract columns for users table    
    users_table = df.select(['userId', 'firstName', 'lastName',
                            'gender', 'level']).dropDuplicates()
    
    # write users table to parquet files
    users_table.write.parquet(output_data + 'users_table.parquet', mode='overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: x/1000, IntegerType())
    df = df.withColumn('start_timestamp', get_timestamp('ts'))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x), TimestampType())
    df = df.withColumn('start_datetime', get_datetime('start_timestamp'))
    
    # extract columns to create time table
    time_table = df.select(['start_timestamp', 'start_datetime']).dropDuplicates()
    time_table = time_table.select([col('start_timestamp').alias('timestamp'), col('start_datetime').alias('datetime'), 
                            year('start_datetime').alias('year'), month('start_datetime').alias('month')])

    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(output_data + 'time_table.parquet', mode='overwrite')

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + 'songs_table.parquet', mode='overwrite')

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df,
                                (df.artist == song_df.artist_name) & (df.song == song_df.title) 
                                & (df.length == song_df.duration),
                                'left_outer').select(
                                                    df.start_timestamp,
                                                    col('userId').alias('user_id'),
                                                    df.level,
                                                    song_df.song_id,
                                                    song_df.artist_id,
                                                    col('sessionId').alias('session_id'),
                                                    df.location,
                                                    col('useragent').alias('user_agent')
                                                    )

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(output_data + 'songplays.parquet', mode='overwrite')


def main():
    """
    - Creates a spark session
    - Process the song_data creating songs and artists parquet file.
    - Process the log_data creating users and songplays parquet file.
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://datalake-project-udct-zen/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
