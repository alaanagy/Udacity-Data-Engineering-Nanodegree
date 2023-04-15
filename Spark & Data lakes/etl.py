# import necessary libraries
import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, year, month, dayofmonth, hour, weekofyear, date_format, dayofweek, monotonically_increasing_id
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, IntegerType, TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Creates a new spark session  or uses the existing one.
    """
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    
    """
    Reads song data from a specific path in S3, processes that data using Spark and make transformations, finally writes them back to the
    output path into S3.
    
    Parameters:
    spark: spark session
    input_data: input data path
    output_data: output data path
    """
    
    # get filepath to song data file
    song_data = os.path.join(input_data, 'song_data/A/A/A/*.json')
    
    # Data schema
    song_schema = StructType([
        StructField("artist_id", StringType()),
        StructField("num_songs", IntegerType()),
        StructField("artist_latitude", DoubleType()),
        StructField("artist_longitude", DoubleType()),
        StructField("artist_location", StringType()),
        StructField("artist_name", StringType()),
        StructField("song_id", StringType()),
        StructField("title", StringType()),
        StructField("duration", DoubleType()),
        StructField("year", IntegerType()),
    ])    
    
    # read song data file
    song_df = spark.read.json(song_data, schema = song_schema)
    
    # extract columns to create songs table
    songs_table = song_df.select(['song_id', 'title', 'artist_id', 'year', 'duration']).dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode("overwrite").partitionBy("year", "artist_id").parquet(os.path.join(output_data, 'songs'))
    
    # extract columns to create artists table
    artists_table = song_df.select(['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']).dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(os.path.join(output_data, 'artists'))


def process_log_data(spark, input_data, output_data):
    
    """
    Reads log data from a specific path in S3, processes that data using Spark and make transformations, finally writes them back to the
    output path into S3.
    
    Parameters:
    spark: spark session
    input_data: input data path
    output_data: output data path
    """
        
    # get filepath to log data file
    log_data_path = os.path.join(input_data, 'log_data/2018/11/*.json')

    # read log data file
    log_df = spark.read.json(log_data_path )
    
    # filter by actions for song plays
    log_df = log_df.filter(log_df.page == 'NextSong')

    # extract columns for users table    
    users_table = log_df.select(['userId','firstName','lastName','gender','level']).dropDuplicates()
    
    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(os.path.join(output_data, 'users'))

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: x / 1000, TimestampType())
    log_df = log_df.withColumn("timestamp", get_timestamp(log_df.ts)) 
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x), TimestampType())
    log_df = log_df.withColumn('start_time' , get_datetime(log_df.timestamp))
    
    # extract columns to create time table
    log_df = log_df.withColumn("hour", hour("start_time")) \
        .withColumn("day", dayofmonth("start_time")) \
        .withColumn("week", weekofyear("start_time")) \
        .withColumn("month", month("start_time")) \
        .withColumn("year", year("start_time")) \
        .withColumn("weekday", dayofweek("start_time"))
    time_table = log_df.select(["start_time", "hour", "day", "week", "month", "year", "weekday"])

    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode("overwrite").partitionBy("year", "month").parquet(os.path.join(output_data, 'time'))
    
    # create view for song and log data to perform join
    log_df = log_df.withColumn('songplay_id', monotonically_increasing_id())
    song_df.createOrReplaceTempView('songs')
    log_df.createOrReplaceTempView('logs')


    # extract columns from joined song and log datasets to create songplays table
    songplays_table = spark.sql("""
    select 
            l.songplay_id,
            l.start_time,
            l.userId,
            l.level,
            s.song_id,
            s.artist_id,
            l.sessionId,
            l.location,
            l.userAgent,
            year(l.start_time) as year,
            month(l.start_time) as month
        FROM logs l
        LEFT JOIN songs s 
        ON
            l.song = s.title AND l.artist = s.artist_name 

""")

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').partitionBy('year', 'month').parquet(os.path.join(output_data, 'songplays'))


def main():
    
    """
    create spark session and go through ETL process.
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend"
    output_data = "s3a://udacity-dend-output"
    
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
