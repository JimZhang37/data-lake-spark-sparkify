import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format,dayofweek
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import monotonically_increasing_id

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS','AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS','AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = '{}song_data/*/*/*/*.json'.format(input_data)

    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create artists table
    artists_table = df.select('artist_id', df.artist_name.alias('name'), df.artist_location.alias('location'), df.artist_latitude.alias('latitude'), df.artist_longitude.alias('longitude')).dropDuplicates(['artist_id'])

    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, 'artists' ), 'overwrite')

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration').dropDuplicates(['song_id'])
    
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year','artist_id').parquet(os.path.join(output_data, 'songs' ), 'overwrite')




def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    # todo(change log path)
    log_data = '{}log-data'.format(input_data)
    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.filter("page = 'NextSong'")

    # extract columns for users table    
    users_table = df.select(df.userId.cast(IntegerType()).alias('user_id'), \
                    df.firstName.alias('first_name'), \
                    df.lastName.alias('last_name'), \
                    'gender', \
                    'level')\
                    .dropDuplicates().dropna()


    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, 'users' ), 'overwrite')


    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x / 1000.0))

    df = df.withColumn('ts', get_timestamp(df.ts))

    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(x))
    df = df.withColumn('start_time', get_datetime(df.ts))


    # extract columns to create time table
    time_table = df.select('start_time') \
                    .withColumn('hour', hour(df.start_time)) \
                    .withColumn('day', dayofmonth(df.start_time)) \
                    .withColumn('week', weekofyear(df.start_time)) \
                    .withColumn('month', month(df.start_time)) \
                    .withColumn('year', year(df.start_time)) \
                    .withColumn('weekday', dayofweek(df.start_time))# weekday TODO(1)

    #write time table to parquet files partitioned by year and month

    time_table.write.partitionBy('year','month').parquet(os.path.join(output_data, 'time' ), 'overwrite')

    # read in song data to use for songplays table
    song_df = spark.read.json('{}song_data/*/*/*/*.json'.format(input_data))

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df \
                .join(song_df, \
                    (song_df.artist_name == df.artist) & \
                    (song_df.title == df.song) & \
                    (df.length ==song_df.duration)) \
                .withColumn('year', year(df.start_time)) \
                .withColumn('month', month(df.start_time)) \
                .withColumn('songplay_id', monotonically_increasing_id()) \
                .select('songplay_id', \
                        'start_time', \
                        df.userId.alias('user_id'), \
                        'level', \
                        'song_id', \
                        'artist_id', \
                        df.sessionId.cast(IntegerType()).alias('session_id'), \
                        'location', \
                        df.userAgent.alias('user_agent'), \
                        'year', \
                        'month') 



    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year','month').parquet(os.path.join(output_data, 'songplays' ), 'overwrite')



def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://emryh/"
    # input_data = "data/"
    # output_data = "new/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
