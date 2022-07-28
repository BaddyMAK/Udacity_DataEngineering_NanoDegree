import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format,dayofweek
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, IntegerType, LongType



config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get("aws",'AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get("aws",'AWS_SECRET_ACCESS_KEY')

def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    This function performs the next steps: 
     1- Extract or read JSON input files (from either local or S3 directory) using the given input_data path
     2- Transform input data into two tables or two dimensional tables : songs and artists tables
     3- Load the created tables in parquet format into S3
        
    Take as input: 
        - spark: Our Spark session.
        - input_data: path for given input data in this case is song_data
        - output_data: where to save or write tables (written in parquet files) after creation .
    
    Give as output: 
    
        - songs     table contains details about songs 
        - artists   table contains details about artists
    """
    
    # get filepath to song data file
    local_song='song_data/*/*/*/*.json' # This is only for local test
    song_data_local = input_data + local_song
    song_data_s3=input_data+'song_data'
    song_data=song_data_local

    # define the song schema
    song_schema = StructType([
        StructField("num_songs", IntegerType()),
        StructField("artist_id", StringType()),
        StructField("artist_name", StringType()),
        StructField("artist_latitude", DoubleType()),
        StructField("artist_longitude", DoubleType()),
        StructField("artist_location", StringType()),
        StructField("song_id", StringType()),
        StructField("title", StringType()),
        StructField("duration", DoubleType()),
        StructField("year", IntegerType()),
    ])

    # read song data file
    df = spark.read.json(song_data, schema=song_schema)

    # extract columns to create songs table
    songs_table = df.select(["song_id","title","artist_id","year", "duration"]).dropDuplicates()

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year","artist_id").parquet(output_data+"/songs_table.parquet")


    # extract columns to create artists table
    artists_table = df.select(["artist_id","artist_name","artist_location","artist_latitude", "artist_longitude"]).dropDuplicates()

    # write artists table to parquet files
    artists_table.write.parquet(output_data+"/artists_table.parquet")



def process_log_data(spark, input_data, output_data):
    """
    This function performs the next steps: 
     1- Extract or read JSON input files (from either local or S3 directory) using the given input_data path
     2- Transform input data into three tables (one fact table called songplays and two dimensional tables : users and time tables)
     3- Load the created tables in parquet format into S3
        
    Take as input: 
        - spark: Our Spark session.
        - input_data: path for given input data in this case is log_data
        - output_data: where to save or write tables (written in parquet files) after creation .
    
    Give as output: 
    
        - users     table contains details about users 
        - time      table contains details about the time, date, year month, week etc. saved under output folder
        - songplays table contains details about songplays stored in output_data path.
    """
    # get filepath to log data file
    local_log='log_data/*.json' # this is used only to test locally
    log_data_local=input_data + local_log
    log_data_s3=input_data+'log_data'
    log_data=log_data_local


    # define the log schema
    log_schema = StructType([
        StructField("artist", StringType()),
        StructField("auth", StringType()),
        StructField("firstName", StringType()),
        StructField("gender", StringType()),
        StructField("itemInSession", IntegerType()),
        StructField("lastName", StringType()),
        StructField("length", DoubleType()),
        StructField("level", StringType()),
        StructField("location", StringType()),
        StructField("method", StringType()),
        StructField("page", StringType()),
        StructField("registration", DoubleType()),
        StructField("sessionId", IntegerType()),
        StructField("song", StringType()),
        StructField("status", IntegerType()),
        StructField("ts", LongType()),
        StructField("userAgent", StringType()),
        StructField("userId", IntegerType()),
    ])

    # read log data file
    df = spark.read.json(log_data, schema=log_schema)

    # filter by actions for song plays
    df = df.filter(df.page == "NextSong").dropDuplicates()

    # extract columns for users table
    # I will try second method here
    df.createOrReplaceTempView("my_log_table")
    users_table = spark.sql("""
                              SELECT
                              userId as user_id,
                              firstName as first_name,
                              lastName as last_name,
                              gender as gender,
                              level as level
                              FROM my_log_table
                              """)

    # write users table to parquet files
    users_table.write.parquet(output_data+"/users_table.parquet")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.datetime.fromtimestamp(x/1000.0))
    df = df.withColumn("timestamp", get_timestamp(df.ts))

    # create datetime column from original timestamp column
    get_datetime = udf(lambda x:x.strftime('%Y-%m-%d %H:%M:%S'))
    df = df.withColumn("datetime", get_datetime(df.timestamp))

    # extract columns to create time table
    time_table= df.select(["datetime"]).dropDuplicates()

    #Extracting the hour
    hour = udf(lambda x: x.hour, IntegerType())
    time_table=time_table.withColumn("hour", hour(time_table.datetime))
    time_table=time_table.withColumn("month", month(time_table.datetime))
    time_table=time_table.withColumn("weekday", dayofweek(time_table.datetime))
    time_table=time_table.withColumn("day", dayofmonth(time_table.datetime))
    time_table=time_table.withColumn("week", weekofyear(time_table.datetime))
    time_table=time_table.withColumn("year", year(time_table.datetime))



    #to update the log table
    time_table.createOrReplaceTempView("my_timetable")
    time_table = spark.sql("""
                           SELECT
                           t2.datetime as start_time,
                           t2.hour as hour,
                           t2.day as day,
                           t2.week as week,
                           t2.month as month,
                           t2.year as year,
                           t2.weekday as weekday
                           FROM my_timetable t2
                              """)

    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year","month").parquet(output_data+"/time_table.parquet")

    # read in song data to use for songplays table
    song_data = input_data +'song_data'
    # define the song schema
    song_schema = StructType([
        StructField("num_songs", IntegerType()),
        StructField("artist_id", StringType()),
        StructField("artist_name", StringType()),
        StructField("artist_latitude", DoubleType()),
        StructField("artist_longitude", DoubleType()),
        StructField("artist_location", StringType()),
        StructField("song_id", StringType()),
        StructField("title", StringType()),
        StructField("duration", DoubleType()),
        StructField("year", IntegerType()),
    ])

    # read song data file
    song_df = spark.read.json(song_data, schema=song_schema)
    song_df.createOrReplaceTempView("my_song_table")

    # extract columns from joined song and log datasets to create songplays table
    df.createOrReplaceTempView("log_table_datetime")
    songplays_table = spark.sql("""
                                SELECT
                                datetime as start_time,
                                t3.userId as user_id,
                                t3.level as level,
                                st.song_id as song_id,
                                st.artist_id as artist_id,
                                t3.sessionId as session_id,
                                st.artist_location as location,
                                t3.userAgent as user_agent
                                FROM log_table_datetime t3
                                INNER JOIN my_song_table st ON st.title = t3.song AND st.artist_name = t3.artist """)

    
    #adding the year column
    songplays_table=songplays_table.withColumn("year", year(songplays_table.start_time))
    songplays_table=songplays_table.withColumn("month", month(songplays_table.start_time))
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year","month").parquet(output_data+"/songplays_table.parquet")


def main():
    spark = create_spark_session()
    input_data = config.get("aws",'s3_input_data')
    output_data = config.get("aws",'s3_output_data')

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()