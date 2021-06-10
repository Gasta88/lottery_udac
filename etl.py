import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, \
    date_format
from pyspark.sql.types import IntegerType, StringType, TimestampType, \
    StructType, StructField, DoubleType, LongType


config = configparser.ConfigParser()
config.read('aws.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['KEYS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['KEYS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """Return Spark session."""
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_customer_data(spark, input_data, debug=False):
    """Process customer data files from S3 buckets.

    The method store the records onto the Redshift database.
    
    Parameters
    ----------
    spark: SparkSession obj
        Spark application
    input_data: str
        S3 input bucket address
    debug: bool
        Flag whether the execution is for production or testing.
    """
    # cust_logins_data = os.path.join(input_data, 'customerlogins.csv')
    cust_registration_data = os.path.join(input_data,
                                          'customerregistration_*.csv')
    # log_df = spark.read.csv(cust_logins_data, header=True, sep=';')
    reg_df = spark.read.csv(cust_registration_data, header=True, sep=';')
    
    customer_table = reg_df.dropDuplicates(["customernumber"])\
                           .select(col("customernumber").alias("id"),
                                   col("customeremail").alias("email"),
                                   col("dateofbirth"),
                                   col("familyname"),
                                   col("givennames"),
                                   col("primaryaddress_addressline").alias("address"),
                                   col("primaryaddress_city").alias("city"),
                                   col("primaryaddress_federalstate").alias("federalstate"),
                                   col("primaryaddress_postalcode").alias("postalcode"),
                                   col("primaryaddress_sovereignstate").alias("sovereignstate"),
                                   col("primaryaddress_street").alias("street"))\
                           .where(col("customernumber").isNotNull() &
                                  col("customeremail").isNotNull &
                                  col("givennames").isNotNull() &
                                  col("familyname").isNotNull() &
                                  col("timestamp").isNotNull() &
                                  col("dateofbirth").isNotNull() &
                                  col("customeremail").rlike('^[a-z0-9]+[\._]?[a-z0-9]+[@]\w+[.]\w{2,3}$'))
    
    # websites_table = log_df.dropDuplicates(["site"])\
    #                        .select(monotonically_increasing_id().alias("id"),
    #                                col("site").alias("name"))\
    #                        .where(col("site").isNotNull())
    if debug:
        return {'customer_table': (customer_table.count(),
                                   len(customer_table.columns))}
    return

def process_time_data(spark, input_data, debug=False):
    """Process time data files from S3 buckets.

    The method store the records onto the Redshift database.
    
    Parameters
    ----------
    spark: SparkSession obj
        Spark application
    input_data: str
        S3 input bucket address
    debug: bool
        Flag whether the execution is for production or testing.
    """
    cust_logins_data = os.path.join(input_data, 'customerlogins.csv')
    cust_registration_data = os.path.join(input_data,
                                          'customerregistration_*.csv')
    lottery_data = os.path.join(input_data, 'lotterygamespurchases.csv')
    instant_games_data = os.path.join(input_data, 'instantgamespurchases.csv')
    
    log_df = spark.read.csv(cust_logins_data, header=True, sep=';')
    reg_df = spark.read.csv(cust_registration_data, header=True, sep=';')
    lottery_df = spark.read.csv(lottery_data, header=True, sep=';')
    games_df = spark.read.csv(instant_games_data, header=True, sep=';')
    
    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: int(int(x)/1000), IntegerType())
    lottery_df = lottery_df.withColumn("timestamp",
                                       get_timestamp(lottery_df.timestampunix))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x), TimestampType())
    lottery_df = lottery_df.withColumn("datetime",
                                       get_datetime(lottery_df.timestamp))
    
    # extract columns to create time table
    time_table = df.withColumn("hour", hour("datetime"))\
                    .withColumn("day", dayofmonth("datetime"))\
                    .withColumn("week", weekofyear("datetime"))\
                    .withColumn("month", month("datetime"))\
                    .withColumn("year", year("datetime"))\
                    .withColumn("weekday", date_format('datetime', 'E'))\
                    .select(col("timestamp").alias("start_time"),
                            col("hour"), col("day"), col("week"), col("month"),
                            col("year"), col("weekday"))
    
                
                    
def process_song_data(spark, input_data, output_data):
    """Process song data files from S3 buckets.

    The method generates the songs and artists parquet files.
    
    Parameters
    ----------
    spark: SparkSession obj
        Spark application
    input_data: str
        S3 input bucket address
    output_data: str
        S3 output bucket address
    """
    # get filepath to song data file
    song_data = os.path.join(input_data, "song_data/*/*/*/*.json")
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.dropDuplicates(["song_id"])\
                    .select(col("song_id"),
                            col("title"),
                            col("artist_id"),
                            col("year"),
                            col("duration"))\
                    .where(col("song_id").isNotNull())
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(os.path.join(output_data, "songs.parquet"),
                              mode="overwrite", partitionBy=["year",
                                                             "artist_id"])

    # extract columns to create artists table
    artists_table = df.dropDuplicates(["artist_id"])\
                      .select(col("artist_id"),
                              col("artist_name").alias("name"),
                              col("artist_location").alias("location"),
                              col("artist_latitude").alias("latitude"),
                              col("artist_longitude").alias("longitude"))\
                      .where(col("artist_id").isNotNull())
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data,
                                             "artists.parquet"),
                                mode="overwrite")


def process_log_data(spark, input_data, output_data):
    """Process log data files from S3 buckets.

    The method generates the users, time and songplays parquet files.
    
    Parameters
    ----------
    spark: SparkSession obj
        Spark application
    input_data: str
        S3 input bucket address
    output_data: str
        S3 output bucket address
    """
    # get filepath to log data file
    log_data = os.path.join(input_data, "log-data/*/*/*.json")

    log_schema = StructType([
                        StructField("artist", StringType(), True),
                        StructField("auth", StringType(), True),
                        StructField("firstName", StringType(), True),
                        StructField("gender", StringType(), True),
                        StructField("itemInSession", IntegerType(), True),
                        StructField("lastName", StringType(), True),
                        StructField("length", DoubleType(), True),
                        StructField("level", StringType(), True),
                        StructField("location", StringType(), True),
                        StructField("method", StringType(), True),
                        StructField("page", StringType(), True),
                        StructField("registration", DoubleType(), True),
                        StructField("sessionId", IntegerType(), True),
                        StructField("song", StringType(), True),
                        StructField("status", IntegerType(), True),
                        StructField("ts", LongType(), True),
                        StructField("userAgent", StringType(), True),
                        StructField("userId", StringType(), True)
                ])
    # read log data file
    df = spark.read.json(log_data, schema=log_schema)
    
    # filter by actions for song plays
    df = df.filter(col("page") == "NextSong")
    # extract columns for users table    
    users_table = df.dropDuplicates(["userId"])\
                .select(col("userId").alias("user_id"),
                        col("firstName").alias("first_name"),
                        col("lastName").alias("last_name"),
                        col("gender"),
                        col("level"))\
                .where(col("user_id").isNotNull())
    
    # # # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data,
                                           "users.parquet"),
                              mode="overwrite")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: int(int(x)/1000), IntegerType())
    df = df.withColumn("timestamp", get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x), TimestampType())
    df = df.withColumn("datetime", get_datetime(df.timestamp))
    
    # extract columns to create time table
    time_table = df.withColumn("hour", hour("datetime"))\
                    .withColumn("day", dayofmonth("datetime"))\
                    .withColumn("week", weekofyear("datetime"))\
                    .withColumn("month", month("datetime"))\
                    .withColumn("year", year("datetime"))\
                    .withColumn("weekday", date_format('datetime', 'E'))\
                    .select(col("timestamp").alias("start_time"),
                            col("hour"), col("day"), col("week"), col("month"),
                            col("year"), col("weekday"))
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(os.path.join(output_data, "time.parquet"),
                              mode="overwrite", partitionBy=["year",
                                                            "month"])

    # read in song data to use for songplays table
    song_df = spark.read.json(os.path.join(input_data, "song_data/*/*/*/*.json"))

    # extract columns from joined song and log datasets to create songplays table 
    cond = [song_df.artist_name == df.artist, song_df.title == df.song]
    songplays_table = song_df.join(df, cond, 'rightouter')\
                             .select(monotonically_increasing_id().alias("songplay_id"),
                                     col("datetime").alias("start_time"),
                                     col("userId").alias("user_id"),
                                     col("level"),
                                     col("song_id"),
                                     col("artist_id"),
                                     col("sessionId").alias("session_id"),
                                     col("location"),
                                     col("userAgent").alias("user_agent"),
                                     month(col("datetime")).alias("month"),
                                     year(col("datetime")).alias("year"))\
                             .where(col("song_id").isNotNull() &
                                    col("artist_id").isNotNull())
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(os.path.join(output_data, "songplays.parquet"),
                                  mode="overwrite", partitionBy=["year",
                                                                 "month"])


def main():
    """Run main steps in the ETL pipeline."""
    spark = create_spark_session()
    input_data = "s3a://udac-lottery-data/"
    
    process_song_data(spark, input_data)    
    process_log_data(spark, input_data)


if __name__ == "__main__":
    main()