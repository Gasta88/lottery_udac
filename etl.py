import configparser
from datetime import datetime
import time
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id, lit, \
    concat
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, \
    date_format
from pyspark.sql.types import IntegerType, StringType, TimestampType, \
    StructType, StructField, DoubleType, LongType
from functools import reduce


config = configparser.ConfigParser()
config.read('aws.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['KEYS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['KEYS']['AWS_SECRET_ACCESS_KEY']
DWH_DB = config.get("DWH","DWH_DB")
DWH_HOST = config.get("DWH", "DWH_HOST")
DWH_DB_USER = config.get("DWH","DWH_DB_USER")
DWH_DB_PASSWORD = config.get("DWH","DWH_DB_PASSWORD")
DWH_PORT = config.get("DWH","DWH_PORT")

def create_spark_session():
    """Return Spark session."""
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_inputdata(spark, input_data):
    """Process data files from S3 buckets and return dataframes.

    Parameters
    ----------
    spark: SparkSession obj
        Spark application
    input_data: str
        S3 input bucket address

    Returns
    -------
    tuple
        Tuple of Spark dataframes for all the data loaded.
    """
    # data files location
    cust_logins_data = os.path.join(input_data, 'customerlogins.csv')
    cust_registration_data = os.path.join(input_data,
                                          'customerregistration_*.csv')
    lottery_data = os.path.join(input_data, 'lotterygamespurchases.csv')
    instant_games_data = os.path.join(input_data, 'instantgamespurchases.csv')
    
    # create dataframes to store in memory
    log_df = spark.read.csv(cust_logins_data, header=True, sep=';')
    reg_df = spark.read.csv(cust_registration_data, header=True, sep=';')
    lottery_df = spark.read.csv(lottery_data, header=True, sep=';')
    games_df = spark.read.csv(instant_games_data, header=True, sep=';')
    
    # udf definitions
    get_timestampunix = udf(lambda x:
                            time.mktime(
                                datetime.strptime(x, '%Y-%m-%d %H:%M:%S:%f')\
                                    .timetuple()), IntegerType())
    get_timestamp = udf(lambda x: int(int(x)/1000), IntegerType())
    get_datetime = udf(lambda x: datetime.fromtimestamp(x), TimestampType())
    
    # preprocess log_df to change column name to timestamp and add
    # unix timestamp
    log_df = log_df.withColumnRenamed("timestamp", "datetime")
    log_df = log_df.withColum("timestamp",
                              get_timestampunix(log_df.datetime))
    
    # preprocess reg_df to change column name to timestamp and add
    # unix timestamp
    reg_df = reg_df.withColumnRenamed("timestamp", "datetime")
    reg_df = reg_df.withColum("timestamp",
                              get_timestampunix(reg_df.datetime))
    
    # preprocess lottery dataframe for timestamp, payment units and
    # missing columns
    lottery_df = lottery_df.withColumn("timestamp",
                                       get_timestamp(lottery_df.timestampunix))
    lottery_df = lottery_df.withColumn("datetime",
                                       get_datetime(lottery_df.timestamp))
    lottery_df = lottery_df.withColumn("priceineur",
                                       col("amountincents") / 100)
    lottery_df = lottery_df.withColumn("feeineur",
                                       col("feeamountincents") / 100)
    lottery_df = lottery_df.withColumn("winningsineur",
                                       lit(None).cast(StringType()))
    
    # preprocess instant games dataframe for missing columns
    games_df = games_df.withColumn("betindex", lit(None).cast(StringType()))
    games_df = games_df.withColumn("discount", lit(None).cast(StringType()))
    games_df = games_df.withColum("timestamp",
                                  get_timestampunix(games_df.datetime))
    
    
    return (log_df, reg_df, lottery_df, games_df)

def create_customer_table(reg_df, debug=False):
    """Load records to customer dimension table on Redshift.

    Parameters
    ----------
    reg_df: Spark.dataframe
        Dataframe with customer registration data.
    debug: bool
        Flag whether the execution is for production or testing.
    """
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
    #TODO: store on Redshift
    
    if debug:
        return customer_table
    return

def create_website_table(log_df, reg_df, lottery_df, games_df, debug=False):
    """Load records to website dimension table on Redshift.

    Parameters
    ----------
    log_df: Spark.dataframe
        Dataframe with customer logins data.
    reg_df: Spark.dataframe
        Dataframe with customer registration data.
    lottery_df: Spark.dataframe
        Dataframe with lottery tickets data.
    games_df: Spark.dataframe
        Dataframe with instant games data.
    debug: bool
        Flag whether the execution is for production or testing.
    """
    website_log_df = log_df.dropDuplicates(["site"])\
                           .select(col("site").alias("name"))\
                           .where(col("site").isNotNull())
    website_reg_df = reg_df.dropDuplicates(["site"])\
                           .select(col("site").alias("name"))\
                           .where(col("site").isNotNull())
    website_lottery_df = lottery_df.dropDuplicates(["site"])\
                                   .select(col("site").alias("name"))\
                                   .where(col("site").isNotNull())
    website_games_df = games_df.dropDuplicates(["siteid"])\
                               .select(col("siteid").alias("name"))\
                               .where(col("siteid").isNotNull())
    website_table = reduce(lambda x, y: x.union(y), [website_log_df,
                                                     website_reg_df,
                                                     website_lottery_df,
                                                     website_games_df])
    website_table = website_table.withColumn("id",
                                             monotonically_increasing_id())
    #TODO: store on Redshift
    if debug:
        return website_table
    return

def create_ticket_table(lottery_df, games_df, debug=False):
    """Load records to ticket dimension table on Redshift.

    Parameters
    ----------
    lottery_df: Spark.dataframe
        Dataframe with lottery tickets data.
    games_df: Spark.dataframe
        Dataframe with instant games data.
    debug: bool
        Flag whether the execution is for production or testing.
    """
    ticket_table1 = lottery_df.dropDuplicates(["ticketid"])\
                             .select(col("ticketid").alias("id"),
                                     col("amountineur").alias("amount"),
                                     col("feeineur").alias("fee"),
                                     col("winningsineur").alias("winnings"),
                                     col("discount"))\
                            .where(col("timestampunix").isNotNull() &
                                   col("site").isNotNull() &
                                   col("customernumber").isNotNull() &
                                   col("amountincents").isNotNull() &
                                   col("feeamountincents").isNotNull() &
                                   col("game").isNotNull() &
                                   col("orderidentifier").isNotNull() &
                                   col("orderidentifier").isNotNull() &
                                   col("ticketid").isNotNull())
    ticket_table2 = games_df.dropDuplicates(["ticketexternalid"])\
                            .select(col("ticketexternalid").alias("id"),
                                    col("amountineur").alias("amount"),
                                    col("feeineur").alias("fee"),
                                    col("winningsineur").alias("winnings"),
                                    col("discount"))\
                            .where(col("timestamp").isNotNull() &
                                   col("siteid").isNotNull() &
                                   col("customernumber").isNotNull() &
                                   col("gamename").isNotNull() &
                                   col("priceineur").isNotNull() &
                                   col("feeineur").isNotNull())
    ticket_table = reduce(lambda x, y: x.union(y), [ticket_table1,
                                                    ticket_table2])
    ticket_table = ticket_table.dropDuplicates(["id"])
    #TODO: store on Redshift
    if debug:
        return ticket_table
    return                         
                             
def create_product_table(lottery_df, games_df, debug=False):                      
    """Load records to product dimension table on Redshift.

    Parameters
    ----------
    lottery_df: Spark.dataframe
        Dataframe with lottery tickets data.
    games_df: Spark.dataframe
        Dataframe with instant games data.
    debug: bool
        Flag whether the execution is for production or testing.
    """                            
    prod_table1 = lottery_df.dropDuplicates(["game"])\
                            .select(col("game").alias("name"))\
                            .where(col("game").isNotNull())\
                            .withColumn("type", "lottery_game")
    prod_table2 = games_df.dropDuplicates(["gamename"])\
                          .select(col("gamename").alias("name"))\
                          .where(col("gamename").isNotNull())\
                          .withColumn("type", "instant_game")
    product_table =  reduce(lambda x, y: x.union(y), [prod_table1,
                                                      prod_table2])
    product_table = product_table.withColumn("id",
                                             monotonically_increasing_id())
    #TODO: store on Redshift
    if debug:
        return product_table
    return  

def create_time_table(log_df, reg_df, lottery_df, games_df, debug=False):
    """Load records to time dimension table on Redshift.

    Parameters
    ----------
    log_df: Spark.dataframe
        Dataframe with customer logins data.
    reg_df: Spark.dataframe
        Dataframe with customer registration data.
    lottery_df: Spark.dataframe
        Dataframe with lottery tickets data.
    games_df: Spark.dataframe
        Dataframe with instant games data.
    debug: bool
        Flag whether the execution is for production or testing.
    """
    time_log_df = log_df.dropDuplicates(["timestamp"])\
                           .select(col("timestamp"),
                                   col("datetime"))\
                           .where(col("timestamp").isNotNull())
    time_reg_df = reg_df.dropDuplicates(["timestamp"])\
                           .select(col("timestamp"),
                                   col("datetime"))\
                           .where(col("timestamp").isNotNull())
    time_lottery_df = lottery_df.dropDuplicates(["timestamp"])\
                                .select(col("timestamp"),
                                        col("datetime"))\
                                .where(col("timestamp").isNotNull())
    time_games_df = games_df.dropDuplicates(["timestamp"])\
                               .select(col("timestamp"),
                                       col("datetime"))\
                               .where(col("timestamp").isNotNull())
    time_table = reduce(lambda x, y: x.union(y), [time_log_df,
                                                  time_reg_df,
                                                  time_lottery_df,
                                                  time_games_df])
    time_table = time_table.dropDuplicates(["timestamp"])\
                           .withColumn("hour", hour("datetime"))\
                           .withColumn("day", dayofmonth("datetime"))\
                           .withColumn("week", weekofyear("datetime"))\
                           .withColumn("month", month("datetime"))\
                           .withColumn("year", year("datetime"))\
                           .withColumn("weekday", date_format('datetime', 'E'))\
                           .select(col("timestamp"),
                                   col("hour"), col("day"), col("week"),
                                   col("month"), col("year"), col("weekday"))
    #TODO: store on Redshift
    if debug:
        return time_table
    return  

def create_registrations_tables(spark, reg_df, debug=False):
    """Load records to registrations facts tables on Redshift.

    Parameters
    ----------
    spark: SparkSession obj
        Spark application
    reg_df: Spark.dataframe
        Dataframe with customer registration data.
    debug: bool
        Flag whether the execution is for production or testing.
    """
    
    website_df = spark.read \
                      .format("com.databricks.spark.redshift") \
                      .option("url", f"{DWH_HOST}:{DWH_PORT}/{DWH_DB}" + "?user="+ DWH_DB_USER +"&password="+ DWH_DB_PASSWORD) \
                      .option("query", "SELECT * FROM WEBSITES") \
                      .load()
    cond = [reg_df.site == website_df.name]
    registrations_table = reg_df.dropDuplicates(["customernumber", "site", "timestamp"])\
                                .join(website_df, cond, 'leftouter')\
                                .select(monotonically_increasing_id().alias("id"),
                                        col("customernumber"),
                                        website_df.id.alias("website_id"),
                                        col("timestamp"))\
                                .where(col("customernumber").isNotNull() &
                                       website_df.id.isNotNull() &
                                       col("timestamp").isNotNull())
    #TODO: store on Redshift
    if debug:
        return registrations_table
    return  

def create_logins_table(spark, log_df, debug=False):
    """Load records to logins facts tables on Redshift.

    Parameters
    ----------
    spark: SparkSession obj
        Spark application
    log_df: Spark.dataframe
        Dataframe with customer login data.
    debug: bool
        Flag whether the execution is for production or testing.
    """
    
    website_df = spark.read \
                      .format("com.databricks.spark.redshift") \
                      .option("url", f"{DWH_HOST}:{DWH_PORT}/{DWH_DB}" + "?user="+ DWH_DB_USER +"&password="+ DWH_DB_PASSWORD) \
                      .option("query", "SELECT * FROM WEBSITES") \
                      .load()
    cond = [log_df.site == website_df.name]
    logins_table = log_df.dropDuplicates(["customernumber", "website", "timestamp"])\
                         .join(website_df, cond, 'leftouter')\
                         .select(monotonically_increasing_id().alias("id"),
                                 col("customernumber"),
                                 website_df.id.alias("website_id"),
                                 col("timestamp"))\
                         .where(col("customernumber").isNotNull() &
                                website_df.id.isNotNull() &
                                col("timestamp").isNotNull())
    #TODO: store on Redshift
    if debug:
        return logins_table
    return

def create_bookings_table(spark, lottery_df, games_df, debug=False):
    """Load records to bookings facts tables on Redshift.

    Parameters
    ----------
    spark: SparkSession obj
        Spark application
    lottery_df: Spark.dataframe
        Dataframe with lottery tickets data.
    games_df: Spark.dataframe
        Dataframe with instant games data.
    debug: bool
        Flag whether the execution is for production or testing.
    """
    website_df = spark.read \
                      .format("com.databricks.spark.redshift") \
                      .option("url", f"{DWH_HOST}:{DWH_PORT}/{DWH_DB}" + "?user="+ DWH_DB_USER +"&password="+ DWH_DB_PASSWORD) \
                      .option("query", "SELECT * FROM WEBSITES") \
                      .load()
    product_df = spark.read \
                      .format("com.databricks.spark.redshift") \
                      .option("url", f"{DWH_HOST}:{DWH_PORT}/{DWH_DB}" + "?user="+ DWH_DB_USER +"&password="+ DWH_DB_PASSWORD) \
                      .option("query", "SELECT * FROM PRODUCTS") \
                      .load()

    lottery_df = lottery_df.dropDuplicates(["ticketid"])\
                           .select(col("ticketid").alias("id"),
                                   col("customernumber"),
                                   col("site").alias("website"),
                                   col("game"),
                                   col("timestamp"),
                                   col("currency"),
                                   col("princeineur"))\
                           .where(col("ticketid").isNotNull() &
                                  col("customernumber").isNotNull() &
                                  col("site").isNotNull() &
                                  col("game").isNotNull() &
                                  col("timestamp").isNotNull())
    games_df = games_df.dropDuplicates(["ticketexternalid", "aggregationkey"])\
                       .select(concat(col("ticketexternalid"), col("aggregationkey")).alias("id"),
                                      col("customernumber"),
                                      col("siteid").alias("website"),
                                      col("gamename").alias("game"),
                                      col("timestamp"),
                                      col("currency"),
                                      col("princeineur"))\
                       .where(concat(col("ticketexternalid"), col("aggregationkey")).isNotNull() &
                              col("customernumber").isNotNull() &
                              col("siteid").isNotNull() &
                              col("gamename").isNotNull() &
                              col("timestamp").isNotNull())
    bookings_table =  reduce(lambda x, y: x.union(y), [lottery_df, games_df])
    cond = [bookings_table.website == website_df.name]
    # HERE!!
    # bookings_table = log_df.dropDuplicates(["customernumber", "website", "timestamp"])\
    #                      .join(website_df, cond, 'leftouter')\
    #                      .select(monotonically_increasing_id().alias("id"),
    #                              col("customernumber"),
    #                              website_df.id.alias("website_id"),
    #                              col("timestamp"))\
    #                      .where(col("customernumber").isNotNull() &
    #                             website_df.id.isNotNull() &
    #                             col("timestamp").isNotNull())
     
     
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