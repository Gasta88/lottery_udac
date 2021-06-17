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

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['KEY']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['SECRET']
DWH_DB = config.get("DWH","DB_NAME")
DWH_HOST = config.get("DWH", "HOST")
DWH_DB_USER = config.get("DWH","DB_USER")
DWH_DB_PASSWORD = config.get("DWH","DB_PASSWORD")
DWH_PORT = config.get("DWH","DB_PORT")

def create_spark_session():
    """Return Spark session."""
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    spark._jsc.hadoopConfiguration().set("fs.s3.awsAccessKeyId",
                                         config['AWS']['KEY'])
    spark._jsc.hadoopConfiguration().set("fs.s3.awsSecretAccessKey",
                                         config['AWS']['SECRET'])
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
                            int(time.mktime(
                                datetime.strptime(x[:-3], '%Y-%m-%d %H:%M:%S.%f')\
                                    .timetuple())), IntegerType())
    get_timestamp = udf(lambda x: int(int(x)/1000), IntegerType())
    get_datetime = udf(lambda x: datetime.fromtimestamp(x), TimestampType())
    
    # preprocess log_df to change column name to timestamp and add
    # unix timestamp
    log_df = log_df.withColumnRenamed("timestamp", "datetime")
    log_df = log_df.withColumn("timestamp",
                               get_timestampunix(log_df.datetime))
    
    # preprocess reg_df to change column name to timestamp and add
    # unix timestamp
    reg_df = reg_df.withColumnRenamed("timestamp", "datetime")
    reg_df = reg_df.withColumn("timestamp",
                              get_timestampunix(reg_df.datetime))
    
    # preprocess lottery dataframe for timestamp, payment units and
    # missing columns
    lottery_df = lottery_df.withColumn("timestamp",
                                       get_timestamp(lottery_df.timestampunix))
    lottery_df = lottery_df.withColumn("datetime",
                                       get_datetime(lottery_df.timestamp))
    lottery_df = lottery_df.withColumn("amountineur",
                                       col("amountincents") / 100)\
                           .withColumn("feeineur",
                                       col("feeamountincents") / 100)\
                           .withColumn("winningsineur",
                                       lit(None).cast(StringType()))
    
    # preprocess instant games dataframe for missing columns
    games_df = games_df.withColumn("betindex", lit(None).cast(StringType()))\
                       .withColumn("discount", lit(None).cast(StringType()))
    games_df = games_df.withColumnRenamed("timestamp", "datetime")
    games_df = games_df.withColumn("timestamp",
                                  get_timestampunix(games_df.datetime))
    
    
    return (log_df, reg_df, lottery_df, games_df)

def create_customer_table(spark, reg_df, debug=False):
    """Load records to customer dimension table on Redshift.

    Parameters
    ----------
    spark: SparkSession obj
        Spark application
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
                                  col("customeremail").isNotNull() &
                                  col("givennames").isNotNull() &
                                  col("familyname").isNotNull() &
                                  col("timestamp").isNotNull() &
                                  col("dateofbirth").isNotNull() &
                                  col("customeremail").rlike('^[a-z0-9]+[\._]?[a-z0-9]+[@]\w+[.]\w{2,3}$'))
    customer_table.write \
                  .format("com.databricks.spark.redshift")\
                  .option("url", f"jdbc:redshift://{DWH_HOST}:{DWH_PORT}/{DWH_DB}?user={DWH_DB_USER}&password={DWH_DB_PASSWORD}")\
                  .option("dbtable", "CUSTOMERS")\
                  .option("aws_region", "us-east-1")\
                  .option("tempdir", "s3://udac-lottery-data/")\
                  .mode("append")\
                  .save()
    
    if debug:
       return customer_table
    return

def create_website_table(spark, log_df, reg_df, lottery_df, games_df, debug=False):
    """Load records to website dimension table on Redshift.

    Parameters
    ----------
    spark: SparkSession obj
        Spark application
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
    website_games_df = games_df.dropDuplicates(["sitetid"])\
                               .select(col("sitetid").alias("name"))\
                               .where(col("sitetid").isNotNull())
    website_table = reduce(lambda x, y: x.union(y), [website_log_df,
                                                     website_reg_df,
                                                     website_lottery_df,
                                                     website_games_df])
    website_table = website_table.dropDuplicates(["name"])\
                             .sort("name", ascending=True)\
                             .withColumn("id",
                                         monotonically_increasing_id())
    website_table.write \
                  .format("com.databricks.spark.redshift")\
                  .option("url", f"jdbc:redshift://{DWH_HOST}:{DWH_PORT}/{DWH_DB}?user={DWH_DB_USER}&password={DWH_DB_PASSWORD}")\
                  .option("dbtable", "WEBSITES")\
                  .option("aws_region", "us-east-1")\
                  .option("tempdir", "s3://udac-lottery-data/")\
                  .mode("append")\
                  .save()
    if debug:
        return website_table
    return

def create_ticket_table(spark, lottery_df, games_df, debug=False):
    """Load records to ticket dimension table on Redshift.

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
    ticket_table1 = lottery_df.dropDuplicates(["ticketid"])\
                             .select(col("ticketid").alias("id"),
                                     col("amountineur").alias("amount"),
                                     col("feeineur").alias("fee"),
                                     col("winningsineur").alias("winnings"))\
                            .where(col("timestampunix").isNotNull() &
                                   col("site").isNotNull() &
                                   col("customernumber").isNotNull() &
                                   col("amountineur").isNotNull() &
                                   col("feeineur").isNotNull() &
                                   col("game").isNotNull() &
                                   col("orderidentifier").isNotNull() &
                                   col("orderidentifier").isNotNull() &
                                   col("ticketid").isNotNull())
    ticket_table2 = games_df.dropDuplicates(["ticketexternalid", "aggregationkey"])\
                            .select(concat(col("ticketexternalid"), col("aggregationkey")).alias("id"),
                                    col("priceineur").alias("amount"),
                                    col("feeineur").alias("fee"),
                                    col("winningsineur").alias("winnings"))\
                            .where(col("timestamp").isNotNull() &
                                   col("sitetid").isNotNull() &
                                   col("customernumber").isNotNull() &
                                   col("gamename").isNotNull() &
                                   col("priceineur").isNotNull() &
                                   col("feeineur").isNotNull())
    ticket_table = reduce(lambda x, y: x.union(y), [ticket_table1,
                                                    ticket_table2])
    ticket_table = ticket_table.dropDuplicates(["id"])
    
    ticket_table.write \
                  .format("com.databricks.spark.redshift")\
                  .option("url", f"jdbc:redshift://{DWH_HOST}:{DWH_PORT}/{DWH_DB}?user={DWH_DB_USER}&password={DWH_DB_PASSWORD}")\
                  .option("dbtable", "TICKETS")\
                  .option("aws_region", "us-east-1")\
                  .option("tempdir", "s3://udac-lottery-data/")\
                  .mode("append")\
                  .save()
    if debug:
        return ticket_table
    return                         
                             
def create_product_table(spark, lottery_df, games_df, debug=False):                      
    """Load records to product dimension table on Redshift.

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
    product_table.write \
                  .format("com.databricks.spark.redshift")\
                  .option("url", f"jdbc:redshift://{DWH_HOST}:{DWH_PORT}/{DWH_DB}?user={DWH_DB_USER}&password={DWH_DB_PASSWORD}")\
                  .option("dbtable", "PRODUCTS")\
                  .option("aws_region", "us-east-1")\
                  .option("tempdir", "s3://udac-lottery-data/")\
                  .mode("append")\
                  .save()
    if debug:
        return product_table
    return  

def create_time_table(spark, log_df, reg_df, lottery_df, games_df, debug=False):
    """Load records to time dimension table on Redshift.

    Parameters
    ----------
    spark: SparkSession obj
        Spark application
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
    time_table.write \
                  .format("com.databricks.spark.redshift")\
                  .option("url", f"jdbc:redshift://{DWH_HOST}:{DWH_PORT}/{DWH_DB}?user={DWH_DB_USER}&password={DWH_DB_PASSWORD}")\
                  .option("dbtable", "TIMES")\
                  .option("aws_region", "us-east-1")\
                  .option("tempdir", "s3://udac-lottery-data/")\
                  .mode("append")\
                  .save()
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
    registrations_table.write \
                  .format("com.databricks.spark.redshift")\
                  .option("url", f"jdbc:redshift://{DWH_HOST}:{DWH_PORT}/{DWH_DB}?user={DWH_DB_USER}&password={DWH_DB_PASSWORD}")\
                  .option("dbtable", "REGISTRATIONS")\
                  .option("aws_region", "us-east-1")\
                  .option("tempdir", "s3://udac-lottery-data/")\
                  .mode("append")\
                  .save()
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
    logins_table.write \
                  .format("com.databricks.spark.redshift")\
                  .option("url", f"jdbc:redshift://{DWH_HOST}:{DWH_PORT}/{DWH_DB}?user={DWH_DB_USER}&password={DWH_DB_PASSWORD}")\
                  .option("dbtable", "LOGINS")\
                  .option("aws_region", "us-east-1")\
                  .option("tempdir", "s3://udac-lottery-data/")\
                  .mode("append")\
                  .save()
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
                      .option("url", f"{DWH_HOST}:{DWH_PORT}/{DWH_DB}?user={DWH_DB_USER}&password={DWH_DB_PASSWORD}") \
                      .option("query", "SELECT * FROM WEBSITES") \
                      .load()
    product_df = spark.read \
                      .format("com.databricks.spark.redshift") \
                      .option("url", f"{DWH_HOST}:{DWH_PORT}/{DWH_DB}?user={DWH_DB_USER}&password={DWH_DB_PASSWORD}") \
                      .option("query", "SELECT * FROM PRODUCTS") \
                      .load()

    lottery_df = lottery_df.dropDuplicates(["ticketid"])\
                           .select(col("ticketid").alias("ticket_id"),
                                   col("customernumber"),
                                   col("site").alias("website"),
                                   col("game"),
                                   col("timestamp"),
                                   col("currency"),
                                   col("amountineur").alias("amount"))\
                           .where(col("ticketid").isNotNull() &
                                  col("customernumber").isNotNull() &
                                  col("site").isNotNull() &
                                  col("game").isNotNull() &
                                  col("timestamp").isNotNull())
    games_df = games_df.dropDuplicates(["ticketexternalid", "aggregationkey"])\
                       .select(concat(col("ticketexternalid"), col("aggregationkey")).alias("ticket_id"),
                                      col("customernumber"),
                                      col("sitetid").alias("website"),
                                      col("gamename").alias("game"),
                                      col("timestamp"),
                                      col("currency"),
                                      col("princeineur").alias("amount"))\
                       .where(concat(col("ticketexternalid"), col("aggregationkey")).isNotNull() &
                              col("customernumber").isNotNull() &
                              col("siteid").isNotNull() &
                              col("gamename").isNotNull() &
                              col("timestamp").isNotNull())
    
    bookings_table =  reduce(lambda x, y: x.union(y), [lottery_df, games_df])
    
    cond_website = [bookings_table.website == website_df.name]
    cond_product = [bookings_table.game == product_df.name]
    bookings_table = bookings_table.dropDuplicates(["customernumber", "website", "timestamp", "ticketid"])\
                                   .join(website_df, cond_website, 'leftouter')\
                                   .join(product_df, cond_product, 'leftouter')\
                                   .select(monotonically_increasing_id().alias("id"),
                                           col("customernumber"),
                                           website_df.id.alias("website_id"),
                                           col("timestamp"),
                                           col("ticket_id"),
                                           product_df.id.alias("product_id"),
                                           col("currency"),
                                           col("amount"))\
                                   .where(col("customernumber").isNotNull() &
                                          website_df.id.isNotNull() &
                                          col("timestamp").isNotNull() &
                                          col("ticket_id").isNotNull() &
                                          product_df.id.alias("product_id").isNotNull())
    bookings_table.write \
                  .format("com.databricks.spark.redshift")\
                  .option("url", f"jdbc:redshift://{DWH_HOST}:{DWH_PORT}/{DWH_DB}?user={DWH_DB_USER}&password={DWH_DB_PASSWORD}")\
                  .option("dbtable", "BOOKINGS")\
                  .option("aws_region", "us-east-1")\
                  .option("tempdir", "s3://udac-lottery-data/")\
                  .mode("append")\
                  .save()
    if debug:
        return bookings_table
    return

def main():
    """Run main steps in the ETL pipeline."""
    spark = create_spark_session()
    input_data = "s3a://udac-lottery-data/"
    
    log_df, reg_df, lottery_df, games_df = process_inputdata(spark, input_data)
    create_customer_table(spark, reg_df, debug=1)
    # create_website_table(spark, log_df, reg_df, lottery_df, games_df)
    # create_ticket_table(spark, lottery_df, games_df)
    # create_product_table(spark, lottery_df, games_df)
    # create_time_table(spark, log_df, reg_df, lottery_df, games_df)
    # create_registrations_tables(spark, reg_df)
    # create_logins_table(spark, log_df)
    # create_bookings_table(spark, lottery_df, games_df)

if __name__ == "__main__":
    main()