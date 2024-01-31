import logging
from cassandra.cluster import Cluster

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, from_unixtime
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, DateType, IntegerType 
from pyspark.sql.functions import year, month, dayofmonth, quarter, weekofyear, dayofweek, hour, minute, second

import os

sfOptions = {
    "sfURL": os.environ.get('SF_URL'),
    "sfAccount": os.environ.get('SF_ACCOUNT'),
    "sfUser": os.environ.get('SF_USER'),
    "pem_private_key": os.environ.get('PKB'),
    "sfPassword": os.environ.get('SF_PASSWORD'),
    "sfDatabase": os.environ.get('SF_DATABASE'),
    "sfSchema": os.environ.get('SF_SCHEMA'),
}

SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"

def convert_columns_to_lowercase(input_df):
    return input_df.select([col(x).alias(x.lower()) for x in input_df.columns])

def create_spark_connection():
    s_conn = None

    try:
        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages',  "net.snowflake:spark-snowflake_2.12:2.13.0-spark_3.4,"
                                            "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,"
                                            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,"
                                            "net.snowflake:snowflake-jdbc:3.14.2") \
            .master("local[*]") \
            .config("spark.driver.memory", "4G") \
            .config("spark.driver.maxResultSize", "2G") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.kryoserializer.buffer.max", "500m") \
            .config("spark.jars.repositories", "http://repo.spring.io/plugins-release") \
            .config('spark.cassandra.connection.host', 'cassandra') \
            .getOrCreate()

        s_conn.sparkContext.setLogLevel("FATAL")

        logging.info("Spark connection created successfully!")  
    except Exception as e:
        logging.error(f"ERROR: Couldn't create the spark session due to exception {e}")

    return s_conn


def connect_to_kafka(spark_conn):
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'broker:29092') \
            .option('subscribe', 'stock_data') \
            .option('startingOffsets', 'earliest') \
            .load()
        logging.info("Kafka dataframe created successfully")
    except Exception as e:
        logging.warning(f"ERROR: Kafka dataframe could not be created because: {e}")

    return spark_df

def create_selection_df_from_kafka(spark_df):
    sel = None

    try:
        schema = StructType([
            StructField("Date", StringType(), False),
            StructField("Symbol", StringType(), False),
            StructField("Close", DoubleType(), False),
            StructField("High", DoubleType(), False),
            StructField("Low", DoubleType(), False),
            StructField("Open", DoubleType(), False),
            StructField("Volume", DoubleType(), False)
        ])

        sel = spark_df.selectExpr("CAST(value AS STRING)") \
            .select(from_json(col('value'), schema) \
            .alias('data')) \
            .select("data.*")

        sel = sel.withColumn("Date", from_unixtime(col("Date")/1000, "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").cast(TimestampType()))


    except Exception as e:
        logging.warning(f"ERROR: Could not create selection DataFrame from Kafka because: {e}")
    finally:
        return sel

def create_date_df_from_kafka(spark_df):
    sel = None

    try:
        sel = spark_df.withColumn("Datetime", spark_df["Date"]) \
            .withColumn("Date", spark_df["Date"].cast(DateType())) \
            .select(
                "Datetime",
                "Date",
                year("Date").alias("Year"),
                month("Date").alias("Month"),
                dayofmonth("Date").alias("Day"),
                quarter("Date").alias("Quarter"),
                weekofyear("Date").alias("Week"),
                dayofweek("Date").alias("Weekday"),
                hour("Datetime").alias("Hour"),
                minute("Datetime").alias("Minute"),
                second("Datetime").alias("Second")
            ).distinct()
        

    except Exception as e:
        logging.warning(f"ERROR: Could not create selection DataFrame from Kafka because: {e}")
    finally:
        return sel

def write_to_snowflake(df, epoch_id):
    df.write \
      .format(SNOWFLAKE_SOURCE_NAME) \
      .options(**sfOptions) \
      .option("dbtable", "Stock_data_FACT") \
      .mode("append") \
      .save()

def write_date_to_snowflake(df, epoch_id):
    df.write \
      .format(SNOWFLAKE_SOURCE_NAME) \
      .options(**sfOptions) \
      .option("dbtable", "Date_DIM") \
      .mode("append") \
      .save()

def create_cassandra_connection():
    try:
        # connecting to the cassandra cluster
        cluster = Cluster(['cassandra'])

        cas_session = cluster.connect()

        return cas_session
    except Exception as e:
        logging.error(f"Could not create cassandra connection due to {e}")
        return None

def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS DW_STOCK
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)

    print("Keyspace created successfully!")

def create_table(session):
    session.execute("""
    CREATE TABLE IF NOT EXISTS DW_STOCK.Stock_data_FACT (
        Date TIMESTAMP,
        Symbol TEXT,
        Close DOUBLE,
        High DOUBLE,
        Low DOUBLE,
        Open DOUBLE,
        Volume DOUBLE,
        PRIMARY KEY(Date, Symbol)
        );
    """)

    session.execute("""
    CREATE TABLE IF NOT EXISTS DW_STOCK.Date_DIM (
        Datetime TIMESTAMP PRIMARY KEY,
        Date DATE,
        Year INT,
        Month INT,
        Day INT,
        Quarter INT,
        Week INT,
        Weekday INT,
        Hour INT,
        Minute INT,
        Second INT);
    """)

    print("Table created successfully!")

if __name__ == "__main__":

    # create spark connection
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        #connect to kafka with spark connection

        spark_df = connect_to_kafka(spark_conn=spark_conn)
        selection_df = create_selection_df_from_kafka(spark_df)
        date_df = create_date_df_from_kafka(selection_df)

        selection_df = convert_columns_to_lowercase(selection_df)
        date_df = convert_columns_to_lowercase(date_df)

        session = create_cassandra_connection()
        if session is not None:
            create_keyspace(session)
            create_table(session)

            logging.info("Streaming data to Cassandra...")
           
            cass_query = selection_df.writeStream \
                .format("org.apache.spark.sql.cassandra") \
                .option('checkpointLocation', 'jobs/cass_checkpoint') \
                .option('keyspace', 'dw_stock') \
                .option('table', 'stock_data_fact') \
                .start()

            cass_date_query = date_df.writeStream \
                .format("org.apache.spark.sql.cassandra") \
                .option('checkpointLocation', 'jobs/cass_checkpoint_date') \
                .option('keyspace', 'dw_stock') \
                .option('table', 'date_dim') \
                .start()
            

        query = selection_df.writeStream \
            .outputMode("append") \
            .option("checkpointLocation", "jobs/checkpoint") \
            .foreachBatch(write_to_snowflake) \
            .start()

        date_query = date_df.writeStream \
            .outputMode("append") \
            .option("checkpointLocation", "jobs/checkpoint_date") \
            .foreachBatch(write_date_to_snowflake) \
            .start()

        query.awaitTermination()
        date_query.awaitTermination()

        cass_query.awaitTermination()
        cass_date_query.awaitTermination()
        
        '''
        # Printing to console
        console_query = selection_df.writeStream \
            .outputMode("append") \
            .format("console") \
            .start()

        console_date_query = date_df.writeStream \
            .outputMode("append") \
            .format("console") \
            .start()

        console_query.awaitTermination()
        console_date_query.awaitTermination()
        '''