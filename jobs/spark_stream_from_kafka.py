import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, from_unixtime
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

import os

from dotenv import load_dotenv
load_dotenv('.env')

sfOptions = {
    "sfURL": os.environ.get(SF_URL),
    "sfAccount": os.environ.get(SF_ACCOUNT),
    "sfUser": os.environ.get(SF_USER),
    "pem_private_key" : os.environ.get(PKB),
    "sfPassword": os.environ.get(SF_PASSWORD),
    "sfDatabase": os.environ.get(SF_DATABASE),
    "sfSchema": os.environ.get(SF_SCHEMA),
}


SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"

def create_spark_connection():
    s_conn = None

    try:
        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages',  "net.snowflake:spark-snowflake_2.12:2.13.0-spark_3.4,"
                                            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,"
                                            "net.snowflake:snowflake-jdbc:3.14.2") \
            .master("local[*]") \
            .config("spark.driver.memory", "4G") \
            .config("spark.driver.maxResultSize", "2G") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.kryoserializer.buffer.max", "500m") \
            .config("spark.jars.repositories", "http://repo.spring.io/plugins-release") \
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

def write_to_snowflake(df, epoch_id):
    df.write \
      .format(SNOWFLAKE_SOURCE_NAME) \
      .options(**sfOptions) \
      .option("dbtable", "FACT_STOCK_DATA") \
      .mode("append") \
      .save()

if __name__ == "__main__":

    # create spark connection
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        #connect to kafka with spark connection

        spark_df = connect_to_kafka(spark_conn=spark_conn)
        selection_df = create_selection_df_from_kafka(spark_df)

        query = selection_df.writeStream \
            .outputMode("append") \
            .option("checkpointLocation", "jobs/checkpoint") \
            .foreachBatch(write_to_snowflake) \
            .start()

        query.awaitTermination()