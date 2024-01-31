from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'danielwiszowaty',
    'depends_on_past': False,
    #'start_date': days_ago(0),
    'email': ['daniwis272@student.polsl.pl'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

my_dag = DAG('Stream_from_Kafka_to_Spark',
    default_args=default_args,
    start_date=datetime(2023, 11, 3),
    schedule_interval='@once',
    description='Stream spark to database',
    catchup=False)

spark_to_db_task = SparkSubmitOperator(
    task_id='stream_spark',
    conn_id='spark_conn',
    application='jobs/spark_stream_from_kafka.py',
    packages='net.snowflake:spark-snowflake_2.12:2.13.0-spark_3.4,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,net.snowflake:snowflake-jdbc:3.14.2,com.datastax.spark:spark-cassandra-connector_2.12:3.4.1',
    dag=my_dag
)

spark_to_db_task