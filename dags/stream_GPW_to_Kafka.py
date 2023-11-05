import yfinance as yf
import json

from uuid import uuid4
from pytz import timezone
import logging
from datetime import datetime, timedelta

import pandas as pd

from kafka import KafkaProducer
from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator

timezone = timezone('Europe/Warsaw')

default_args = {
    'owner': 'danielwiszowaty',
    'depends_on_past': False,
    #'start_date': days_ago(0),
    #'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

companies_GPW = [
    'PKN.WA',
    'PKO.WA',
    'SPL.WA',
    'PZU.WA',
    'DNP.WA',
    'PEO.WA',
    'ALE.WA',
    'ING.WA',
    'LPP.WA',
    'KGH.WA',
    'MBK.WA',
    'PGE.WA',
    'KRK.WA',
]

def transpose_df(df):
    if df is not None:
        try:
            df = df.stack(level=1).rename_axis(['Date', 'Symbol']).drop(['Dividends', 'Stock Splits'], axis=1)

        except Exception as e:
            logging.error(f'An error occured: {e}')

        finally: 
            return df

def filter_df_based_on_time(df, time):
    if df is not None:
            try:
                df = df.loc[df.index.get_level_values(0) == time].reset_index()
                df['Date'] = pd.Series(df['Date'].dt.tz_localize(None), dtype=object)

            except Exception as e:
                logging.error(f'An error occured: {e}')

            finally: 
                return df

def get_time_with_timezone(delta = 16):
    return (datetime.now(timezone) - timedelta(minutes=delta)).replace(second=0, microsecond=0)


def get_stock_data_every_minute(companies, period = '1d', interval = '1m'):
    tickers = yf.Tickers(companies)
    df = transpose_df(tickers.history(period=period, interval=interval))
    
    #Change delta to none for 9-17 data
    time = get_time_with_timezone(delta=2880)
    df = filter_df_based_on_time(df, time)

    if not df.empty:
        return df
    else:
        return None

def send_message(row):
    message = json.dumps(json.loads(row.to_json()), default=str).encode('utf-8')
    producer.send(topic, message)

def stream_data_to_kafka():
    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    data = get_stock_data_every_minute(companies_GPW)

    if data is not None:
        for _, row in data.iterrows():
            try:
                message = json.dumps(json.loads(row.to_json()), default=str).encode('utf-8')
                producer.send('stock_data', message) 

            except Exception as e:
                logging.error(f'An error occured: {e}')
                continue        

def check_GPW_hours():
    now = datetime.now()
    #return (now.hour == 9 and now.minute >= 15) or (now.hour > 9 and now.hour < 17) or (now.hour == 17 and now.minute <= 15)
    return True
    
my_dag = DAG('get_data_from_Yahoo_Finance',
    default_args=default_args,
    start_date=datetime(2023, 11, 3),
    #schedule_interval='* 9-17 * * MON-FRI',
    schedule_interval='* * * * *',
    #schedule_interval='@daily',
    description='Get data from Yahoo Finance every minute',
    catchup=False)

check_time_task = ShortCircuitOperator(
    task_id='check_GPW_hours',
    python_callable=check_GPW_hours,
    dag=my_dag
)


stream_data_task = PythonOperator(
    task_id='stream_data_from_api',
    python_callable=stream_data_to_kafka,
    dag=my_dag
)

check_time_task >> stream_data_task