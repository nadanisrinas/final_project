from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
import os
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests
import http.client
import json
import pandas as pd
import ast
# Define the default_args dictionary to specify the default parameters of the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 29),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}


# Create a DAG instance
dag = DAG(
    'daily_nft_etl',
    default_args=default_args,
    description='Daily NFT ETL Pipeline to run every 1 day',
    schedule_interval=timedelta(days=1),
    catchup=False,
)

def print_begin():
    print("Begin ETL process")

def get_data_nft(payload):
    # Import your Python script here
    conn = http.client.HTTPSConnection("streaming.bitquery.io")
    headers = {
    'Content-Type': 'application/json',
        'Authorization': f"Bearer ory_at_M3kjLDmYgBSZ1YyokQy-csBBheyTX_CxSL2T6mMNjbY.9YKUbZxahp8uEfeeYtbQWDfYnA9h2-QMfuCVHpaSUYo",
    }
    conn.request("POST", "/graphql", payload, headers)
    res = conn.getresponse()
    data = res.read()
    resp= json.loads(data)
    return {"metric_data": resp}


def add_daily_metric(): 
    payload_daily_metric = json.dumps({
    "query": "{\n  EVM(dataset: archive) {\n    DEXTrades(\n      where: {Trade: {Dex: {ProtocolFamily: {is: \"OpenSea\"}}, Buy: {Currency: {SmartContract: {is: \"0x322e2741c792c1f2666d159bcc6d3a816f98d954\"}}}}}\n    ) {\n      Count_NFTS_bought: sum(of: Trade_Buy_Amount)\n      Block {\n        Date\n      }\n    }\n  }\n}\n",
    "variables": "{}"
    })
    resp = get_data_nft(payload_daily_metric)
    return resp

# Function to insert records into PostgreSQL
def insert_records_metric(transformed_data):
    dictionary = ast.literal_eval(transformed_data)
    dex_trade = dictionary['metric_data']['data']['EVM']['DEXTrades']
    print("dex_trade", dex_trade)
    pg_hook = PostgresHook(postgres_conn_id='postgres_db')
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(
        "CREATE TABLE IF NOT EXISTS daily_metric_table (count_nfts_bought INT, block_date DATE)"
    )
    for record in dex_trade:
        values = (
            record['Count_NFTS_bought'],
            record['Block']['Date']
        )
        cursor.execute(
            "INSERT INTO daily_metric_table (count_nfts_bought, block_date) VALUES (%s, %s)",
            values
        )
    connection.commit()
    cursor.close()
    connection.close()

def print_end():
    print("ETL process completed")


with dag:
    print_begin_task = PythonOperator(
        task_id='print_begin_task',
        python_callable=print_begin,
        dag=dag,
    )

    op_get_data_daily_metric = PythonOperator(
        task_id='op_get_data_daily_metric',
        python_callable=add_daily_metric,
        dag=dag,
    )

    op_load_metric = PythonOperator(
        task_id='op_load_metric',
        python_callable=insert_records_metric,
        op_kwargs={
            'transformed_data': "{{ task_instance.xcom_pull(task_ids='op_get_data_daily_metric') }}",
        }
    )

    print_end_task = PythonOperator(
        task_id='print_end_task',
        python_callable=print_end,
        dag=dag,
    )

    print_begin_task >> op_get_data_daily_metric >>  op_load_metric  >>  print_end_task

