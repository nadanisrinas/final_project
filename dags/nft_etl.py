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
    'nft_etl',
    default_args=default_args,
    description='NFT ETL Pipeline to run every 30 minutes',
    schedule_interval='*/30 * * * *',
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

def display_metric(): 
    payload_metrics = json.dumps({
    "query": "{\n  EVM(dataset: archive) {\n    DEXTrades(\n      where: {Trade: {Dex: {ProtocolFamily: {is: \"OpenSea\"}}, Buy: {Currency: {SmartContract: {is: \"0x322e2741c792c1f2666d159bcc6d3a816f98d954\"}}}}}\n    ) {\n      Count_NFTS_bought: sum(of: Trade_Buy_Amount)\n    }\n  }\n}\n",
    "variables": "{}"
    })
    resp = get_data_nft(payload_metrics)
    return resp

def add_table_buyer(): 
    payload_table = json.dumps({
        "query": "{\n  EVM(dataset: archive, network: eth) {\n    buyside: DEXTrades(\n      limit: {count: 10}\n      orderBy: {descending: Block_Time}\n      where: {Trade: {Buy: {Currency: {SmartContract: {is: \"0x322e2741c792c1f2666d159bcc6d3a816f98d954\"}}}}}\n    ) {\n      Block {\n        Number\n        Time\n      }\n      Transaction {\n        From\n        To\n        Hash\n      }\n      Trade {\n        Buy {\n          Amount\n          Buyer\n          Currency {\n            Name\n            Symbol\n            SmartContract\n          }\n          Seller\n          Price\n        }\n        Sell {\n          Amount\n          Buyer\n          Currency {\n            Name\n            SmartContract\n            Symbol\n          }\n          Seller\n          Price\n        }\n      }\n    }\n    sellside: DEXTrades(\n      limit: {count: 10}\n      orderBy: {descending: Block_Time}\n      where: {Trade: {Buy: {Currency: {SmartContract: {is: \"0x322e2741c792c1f2666d159bcc6d3a816f98d954\"}}}}}\n    ) {\n      Block {\n        Number\n        Time\n      }\n      Transaction {\n        From\n        To\n        Hash\n      }\n      Trade {\n        Buy {\n          Amount\n          Buyer\n          Currency {\n            Name\n            Symbol\n            SmartContract\n          }\n          Seller\n          Price\n        }\n        Sell {\n          Amount\n          Buyer\n          Currency {\n            Name\n            SmartContract\n            Symbol\n          }\n          Seller\n          Price\n        }\n      }\n    }\n  }\n}\n",
    "variables": "{}"
    })
    resp = get_data_nft(payload_table)
    return resp

# Function to insert records into PostgreSQL
def insert_records_metric(transformed_data):
    dictionary = ast.literal_eval(transformed_data)
    count_nfts_bought = dictionary['metric_data']['data']['EVM']['DEXTrades'][0]['Count_NFTS_bought']
    pg_hook = PostgresHook(postgres_conn_id='postgres_db')
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(
        "CREATE TABLE IF NOT EXISTS metric_table (count_nfts_bought INT)"
    )

    cursor.execute(
        "INSERT INTO metric_table (count_nfts_bought) VALUES (%s)",
        [count_nfts_bought]
    )
    connection.commit()
    cursor.close()
    connection.close()

def insert_records_table_buyer(transformed_data):
    dictionary = ast.literal_eval(transformed_data)
    buyside = dictionary['metric_data']['data']['EVM']['buyside']

    pg_hook = PostgresHook(postgres_conn_id='postgres_db')
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(
        "CREATE TABLE IF NOT EXISTS buyer_table (block_number INT, block_time DATE , trade_buy_amount INT, trade_buy_buyer VARCHAR(100), trade_buy_currency_name VARCHAR(100), trade_buy_currency_smartcontract VARCHAR(255), trade_buy_currency_symbol VARCHAR(500), trade_buy_price INT, trade_buy_seller VARCHAR(255), trade_sell_amount VARCHAR(255), trade_sell_buyer VARCHAR(255), trade_sell_currency_name VARCHAR(255), trade_sell_currency_smartcontract VARCHAR(255),trade_sell_currency_symbol VARCHAR(255), trade_sell_price INT, trade_sell_seller VARCHAR(255), transaction_from  VARCHAR(255),transaction_hash VARCHAR(255),transaction_to VARCHAR(255))"
    )

    for record in buyside:
        values = (
            record['Block']['Number'],
            record['Block']['Time'],
            record['Trade']['Buy']['Amount'],
            record['Trade']['Buy']['Buyer'],
            record['Trade']['Buy']['Currency']['Name'],
            record['Trade']['Buy']['Currency']['SmartContract'],
            record['Trade']['Buy']['Currency']['Symbol'],
            record['Trade']['Buy']['Price'],
            record['Trade']['Buy']['Seller'],
            record['Trade']['Sell']['Amount'],
            record['Trade']['Sell']['Buyer'],
            record['Trade']['Sell']['Currency']['Name'],
            record['Trade']['Sell']['Currency']['SmartContract'],
            record['Trade']['Sell']['Currency']['Symbol'],
            record['Trade']['Sell']['Price'],
            record['Trade']['Sell']['Seller'],
            record['Transaction']['From'],
            record['Transaction']['Hash'],
            record['Transaction']['To'],
        )
        cursor.execute(
            "INSERT INTO buyer_table (block_number, block_time , trade_buy_amount, trade_buy_buyer, trade_buy_currency_name, trade_buy_currency_smartcontract, trade_buy_currency_symbol, trade_buy_price, trade_buy_seller, trade_sell_amount, trade_sell_buyer, trade_sell_currency_name, trade_sell_currency_SmartContract,trade_sell_currency_symbol, trade_sell_price,trade_sell_seller,  transaction_from ,transaction_hash,transaction_to) VALUES (%s, %s, %s, %s,%s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
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

    op_get_data_metric = PythonOperator(
        task_id='op_get_data_metric',
        python_callable=display_metric,
        dag=dag,
    )

    op_get_data_table_buyer = PythonOperator(
        task_id='op_get_data_table_buyer',
        python_callable=add_table_buyer,
        dag=dag,
    )

    op_load_metric = PythonOperator(
        task_id='op_load_metric',
        python_callable=insert_records_metric,
        op_kwargs={
            'transformed_data': "{{ task_instance.xcom_pull(task_ids='op_get_data_metric') }}",
        }
    )

    op_load_table_buyer = PythonOperator(
        task_id='op_load_table_buyer',
        python_callable=insert_records_table_buyer,
        op_kwargs={
            'transformed_data': "{{ task_instance.xcom_pull(task_ids='op_get_data_table_buyer') }}",
        }
    )

    print_end_task = PythonOperator(
        task_id='print_end_task',
        python_callable=print_end,
        dag=dag,
    )

    print_begin_task >> op_get_data_metric >>  op_load_metric  >>  print_end_task
    print_begin_task >> op_get_data_table_buyer >> op_load_table_buyer >> print_end_task

