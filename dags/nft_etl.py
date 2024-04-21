from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
import os
# from dotenv import load_dotenv

import streamlit as st
import http.client
import json
import pandas as pd
import config
# Define the default_args dictionary to specify the default parameters of the DAG
default_args = {
    "depends_on_past": False,
    'owner': 'airflow',
    'start_date': datetime(2023, 12, 16),
}

# Create a DAG instance
dag = DAG(
    'nft_etl',
    default_args=default_args,
    description='Weather ETL Pipeline to run every 15 minutes',
    schedule_interval='*/15 * * * *'
)

def print_begin():
    # load_dotenv()
    print("Begin ETL process")

def run_python_script():
    # Import your Python script here
    conn = http.client.HTTPSConnection("streaming.bitquery.io")
    payload = json.dumps({
    "query": "{\n  EVM(dataset: archive) {\n    DEXTrades(\n      where: {Trade: {Dex: {ProtocolFamily: {is: \"OpenSea\"}}, Buy: {Currency: {SmartContract: {is: \"0x322e2741c792c1f2666d159bcc6d3a816f98d954\"}}}}}\n    ) {\n      Count_NFTS_bought: sum(of: Trade_Buy_Amount)\n    }\n  }\n}\n",
    "variables": "{}"
    })
    headers = {
    'Content-Type': 'application/json',
        'Authorization': f"Bearer ory_at_t4Z7IRE2WrccGhR3lqqoLElGlN5IGmMehtzFXMkrJcY.PJlf92EVE7hk2LrD2taMYNk-E9fDAtEus4m9JXF_kPM",
    }
    conn.request("POST", "/graphql", payload, headers)
    res = conn.getresponse()
    data = res.read()
    print("data", data)
    resp= json.loads( data.decode("utf-8"))

    count_nfts_bought = resp['data']['EVM']['DEXTrades'][0]['Count_NFTS_bought']
    st.title ("NFT Dashboard")
    st.header("Punk Evil Rabbit NFT")
    st.metric("Count of Punk Evil Rabbit NFTS Bought",count_nfts_bought)
    payload_table = json.dumps({
        "query": "{\n  EVM(dataset: archive, network: eth) {\n    buyside: DEXTrades(\n      limit: {count: 10}\n      orderBy: {descending: Block_Time}\n      where: {Trade: {Buy: {Currency: {SmartContract: {is: \"0x322e2741c792c1f2666d159bcc6d3a816f98d954\"}}}}}\n    ) {\n      Block {\n        Number\n        Time\n      }\n      Transaction {\n        From\n        To\n        Hash\n      }\n      Trade {\n        Buy {\n          Amount\n          Buyer\n          Currency {\n            Name\n            Symbol\n            SmartContract\n          }\n          Seller\n          Price\n        }\n        Sell {\n          Amount\n          Buyer\n          Currency {\n            Name\n            SmartContract\n            Symbol\n          }\n          Seller\n          Price\n        }\n      }\n    }\n    sellside: DEXTrades(\n      limit: {count: 10}\n      orderBy: {descending: Block_Time}\n      where: {Trade: {Buy: {Currency: {SmartContract: {is: \"0x322e2741c792c1f2666d159bcc6d3a816f98d954\"}}}}}\n    ) {\n      Block {\n        Number\n        Time\n      }\n      Transaction {\n        From\n        To\n        Hash\n      }\n      Trade {\n        Buy {\n          Amount\n          Buyer\n          Currency {\n            Name\n            Symbol\n            SmartContract\n          }\n          Seller\n          Price\n        }\n        Sell {\n          Amount\n          Buyer\n          Currency {\n            Name\n            SmartContract\n            Symbol\n          }\n          Seller\n          Price\n        }\n      }\n    }\n  }\n}\n",
    "variables": "{}"
    })

    conn.request("POST", "/graphql", payload_table, headers)
    res1 = conn.getresponse()
    data1 = res1.read()
    resp1= json.loads( data1.decode("utf-8"))

    st.subheader("Latest DEX Trades")

    data_table= resp1['data']['EVM']['buyside']
    df = pd.json_normalize(data_table)
    st.dataframe(df)
    ## chart
    payload3 = json.dumps({
    "query": "{\n  EVM(dataset: archive) {\n    DEXTrades(\n      where: {Trade: {Dex: {ProtocolFamily: {is: \"OpenSea\"}}, Buy: {Currency: {SmartContract: {is: \"0x322e2741c792c1f2666d159bcc6d3a816f98d954\"}}}}}\n    ) {\n      Count_NFTS_bought: sum(of: Trade_Buy_Amount)\n      Block {\n        Date\n      }\n    }\n  }\n}\n",
    "variables": "{}"
    })

    conn.request("POST", "/graphql", payload3, headers)
    res3 = conn.getresponse()
    data3 = res3.read()

    chart_data=json.loads(data3)['data']['EVM']['DEXTrades']

    df_chart = pd.json_normalize(chart_data)
    df_chart.columns = ['Count_NFTS_bought', 'Block_Date']
    # Convert the 'Count_NFTS_bought' column to integer data type
    df_chart['Count_NFTS_bought'] = df_chart['Count_NFTS_bought'].astype(int)
    df_chart['Block_Date'] = pd.to_datetime(df_chart['Block_Date'])

    st.subheader('Daily Metrics')
    st.line_chart(df_chart,x='Block_Date',y='Count_NFTS_bought')

def print_end():
    print("ETL process completed")


with dag:
    print_begin_task = PythonOperator(
        task_id='print_begin_task',
        python_callable=print_begin,
        dag=dag,
    )

    execute_etl_task = PythonOperator(
        task_id='execute_etl_task',
        python_callable=run_python_script,
        dag=dag,
    )

    print_end_task = PythonOperator(
        task_id='print_end_task',
        python_callable=print_end,
        dag=dag,
    )

    print_begin_task >> execute_etl_task >> print_end_task


