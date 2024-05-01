import streamlit as st
import pandas as pd
import json


def display_metric(): 
    st.title ("NFT Dashboard")
    st.header("Punk Evil Rabbit NFT")
    st.metric("Count of Punk Evil Rabbit NFTS Bought",count_nfts_bought)

def add_table():
    st.subheader("Latest DEX Trades")
    data_table= resp1['data']['EVM']['buyside']
    df = pd.json_normalize(data_table)
    st.dataframe(df)

def add_chart():
    chart_data=json.loads(resp1)['data']['EVM']['DEXTrades']
    df_chart = pd.json_normalize(chart_data)
    df_chart.columns = ['Count_NFTS_bought', 'Block_Date']
    # Convert the 'Count_NFTS_bought' column to integer data type
    df_chart['Count_NFTS_bought'] = df_chart['Count_NFTS_bought'].astype(int)
    df_chart['Block_Date'] = pd.to_datetime(df_chart['Block_Date'])

    st.subheader('Daily Metrics')
    st.line_chart(df_chart,x='Block_Date',y='Count_NFTS_bought')