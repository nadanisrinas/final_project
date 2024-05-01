import streamlit as st
import pandas as pd
import json

conn = st.connection("postgresql", type="sql")
def display_metric(): 
    result = conn.query('SELECT * FROM metric_table LIMIT 1;', ttl="10m")
    df = pd.DataFrame(result)
    st.title ("NFT Dashboard")
    st.header("Punk Evil Rabbit NFT")
    for row in df.itertuples():
        st.metric("Count of Punk Evil Rabbit NFTS Bought",row.count_nfts_bought)

def add_table():
    st.subheader("Latest DEX Trades")
    result = conn.query('SELECT * FROM buyer_table;', ttl="10m")
    df = pd.DataFrame(result, columns=['block_number', 'block_time' , 'trade_buy_amount', 'trade_buy_buyer', 'trade_buy_currency_name', 'trade_buy_currency_smartcontract', 'trade_buy_currency_symbol', 'trade_buy_price', 'trade_buy_seller', 'trade_sell_amount', 'trade_sell_buyer', 'trade_sell_currency_name', 'trade_sell_currency_SmartContract','trade_sell_currency_symbol', 'trade_sell_price','trade_sell_seller',  'transaction_from' ,'transaction_hash','transaction_to'])
    st.dataframe(df)

def add_chart():
    st.subheader('Daily Metrics')
    result = conn.query('SELECT * FROM daily_metric_table;', ttl="10m")
    df = pd.DataFrame(result, columns=['count_nfts_bought', 'block_date'])

    df['block_date'] = pd.to_datetime(df['block_date'])
    print(df)

    st.line_chart(df.set_index('block_date')['count_nfts_bought'])

display_metric()
add_table()
add_chart()



