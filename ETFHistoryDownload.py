#ETF HISTORY
#Download historical Data and store into DB - Rabia Talib

# Import Modules
import pandas as pd
import os
import json
import requests
from dotenv import load_dotenv
import matplotlib.pyplot as plt
import alpaca_trade_api as tradeapi
#import yfinance as yf
from pathlib import Path
import sqlalchemy as sql
from datetime import date
import logging
from dateutil.relativedelta import relativedelta
%matplotlib inline

load_dotenv()

alpaca_api_key = os.getenv("ALPACA_API_KEY")
alpaca_secret_key = os.getenv("ALPACA_SECRET_KEY")

# Database connection string
eft_data_connection_string = 'sqlite:///./Resources/etf.db'
# Database engine
etf_data_engine = sql.create_engine(eft_data_connection_string, echo=True)

# Create the Alpaca API object
alpaca = tradeapi.REST(
    alpaca_api_key,
    alpaca_secret_key,
    api_version="v2")

def drop_table(p_table_name):
    connection = etf_data_engine.raw_connection()
    cursor = connection.cursor()
    command = "DROP TABLE IF EXISTS {};".format(p_table_name)
    cursor.execute(command)
    connection.commit()
    cursor.close()


def fetch_hitorical_data(p_tickers, p_startDt, p_endDt):
    timeframe = "1D"
    start_date = pd.Timestamp(p_startDt, tz="America/New_York").isoformat()
    end_date = pd.Timestamp(p_endDt, tz="America/New_York").isoformat()
    
    df_hist_data = alpaca.get_barset(
    p_tickers,
    timeframe,
    limit = 1000,
    start = start_date,
    end = end_date,
    ).df
    
    
    #loop thru tickets and insert into data
    for symbol in p_tickers:
        close_df = df_hist_data[symbol]
        close_df['date'] = pd.to_datetime(close_df.index).date
        close_df.index = pd.to_datetime(close_df.index).date
        close_df = close_df.drop(columns = ['open','high','low'])
        close_df.insert(0, 'symbol', symbol)
        close_df.to_sql('STOCK_HISTORY', etf_data_engine, index=True, if_exists='append')

def get_market_datas_by_period(p_today):
    day_1 = p_today + relativedelta(days=-1)
    year_1 = day_1 + relativedelta(years=-1)
    year_2 = day_1 + relativedelta(years=-2)
    year_3 = day_1 + relativedelta(years=-3)
    day_2 = day_1 + relativedelta(days=-2)
    week_1 = day_1 + relativedelta(weeks=-1)
    month_1 = day_1 + relativedelta(months=-1)
    month_3 = day_1 + relativedelta(months=-6)
    month_6 = day_1 + relativedelta(months=-6)
    ytd = date(day_1.year, 1, 1)
    
    sql_query = f"""
    SELECT * FROM (SELECT 'D0' as period,date from STOCK_HISTORY order by date desc LIMIT 1)
    UNION
    SELECT * FROM (SELECT 'D7_W1' as period,date from STOCK_HISTORY where '{week_1}' <= date order by date asc LIMIT 1)
    UNION
    SELECT * FROM (SELECT 'M1' as period,date from STOCK_HISTORY where '{month_1}' <= date order by date asc LIMIT 1)
    UNION
    SELECT * FROM (SELECT 'M3' as period,date from STOCK_HISTORY where '{month_3}' <= date order by date asc LIMIT 1)
    UNION
    SELECT * FROM (SELECT 'M6' as period,date from STOCK_HISTORY where '{month_6}' <= date order by date asc LIMIT 1)
    UNION
    SELECT * FROM (SELECT 'Y1' as period,date from STOCK_HISTORY where '{year_1}' <= date order by date asc LIMIT 1)
    UNION
    SELECT * FROM (SELECT 'Y2' as period,date from STOCK_HISTORY where '{year_2}' <= date order by date asc LIMIT 1)
    UNION
    SELECT * FROM (SELECT 'Y3' as period,date from STOCK_HISTORY where '{year_3}' <= date order by date asc LIMIT 1)
    UNION
    SELECT * FROM (SELECT 'Y0_YTD' as period,date from STOCK_HISTORY where '{ytd}' <= date order by date asc LIMIT 1)
    """
    history_dates = pd.read_sql_query(sql_query, eft_data_connection_string)
    history_dates = history_dates.sort_values(by=['date'], ascending=False)
    return history_dates
            

def get_market_dates_list_condition(p_history_dates):
    where_dates = "" 
    for index, row in p_history_dates.iterrows():
        if where_dates == "":
            where_dates = f"'{row['date']}'"
        else:
            where_dates = f"{where_dates}, '{row['date']}'"
    return where_dates
    
def get_price_history_by_period(p_today):
    history_dates = get_market_datas_by_period(p_today)
    where_dates = get_market_dates_list_condition(history_dates)
    sql_query = f"""
    SELECT * FROM STOCK_HISTORY WHERE date in ({where_dates})
    """
    stock_history = pd.read_sql_query(sql_query, eft_data_connection_string)        
    #stock_history        
    history_df = stock_history.merge(history_dates, on="date", how='inner')
    price_hist_matrix = history_df.pivot('symbol','period',values = 'close')     
    
    return price_hist_matrix
    
def get_performance_by_period(p_today, p_w_px):
    price_matrix = get_price_history_by_period(date.today())
    price_matrix['D7_W1%'] = ((price_matrix['D0']/price_matrix['D7_W1'])-1) * 100
    price_matrix['M1%'] = ((price_matrix['D0']/price_matrix['M1'])-1) * 100
    price_matrix['M3%'] = ((price_matrix['D0']/price_matrix['M3'])-1) * 100
    price_matrix['M6%'] = ((price_matrix['D0']/price_matrix['M6'])-1) * 100
    price_matrix['Y0_YTD%'] = ((price_matrix['D0']/price_matrix['Y0_YTD'])-1) * 100
    price_matrix['Y1%'] = ((price_matrix['D0']/price_matrix['Y1'])-1) * 100
    price_matrix['Y2%'] = ((price_matrix['D0']/price_matrix['Y2'])-1) * 100
    price_matrix['Y3%'] = ((price_matrix['D0']/price_matrix['Y3'])-1) * 100
    
    if p_w_px == False:
        price_matrix = price_matrix.drop(columns=['D7_W1', 'M1', 'M3', 'M6', 'Y0_YTD', 'Y1', 'Y2', 'Y3'])
        price_matrix = price_matrix.rename({'D0':'D0_PX'}, axis = 1)
    return price_matrix
