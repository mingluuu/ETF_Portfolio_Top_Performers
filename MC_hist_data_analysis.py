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
from MCForecastTools import MCSimulation

def performance_forecast(etf_df, sim_num=500, term=10):
    
    # Compute MC Confidence Interval for 'term' years
    MC_sim = MCSimulation(
        portfolio_data = etf_df,
        num_simulation = sim_num,
        num_trading_days = 252*term)
    cum_return = MC_sim.summarize_cumulative_return()
    ci_lower = cum_return[8]
    ci_upper = cum_return[9]
    
    return ci_lower, ci_upper

# Load Alpaca Key
load_dotenv("./SAMPLE.env")
alpaca_api_key = os.getenv("ALPACA_API_KEY")
alpaca_secret_key = os.getenv("ALPACA_SECRET_KEY")

# Create the Alpaca API object
alpaca = tradeapi.REST(
    alpaca_api_key,
    alpaca_secret_key,
    api_version="v2")

tickers = ["RYT","XSW","XLK","USRT","XLRE","RWR","BBUS","JMOM","SPMD","MDYG","SLY","SLYG","SPY", "QQQ", "GLD"]
timeframe = "1D"
start_date = pd.Timestamp("2019-10-01", tz="America/New_York").isoformat()
end_date = pd.Timestamp("2021-10-01", tz="America/New_York").isoformat()

df_hist_data = alpaca.get_barset(
    tickers,
    timeframe,
    start = start_date,
    end = end_date,
    limit = 1000
).df

print(df_hist_data.head())

# Load etf list from csv
csv_path = Path('./Resources/etf_list.csv')
etf_list = pd.read_csv(csv_path)['etf']

etf_list_3extra = ["SPY", "QQQ", "GLD"]

# Initialize parameters
etf_final_list = []
ci_lower_10yrs_list = []
ci_upper_10yrs_list = []
ci_lower_20yrs_list = []
ci_upper_20yrs_list = []
ci_lower_30yrs_list = []
ci_upper_30yrs_list = []

# Loop over etf list and compute Confidence Interval for each etf
for i in range(len(etf_list)):
    current_etf = etf_list[i]
    print("Working on ETF: " + current_etf)
    etf_df = df_hist_data.loc[:,[current_etf]]
    # Do 10, 20, 30 years MC for current etf respectively
    print("Computing 10 years return ... ")
    ci_lower_10yrs, ci_upper_10yrs = performance_forecast(etf_df, sim_num=500, term=10)
    print("Computing 20 years return ... ")
    ci_lower_20yrs, ci_upper_20yrs = performance_forecast(etf_df, sim_num=500, term=20)
    print("Computing 30 years return ... ")
    ci_lower_30yrs, ci_upper_30yrs = performance_forecast(etf_df, sim_num=500, term=30)
    # Store results into list
    etf_final_list.append(current_etf)
    ci_lower_10yrs_list.append(ci_lower_10yrs)
    ci_upper_10yrs_list.append(ci_upper_10yrs)
    ci_lower_20yrs_list.append(ci_lower_20yrs)
    ci_upper_20yrs_list.append(ci_upper_20yrs)
    ci_lower_30yrs_list.append(ci_lower_30yrs)
    ci_upper_30yrs_list.append(ci_upper_30yrs)
    
# Loop over 3 extra etf and compute Confidence Interval for each etf
for i in range(len(etf_list_3extra)):
    current_etf = etf_list_3extra[i]
    print("Working on ETF: " + current_etf)
    etf_df = df_hist_data.loc[:,[current_etf]]
    # Do 10, 20, 30 years MC for current etf respectively
    print("Computing 10 years return ... ")
    ci_lower_10yrs, ci_upper_10yrs = performance_forecast(etf_df, sim_num=500, term=10)
    print("Computing 20 years return ... ")
    ci_lower_20yrs, ci_upper_20yrs = performance_forecast(etf_df, sim_num=500, term=20)
    print("Computing 30 years return ... ")
    ci_lower_30yrs, ci_upper_30yrs = performance_forecast(etf_df, sim_num=500, term=30)
    # Store results into list
    etf_final_list.append(current_etf)
    ci_lower_10yrs_list.append(ci_lower_10yrs)
    ci_upper_10yrs_list.append(ci_upper_10yrs)
    ci_lower_20yrs_list.append(ci_lower_20yrs)
    ci_upper_20yrs_list.append(ci_upper_20yrs)
    ci_lower_30yrs_list.append(ci_lower_30yrs)
    ci_upper_30yrs_list.append(ci_upper_30yrs)

# Create ETF performance datafram
etf_performance_df = pd.DataFrame()
etf_performance_df['ETF'] = etf_final_list
etf_performance_df['ci_lower_10yrs'] = ci_lower_10yrs_list
etf_performance_df['ci_upper_10yrs'] = ci_upper_10yrs_list
etf_performance_df['ci_lower_20yrs'] = ci_lower_20yrs_list
etf_performance_df['ci_upper_20yrs'] = ci_upper_20yrs_list
etf_performance_df['ci_lower_30yrs'] = ci_lower_30yrs_list
etf_performance_df['ci_upper_30yrs'] = ci_upper_30yrs_list

print(etf_performance_df)