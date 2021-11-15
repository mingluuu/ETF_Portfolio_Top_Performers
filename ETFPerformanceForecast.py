#<ETF PERFORMANCE COMPARISON V.1.0  Latest Update 2021.11.14> 
#Visualize and Publish Performance Comparison Report - Our portfolio performnace vs. benchmarks (ETFs, SPY(S&P 500), QQQ(Nasdaq 100) 
#Run MonteCarlo Simulation to Forecast Performance with Past 2 years history
#Author: Minglu Li and Ken Lee 

def get_combined_avg_daily_return(p_portfolio_df, p_start_date, p_end_date, p_name):
    names = hist.get_where_condition(p_portfolio_df, 'symbol')
    names
    sql_query = f"""
    SELECT date, symbol, close FROM STOCK_HISTORY WHERE (date > '{p_start_date}' and date <= '{p_end_date}') and symbol in ({names})
    """
    portfolio_df = pd.read_sql_query(sql_query, eft_data_connection_string)
    stock_hist_matrix = portfolio_df.pivot('date','symbol',values = 'close')  
    stock_hist_matrix = stock_hist_matrix.pct_change().dropna()
    stock_hist_matrix['daily_return'] = stock_hist_matrix.mean(numeric_only=True, axis=1)
    stock_hist_matrix[p_name] = stock_hist_matrix['daily_return']
    return(stock_hist_matrix[[p_name]])

def back_calc_price100_from_daily_return(p_dataframe, p_name):
    start_value = 100
    for index, row in p_dataframe.iterrows():
        row[p_name] = (row[p_name] + 1) * start_value
        start_value = row[p_name]
    return p_dataframe