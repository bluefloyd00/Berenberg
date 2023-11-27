import time
import pandas as pd
import numpy as np

# Suppressing the SettingWithCopyWarning in pandas
pd.options.mode.chained_assignment = None 

# Decorator to measure the execution time of functions
def time_performance(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        print(f"Execution time of {func.__name__}: {end_time - start_time:.2f} seconds")
        return result
    return wrapper

# Function to load data from a parquet file
@time_performance
def load_data(file_path):
    print(f"loading {file_path}")
    return pd.read_parquet(file_path)

# Function to analyze execution data
@time_performance
def analyse_executions(df):
    num_executions = len(df)
    unique_venues = df['Venue'].nunique()
    df['TradeTime'] = pd.to_datetime(df['TradeTime'])
    execution_dates = df['TradeTime'].dt.date.unique()
    formatted_dates = [date.strftime('%Y-%m-%d') for date in execution_dates]

    return {
        "# Executions": num_executions,
        "Unique Venues": unique_venues,
        "Execution Dates": formatted_dates
        }
    
# Function for cleaning data
@time_performance
def data_cleaning(df):
    # Filtering data for continuous trading phase
    continuous_trading_df = df[df['Phase'] == 'CONTINUOUS_TRADING']
    return continuous_trading_df

# Function for data transformation
@time_performance
def data_transformation(executions_df, refdata_df):   
    # Adding a 'side' column based on 'Quantity'    
    executions_df['side'] = np.where(executions_df['Quantity'] > 0, 1, 2)      
    merged_df = executions_df.merge(refdata_df, on=['ISIN', 'Currency'], how='left')
    
    return merged_df

# Function to perform various calculations
@time_performance
def calculations(executions, marketdata):
    # Renaming 'id' column if it exists
    if 'id' in executions.columns:
        executions = executions.rename(columns={"id": "listing_id"})

    # Converting timestamps to datetime
    marketdata['Time'] = pd.to_datetime(marketdata['event_timestamp'])
    
    # In cases where multiple marketdata transactions occur within the same millisecond for a given execution, we will consider only the latest transaction.
    executions['TradeTime'] = executions['TradeTime'] + pd.to_timedelta(0.000999999, unit='s')

    # Sorting DataFrames by time
    executions.sort_values(by='TradeTime', inplace=True)
    marketdata.sort_values(by='Time', inplace=True)

    # Calculating for various time offsets
    time_offsets = [-1, 0, 1]  
    for offset in time_offsets:
        # Creating new time column for each offset
        marketdata[f'Time_{offset}'] = marketdata['Time'] + pd.Timedelta(seconds=offset)

        # Renaming marketdata columns based on offset
        suffix = {  1: '_min_1s', 0: '', -1: '_1s' }[offset]
        marketdata_renamed = marketdata.rename(columns={'best_bid_price': f'best_bid_price{suffix}', 
                                                        'best_ask_price': f'best_ask_price{suffix}'})
        
        # Merging data using the asof merge method
        executions = pd.merge_asof(executions,
                                   marketdata_renamed[['listing_id', f'best_bid_price{suffix}', f'best_ask_price{suffix}', f'Time_{offset}']],
                                   left_on='TradeTime',
                                   right_on=f'Time_{offset}',
                                   by='listing_id',
                                   direction='backward')

        # Calculating mid-price for each offset
        executions[f'mid_price{suffix}'] = (executions[f'best_bid_price{suffix}'] + executions[f'best_ask_price{suffix}']) / 2
        executions.drop(f'Time_{offset}', axis=1, inplace=True)
    
    # Calculating slippage
    conditions = [
        executions['Quantity'] < 0,
        executions['Quantity'] > 0         
    ]
    choices = [        
        (executions['Price'] - executions['best_bid_price']) / (executions['best_ask_price'] - executions['best_bid_price']),
        (executions['best_ask_price'] - executions['Price']) / (executions['best_ask_price'] - executions['best_bid_price'])        
    ]
    executions['slippage'] = np.select(conditions, choices, default=np.nan)

    return executions

# Main function to orchestrate the data processing  
@time_performance
def main():
    exectuions_file = 'data/exectuions.parquet'
    marketdata_file = 'data/marketdata.parquet'
    refdata_file = 'data/refdata.parquet' 
    exectuions_df = load_data(exectuions_file)
    refdata_df = load_data(refdata_file)
    marketdata_df = load_data(marketdata_file)

    print(f'1) { ", ".join([f"{k}: {v}" for k, v in analyse_executions(exectuions_df).items()]) }')

    continues_trading_df = data_cleaning(exectuions_df)
    
    assert len(exectuions_df) > len(continues_trading_df)
    
    print(f"2) # CONTINUOUS_TRADING executions: {len(continues_trading_df)}")
    data_transformation_df = data_transformation(continues_trading_df, refdata_df)

    assert len(data_transformation_df) == len(continues_trading_df)

    final_df = calculations(data_transformation_df, marketdata_df)
    
    assert len(data_transformation_df) == len(final_df)

    final_df.to_parquet('data/result.parquet')


if __name__ == '__main__':
    main()