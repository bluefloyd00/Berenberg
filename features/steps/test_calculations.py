from behave import given, when, then, use_step_matcher
import pandas as pd
from pipeline import calculations 

use_step_matcher("re")

def convert_to_numeric(df):
    return df.map(lambda x: pd.to_numeric(x, errors='ignore'))

@given('I have a mock parquet file with the following execution data merged with reference data')
def step_impl(context):  
    print("pippo4")
    context.executions = pd.DataFrame([row.as_dict() for row in context.table.rows])
    context.executions = convert_to_numeric(context.executions)
    context.executions['TradeTime'] = pd.to_datetime(context.executions['TradeTime'])
    
    print(context.executions)
    

@given('I have a mock parquet file with the following market data')
def step_impl(context):
    print("pippo5")
    context.marketdata = pd.DataFrame([row.as_dict() for row in context.table.rows])
    context.marketdata = convert_to_numeric(context.marketdata)
    context.marketdata['event_timestamp'] = pd.to_datetime(context.marketdata['event_timestamp'])

@when('I calculate the trading metrics for each trade')
def step_impl(context):
    print("pippo6")
    print("pippo3")
    print(context.executions)
    context.result = calculations(context.executions, context.marketdata)    

@then('I should get the following results')
def step_impl(context):
    expected = pd.DataFrame([row.as_dict() for row in context.table.rows]) 
    expected = convert_to_numeric(expected)   
    expected['TradeTime'] = pd.to_datetime(expected['TradeTime'])

    pd.testing.assert_frame_equal(context.result.sort_index(axis=1), expected.sort_index(axis=1))
