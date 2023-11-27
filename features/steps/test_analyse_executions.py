from behave import given, when, then
import pandas as pd
from pipeline import analyse_executions
from io import StringIO

@given('I have a mock parquet file with the following execution data')
def step_impl(context):
    context.df = pd.DataFrame([row.as_dict() for row in context.table.rows])
    context.df['TradeTime'] = pd.to_datetime(context.df['TradeTime'])

@when('I analyze the mock parquet file')
def step_impl(context):
    context.result = analyse_executions(context.df)

@then('I should get the correct count of {executions} executions, {venues} unique venue, and the formatted execution date {date}')
def step_impl(context, executions, venues, date):
    assert context.result['# Executions'] == int(executions)
    assert context.result['Unique Venues'] == int(venues)
    assert context.result['Execution Dates'] == [date]
