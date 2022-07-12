import json
import os
import pymysql
import pandas as pd 
import numpy as np
import logging
import sys
import awswrangler as wr
from pandas_datareader import data as pdr
from datetime import datetime, timedelta
import boto3

"""
            Didier Bouba Ndengue
                7/9/2022

Function used to:
1. connect to AWS RDS to retrieve stocks tickers
2. call Yahoo Finance API to retrieve closing price of each stock - date used 
        should be current date
3. Transform data, convert into parquet and store into S3 bucket

"""

rds_host = os.getenv('DB_HOST')
name = os.getenv('DB_USER')
password = os.getenv('DB_PASSWORD')
db_name = os.getenv('DB_DATABASE')

logger = logging.getLogger()
logger.setLevel(logging.INFO)



try:
    conn = pymysql.connect(host=rds_host, user=name, passwd=password, db=db_name, connect_timeout=5)
    logger.info("connection successful")
except pymysql.MySQLError as e:
    logger.error("ERROR: Unexpected error: Could not connect to MySQL instance.")
    logger.error(e)
    conn.close()
    sys.exit()

s3= boto3.client('s3')

def lambda_handler(event, context):
    # Get the previous closing date as start_dt
    previous_cls_dt = get_previous_day()
    start = datetime(int(previous_cls_dt.strftime('%Y')), int(previous_cls_dt.strftime('%m')), int(previous_cls_dt.strftime('%d')))
    end = datetime(int(datetime.today().strftime('%Y')), int(datetime.today().strftime('%m')), int(datetime.today().strftime('%d')))
    logger.info('Getting Stocks tickers from the database: ')
    tickers_list = retrieve_stocks_from_db()
    #Call the Yahoo API to get the daily closing price of the stock
    daily_stocks_df = get_daily_price(tickers_list, start, end)
    test_df = adjust_stock_dataframe(daily_stocks_df)
    #print("done")
    #write_dataframe_in_s3(test_df, end)
    # Convert file into JSON and push it into firehose
    #push_file_to_firehose(test_df)
    # Update the RDS 
    update_rds_after_api_call(tickers_list)
    # closing the databse
    logger.info("database closed")
    conn.close()
    
    
def retrieve_stocks_from_db():
    result = []
    mycur = conn.cursor()
    query = "select  distinct symbol from STOCKS where LAST_UPDATED IS NULL LIMIT 100;"
    try:
        logger.info('Getting data from the database')
        mycur.execute(query)
        apple_df = mycur.fetchall()
        for data in apple_df:
            result.append(data[0])
        conn.commit()
        
    except Exception as e:
        logger.error("Error: Unexpected error while retrieving data")
        logger.error(e)
        conn.close()
    return result
    
def get_daily_price(stocks_tickers, start_dt, end_dt):
    '''
    Input: Ticker list
    call yahoo api for each item in the list
    '''
    if len(stocks_tickers) > 0:
        all_data = pd.DataFrame()
        final_tickers = []
        logger.info(f"pulling data from Yahoo API from {start_dt} to {end_dt}")
        for x in stocks_tickers:
            try:
                all_data = pd.merge(all_data, pd.DataFrame(pdr.get_data_yahoo(x, \
                start=start_dt, end=end_dt)['Adj Close']), right_index=True, left_index=True,\
                suffixes=('', '_delme'), how='outer')
                final_tickers.append(x)
                logger.info(f"Successfully reading data for {x}")
            except:
                logger.info(f"Error reading data for {x}")
                next
        all_data.columns = final_tickers
        all_data = all_data.dropna(axis='columns')
        return all_data.reset_index()
        
def adjust_stock_dataframe(stocks_df):
    #getting the date dataframe
    date_df = stocks_df['Date']
    # Getting the tickers from the dataframe excluding Date
    tickers = stocks_df.columns.to_numpy()[1::]
    for ticker in tickers:
        time_df = pd.DataFrame(date_df)
        time_df['DateTypeCol'] = np.datetime_as_string(time_df['Date'], unit='D')
        time_df.drop('Date', inplace=True, axis=1)
        # rounding the dataframe to two digit
        ticker_df = pd.DataFrame(stocks_df[ticker]).round(2)
        data_df = pd.concat([time_df, ticker_df], axis=1)
        data_df['Ticker'] = str(ticker)
        tup_data = [tuple(x) for x in data_df.to_numpy()]
        final_df = pd.DataFrame(tup_data, columns = ['Date','Price','Symbol'])
        push_file_to_firehose(final_df)
    return final_df
"""
def write_dataframe_in_s3(stocks_df, end_dt):
    ''' Get the daily stocks dataframe
    write in S3 in parquet format
    '''
    
    bucket='bndnetworks'
    time_string = end_dt.strftime('%Y-%m-%d') 
    s3_path = f'data=daily/{time_string}/'
    write_path=f"s3://{bucket}/{s3_path}"
    database='default'
    dtype = {'Date': 'string',
    'Price': 'double',
    'Symbol': 'string'}
    table = 'daily_stocks'
    description= ' table with daily price of stocks'
    comments = {'Date': 'Date when the price has been obtained from Yahoo',
    'Price':'closing prince of the stock',
    'Symbol': 'Stock ticker'}
    partition = ['Date']
    
    try:
        print("writting file to parquet")
        wr.s3.to_parquet(df=stocks_df, path=write_path, dataset=True, mode='overwrite')
        logger.info("successfully wrote file into s3")
    except Exception as e:
        logger.error("Error while writing into S3")
        logger.error(e)
"""
def push_file_to_firehose(stocks_df):
    """
    jstr = json.dumps(result,
                   default=lambda df: json.loads(df.to_json()))
    newresult = json.loads(jstr)"""
    fh = boto3.client('firehose')
    stocks_json = stocks_df.to_json(orient='records').strip('[]')
    logger.info(stocks_json)
    try: 
        fh.put_record(DeliveryStreamName='PUT-S3-bndnetworks', \
        Record={'Data': json.dumps(stocks_json)} )
        logger.info("Successfully pushed the file into Firehose")
    except Exception as e:
        logging.error(e)
        
        
def update_rds_after_api_call(tickers_df):
    '''
    This function is used to update the MySql db after the Yahoo Finance
    API is called. the last_updated column will be put the todays date. so that
    the next lambda call will not read data that have already been read.
    '''
    # query: update stock where symbols in tickers_df
    params = tuple(set(tickers_df))
    today = datetime.today().strftime('%Y-%m-%d')
    mycur = conn.cursor()
    query = f"update STOCKS set LAST_UPDATED = '{today}' where symbol in {params}"
    try:
        # call the database
        mycur.execute(query)
        logger.info("successfully updated the stock table")
        conn.commit()
    except Exception as e:
        logger.error('ERROR updating the Stock database')
        logger.error(e)

def get_previous_day():
    '''
    Function to return the previous closing date
    If it is monday, return Friday since the market is close
    on Sundays 
    '''
    logger.info("Getting previous date")
    
    if datetime.today().strftime('%A') == 'Monday':
        return datetime.today() + timedelta(days=-3)
    else:
        return  datetime.today() + timedelta(days=-1)
    
