import pandas as pd
import numpy as np
import yfinance as yf 
import sqlite3 
from datetime import datetime, timedelta, date

# importing the currency json path file 
currency_json_path = 'currency.json'

#################### STEPS #########################
# We will first extract the currency data using the initial json file
# whatever currency has no data will be ignored
# we will try and implement logging for this operation to make it streamlined and for debugging purposes
# after that is done we can work with all the iterations we have for our use case
#####################################################

# getting the json metadata
def get_currency_metadata(currency_json_path):
  df_currency_list = pd.read_json(currency_json_path)
  df_currency_list = df_currency_list.transpose()
  df_currency_list['yf_index'] = df_currency_list.index
  return df_currency_list

# getting the information from yfinance for all data
def extract_currency_data(df_currency_list,batch_size,start_date,end_date):
  all_currency_data = pd.DataFrame()
  ticker_list = df_currency_list['yf_index'].tolist()
  for i in range(0,len(ticker_list),batch_size):
    batch = ticker_list[i:i+batch_size]
    print(f'processing batch {batch}')

    for currency in batch:
      ticker = f'{currency}=X'
      print(f'processing {ticker}')
      df_ticker = yf.download(ticker,start=start_date,end=end_date)
      if not df_ticker.empty:
        #df_ticker = df_ticker[ f'Close',ticker]
        df_ticker = pd.DataFrame(df_ticker)
        if all_currency_data.empty:
          all_currency_data = df_ticker
        else:
          all_currency_data = all_currency_data.join(df_ticker, how='outer')

  return all_currency_data


# conducting the fixes required for the currency database
def fix_currency_data(all_currency_data):
  # filling in the empty values
  all_currency_data = all_currency_data.fillna(method='ffill')
  all_currency_data = all_currency_data.fillna(method='bfill')
  # making the currency data stick
  all_currency_data.index = pd.to_datetime(all_currency_data.index)
  return all_currency_data

def push_to_sql(all_currency_data,currency_db_name,currency_table_name):
  # saving the data as a sqlite table
  conn = sqlite3.connect('currency_data.db')
  all_currency_data.to_sql('currency_data_historical', conn, if_exists='replace', index=True)

  print('the table in the database has been created/appended')
  conn.close()


def main():
  currency_db_name = 'currency_data.db'
  currency_table_name = 'currency_data_historical'
  currency_json_path = 'currency.json'
  df_currency_list = get_currency_metadata(currency_json_path)
  batch_size = 10
  start_date = (datetime.now() - timedelta(days=365*3)).strftime('%Y-%m-%d')
  end_date = datetime.now()
  all_currency_data = extract_currency_data(df_currency_list,batch_size,start_date,end_date)
  all_currency_data = fix_currency_data(all_currency_data)
  push_to_sql(all_currency_data,currency_db_name,currency_table_name)
  return all_currency_data

if __name__ == '__main__':
  df_final = main()
  print(df_final)