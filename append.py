import pandas as pd
import numpy as np
import yfinance as yf 
import sqlite3 
from datetime import datetime, timedelta, date
import ast
import logging
import os
import sys

# Configure logging
log_dir = 'logs'
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

log_file = os.path.join(log_dir, f'currency_append_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')

# Set up logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger('currency_append')

def get_last_date_from_db():
    logger.info("Starting to retrieve last date from database")
    try:
        # Connect to the test database
        conn = sqlite3.connect('currency_data.db')
        logger.info("Connected to currency_data.db")
        
        # Get the table name
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        table_name = cursor.fetchone()[0]
        logger.info(f"Found table: {table_name}")
        
        # Read the entire table into a DataFrame
        df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
        logger.info(f"Read {len(df)} rows from database")
        
        df['Date'] = pd.to_datetime(df['Date'])
        df.set_index('Date', inplace=True)
        
        # Get the last date
        last_date = df.index.max()
        logger.info(f"Last date in database: {last_date}")
        
        # Get unique currency codes from the columns
        currency_codes = set()
        for col in df.columns:
            if 'Close' in str(col):
                try:
                    # Extract currency code from column name
                    code = str(col).split('=')[0].strip("('Close', '")
                    if len(code) == 3:  # Standard currency code length
                        currency_codes.add(code)
                except Exception as e:
                    logger.warning(f"Error extracting currency code from column {col}: {str(e)}")
                    continue
        
        logger.info(f"Found {len(currency_codes)} currency codes")
        conn.close()
        return last_date, list(currency_codes)
    except Exception as e:
        logger.error(f"Error in get_last_date_from_db: {str(e)}", exc_info=True)
        raise

def get_missing_data(last_date, currency_codes):
    logger.info("Starting to retrieve missing data")
    try:
        # Calculate date range
        today = datetime.now()
        if pd.to_datetime(last_date).date() >= today.date():
            logger.info("Database is up to date, no new data needed")
            return None
        
        # Create list of currency pairs
        currency_pairs = [f"{code}=X" for code in currency_codes]
        logger.info(f"Will process {len(currency_pairs)} currency pairs")
        
        # Download data for missing dates
        start_date = pd.to_datetime(last_date) + timedelta(days=1)
        logger.info(f"Fetching data from {start_date} to {today}")
        all_currency_data = pd.DataFrame()
        
        batch_size = 10
        for i in range(0, len(currency_pairs), batch_size):
            batch = currency_pairs[i:i+batch_size]
            logger.info(f'Processing batch {i//batch_size + 1}/{(len(currency_pairs)-1)//batch_size + 1}: {batch}')
            
            for currency in batch:
                logger.info(f'Processing {currency}')
                try:
                    df_ticker = yf.download(currency, start=start_date, end=today)
                    if not df_ticker.empty:
                        logger.info(f"Downloaded {len(df_ticker)} rows for {currency}")
                        df_ticker = pd.DataFrame(df_ticker['Close'])
                        df_ticker.columns = [f"('Close', '{currency}')"]
                        if all_currency_data.empty:
                            all_currency_data = df_ticker
                        else:
                            all_currency_data = all_currency_data.join(df_ticker, how='outer')
                    else:
                        logger.warning(f"No data available for {currency}")
                except Exception as e:
                    logger.error(f"Error downloading data for {currency}: {str(e)}")
                    continue
        
        if all_currency_data.empty:
            logger.warning("No new data was retrieved")
            return None
            
        logger.info(f"Retrieved {len(all_currency_data)} new rows of data")
        return all_currency_data
    except Exception as e:
        logger.error(f"Error in get_missing_data: {str(e)}", exc_info=True)
        raise

def fix_currency_data(all_currency_data):
    logger.info("Starting to fix currency data")
    try:
        if all_currency_data is None or all_currency_data.empty:
            logger.info("No data to fix")
            return None
            
        # Count NaN values before filling
        nan_count_before = all_currency_data.isna().sum().sum()
        logger.info(f"Number of NaN values before filling: {nan_count_before}")
        
        # Fill missing values using ffill() and bfill() instead of deprecated fillna(method='ffill')
        all_currency_data = all_currency_data.ffill()
        all_currency_data = all_currency_data.bfill()
        
        # Count NaN values after filling
        nan_count_after = all_currency_data.isna().sum().sum()
        logger.info(f"Number of NaN values after filling: {nan_count_after}")
        
        all_currency_data.index = pd.to_datetime(all_currency_data.index)
        logger.info("Data fixing completed successfully")
        return all_currency_data
    except Exception as e:
        logger.error(f"Error in fix_currency_data: {str(e)}", exc_info=True)
        raise

def update_database(new_data):
    logger.info("Starting database update")
    try:
        if new_data is None:
            logger.info("No new data to add to database")
            return
            
        # Connect to the database
        conn = sqlite3.connect('currency_data.db')
        logger.info("Connected to currency_data.db for update")
        
        # Get the table name
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        table_name = cursor.fetchone()[0]
        logger.info(f"Will update table: {table_name}")
        
        # Reset index to make Date a column
        new_data = new_data.reset_index()
        new_data = new_data.rename(columns={'index': 'Date'})
        
        # Append new data
        logger.info(f"Adding {len(new_data)} new rows to the database")
        new_data.to_sql(table_name, conn, if_exists='append', index=False)
        
        logger.info(f"Successfully added {len(new_data)} new rows to the database")
        conn.close()
    except Exception as e:
        logger.error(f"Error in update_database: {str(e)}", exc_info=True)
        raise

def main():
    logger.info("=== Starting currency data update process ===")
    try:
        # Get the last date from the database and currency codes
        last_date, currency_codes = get_last_date_from_db()
        logger.info(f"Last date in database: {last_date}")
        logger.info(f"Found {len(currency_codes)} currency codes")
        
        # Get missing data
        new_data = get_missing_data(last_date, currency_codes)
        
        # Fix the data (handle missing values)
        new_data = fix_currency_data(new_data)
        
        # Update the database
        update_database(new_data)
        
        logger.info("=== Currency data update process completed successfully ===")
    except Exception as e:
        logger.error(f"Error in main process: {str(e)}", exc_info=True)
        logger.error("=== Currency data update process failed ===")
        raise

if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        logger.critical(f"Unhandled exception: {str(e)}", exc_info=True)
        sys.exit(1)
