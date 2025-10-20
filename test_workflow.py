import unittest
from unittest.mock import patch
import sqlite3
from datetime import date, timedelta
import os
import shutil
import pandas as pd
from append import main as append_main
from initial_extraction import main as initial_main

class TestWorkflow(unittest.TestCase):

    def mock_yf_download(self, tickers, start, end):
        # Create a mock DataFrame that mimics the yfinance library for batch downloads
        dates = pd.to_datetime([date.today() - timedelta(days=2), date.today() - timedelta(days=1), date.today()])
        dates.name = 'Date'
        columns = pd.MultiIndex.from_product([['Close'], tickers])
        data = [[i+j for j in range(len(tickers))] for i in range(len(dates))]
        return pd.DataFrame(data, index=dates, columns=columns)

    def setUp(self):
        self.db = 'currency_data.db'
        self.test_db = 'test_currency_data.db'

        with patch('initial_extraction.yf.download', side_effect=self.mock_yf_download):
            initial_main()

        shutil.copyfile(self.db, self.test_db)

    def tearDown(self):
        # Remove the database files after each test
        os.remove(self.db)
        os.remove(self.test_db)

    def test_workflow(self):
        with patch('append.yf.download', side_effect=self.mock_yf_download):
            append_main(self.test_db)

        # Connect to the test database
        conn = sqlite3.connect(self.test_db)
        cursor = conn.cursor()

        # Get the table name
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        table_name = cursor.fetchone()[0]

        # Get the last date from the database
        cursor.execute(f"SELECT MAX(Date) FROM {table_name}")
        last_date_str = cursor.fetchone()[0]
        last_date = date.fromisoformat(last_date_str.split(' ')[0])

        # Get today's date
        today = date.today()

        # Assert that the last date is today's date
        self.assertEqual(last_date, today)

        # Close the connection
        conn.close()

if __name__ == '__main__':
    unittest.main()
