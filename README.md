# Currency Data Automation

This repository contains scripts to automatically update currency exchange rate data in an SQLite database.

## Features

- Fetches the latest currency exchange rates using Yahoo Finance API
- Stores data in an SQLite database
- Fills in missing data using forward and backward fill methods
- Comprehensive logging for debugging and monitoring
- Automated daily updates via GitHub Actions

## Files

- `append.py`: Main script that updates the currency database with the latest data
- `currency_test.db`: SQLite database containing currency exchange rate data
- `requirements.txt`: List of required Python packages
- `.github/workflows/currency_update.yml`: GitHub Actions workflow for automation

## Setup

1. Clone this repository:
   ```
   git clone https://github.com/yourusername/currency.git
   cd currency
   ```

2. Install the required packages:
   ```
   pip install -r requirements.txt
   ```

3. Run the script manually:
   ```
   python append.py
   ```

## Automated Updates

This repository is configured with GitHub Actions to automatically run the `append.py` script every 24 hours. The workflow:

1. Runs at midnight UTC every day
2. Installs all required dependencies
3. Executes the append.py script to update the database
4. Commits and pushes any changes to the repository

You can also trigger the workflow manually from the "Actions" tab in the GitHub repository.

## Logs

Logs are stored in the `logs/` directory with timestamps in the filename. Each log contains detailed information about:

- Database connections
- Data retrieval operations
- Data processing steps
- Error messages (if any)

## Customization

To modify the update schedule, edit the cron expression in `.github/workflows/currency_update.yml`. For example:

- `0 0 * * *`: Every day at midnight UTC (default)
- `0 */12 * * *`: Every 12 hours
- `0 0 * * 1-5`: Every weekday at midnight UTC

## Requirements

- Python 3.10 or higher
- Packages listed in requirements.txt 