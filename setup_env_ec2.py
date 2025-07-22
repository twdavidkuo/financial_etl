#!/usr/bin/env python3
"""
Financial ETL Environment Setup Script

This script sets up the environment for the Financial ETL pipeline by:
1. Loading environment variables from extraction.env file
2. Creating the data directory structure
3. Reading S&P 500 companies from CSV
4. Creating ticker-specific directories
"""

import os
import sys
import pandas as pd
from dotenv import load_dotenv


def setup_environment():
    """Set up the Financial ETL environment by loading .env file."""
    print("Setting up Financial ETL environment...")
    
    # Load environment variables from extraction.env file
    load_dotenv('extraction.env')
    
    # Get the current working directory
    current_dir = os.getcwd()
    
    # Set environment variables with fallbacks to .env values
    os.environ['AIRFLOW_HOME'] = os.getenv('AIRFLOW_HOME', os.path.join(current_dir, 'airflow'))
    os.environ['AIRFLOW__CORE__DAGS_FOLDER'] = os.getenv('AIRFLOW__CORE__DAGS_FOLDER', os.path.join(current_dir, 'dags'))
    os.environ['AIRFLOW__EMAIL__FROM_EMAIL'] = os.getenv('AIRFLOW__EMAIL__FROM_EMAIL', "David Kuo <davidkuotwk@gmail.com>")
    
    print("Environment variables loaded from extraction.env file:")
    print(f"AIRFLOW__CORE__DAGS_FOLDER={os.environ['AIRFLOW__CORE__DAGS_FOLDER']}")
    print(f"OUTPUT_DATA_DIR={os.environ['OUTPUT_DATA_DIR']}")
    print(f"AIRFLOW__EMAIL__FROM_EMAIL={os.environ['AIRFLOW__EMAIL__FROM_EMAIL']}")

)


def get_tickers(csv_path):
    """Read S&P 500 companies from CSV file."""
    print("Reading S&P 500 companies from CSV...")
    
    if not os.path.exists(csv_path):
        print(f"Error: {csv_path} not found in {os.path.dirname(csv_path)}")
        sys.exit(1)
    
    try:
        companies = pd.read_csv(csv_path)
        tickers = companies['Symbol'].tolist()
        return tickers
    except Exception as e:
        print(f"Error reading CSV: {e}")
        sys.exit(1)


def create_s3_prefix_structure_boto3(tickers, bucket_name):
    s3 = boto3.client("s3")
    forms = ["10k", "10q"]
    subdirs = ["balance_sheet", "income_statement", "statement_of_equity", "revenue", "basic_eps", "diluted_eps"]

    for ticker in tickers:
        for form in forms:
            for subdir in subdirs:
                key = f"{ticker}/{form}/{subdir}/.keep"
                # Upload an empty object to simulate a folder
                s3.put_object(Bucket=bucket_name, Key=key, Body=b'')
                print(f"Created: s3://{bucket_name}/{key}")

def main():
    """Main function to orchestrate the setup process."""
    try:
        # Setup environment variables from extraction.env file
        setup_environment()
        
        # Get the current working directory
        current_dir = os.getcwd()
        output_data_dir = os.environ['OUTPUT_DATA_DIR']
        
        # Create main data directory
        create_data_directory(output_data_dir)
        
        # Read tickers from CSV
        csv_path = os.path.join(current_dir, 'sp500_companies.csv')
        tickers = get_tickers(csv_path)
        
        # Create ticker directories
        total_tickers = create_ticker_directories(tickers, output_data_dir)
        
        print("Setup completed successfully!")
        print(f"Total tickers processed: {total_tickers}")
        
    except Exception as e:
        print(f"Error during setup: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main() 


