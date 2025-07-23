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
import boto3
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime
from airflow.sdk import dag, task
from airflow.models import Variable

@task
def setup_environment():
    """Set up the Financial ETL environment by setting Airflow Variables."""
    print("Setting up Financial ETL environment...")
    
    # Load environment variables from extraction.env file
    load_dotenv('extraction.env')
    
    # Set sec_identity as Airflow Variable
    sec_identity = os.getenv('sec_identity', "David Kuo <davidkuotwk@gmail.com>")
    
    try:
        # Set the Airflow Variable
        Variable.set("sec_identity", sec_identity)
        print(f"Successfully set Airflow Variable 'sec_identity': {sec_identity}")
    except Exception as e:
        print(f"Error setting Airflow Variable: {e}")
        # If Variable.set fails, try to update existing variable
        try:
            existing_var = Variable.get("sec_identity")
            if existing_var != sec_identity:
                Variable.set("sec_identity", sec_identity)
                print(f"Updated existing Airflow Variable 'sec_identity': {sec_identity}")
            else:
                print(f"Airflow Variable 'sec_identity' already set to: {sec_identity}")
        except Exception as update_error:
            print(f"Error updating Airflow Variable: {update_error}")
    
    print("Environment setup completed.")

@task
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

@task
def create_s3_dirs(tickers, bucket_name):
    s3 = boto3.client("s3")
    forms = ["10k", "10q"]
    subdirs = ["balance_sheet", "income_statement", "statement_of_equity", "revenue", "basic_eps", "diluted_eps"]

    for ticker in tickers:
        for form in forms:
            for subdir in subdirs:
                key = f"{ticker}/{form}/{subdir}/.keep"
                
                # Check if object exists
                response = s3.list_objects_v2(Bucket=bucket_name, Prefix=key)
                if 'Contents' in response:
                    print(f"Exists: s3://{bucket_name}/{key}")
                else:
                    s3.put_object(Bucket=bucket_name, Key=key, Body=b'')
                    print(f"Created: s3://{bucket_name}/{key}")

@dag(
    dag_id="setup_env_ec2",
    schedule=None,
    start_date=datetime.now(),
    catchup=False,
    tags=["setup_env_ec2"],
)
def setup_env_ec2():
    """Main function to orchestrate the setup process."""
    try:
        # Setup environment variables from extraction.env file
        setup_environment()
        
        # Get the current working directory
        current_dir = os.getcwd()
  
        # Read tickers from CSV
        csv_path = os.path.join(current_dir, 'sp500_companies.csv')
        tickers = get_tickers(csv_path)
        
        # Create ticker directories
        #create_s3_dirs(tickers, 'sec-etl-raw-data')
        
        print("Setup completed successfully!")
        
    except Exception as e:
        print(f"Error during setup: {e}")
        sys.exit(1)


setup_env_ec2()