#!/usr/bin/env python3
"""
Financial ETL Directory Creation Script

This script creates the directory structure for the Financial ETL pipeline by:
1. Reading S&P 500 companies from CSV
2. Creating ticker-specific directories for different form types
"""

import os
import sys
import pandas as pd


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


def create_ticker_directories(tickers, output_data_dir):
    """Create directory structure for each ticker."""
    print("Creating directory structure for all tickers...")
    
    # Directory structure for each form type
    forms = ["10k", "10q"]
    subdirs = ["balance_sheet", "income_statement", "statement_of_equity", "revenue", "basic_eps", "diluted_eps"]
    
    for ticker in tickers:
        print(f"Processing ticker: {ticker}")
        
        # Create directories for each form type
        for form in forms:
            for subdir in subdirs:
                path = os.path.join(output_data_dir, ticker, form, subdir)
                if not os.path.exists(path):
                    os.makedirs(path)
    
    return len(tickers)


def main():
    """Main function to orchestrate the directory creation process."""
    try:
        # Get the current working directory
        current_dir = os.getcwd()
        
        # Get output data directory from environment variable or use default
        output_data_dir = os.getenv('OUTPUT_DATA_DIR', os.path.join(current_dir, 'data'))
        
        # Create main data directory if it doesn't exist
        if not os.path.exists(output_data_dir):
            print(f"Creating data directory: {output_data_dir}")
            os.makedirs(output_data_dir)
        else:
            print(f"Data directory already exists: {output_data_dir}")
        
        # Read tickers from CSV
        csv_path = os.path.join(current_dir, 'sp500_companies.csv')
        tickers = get_tickers(csv_path)
        
        # Create ticker directories
        total_tickers = create_ticker_directories(tickers, output_data_dir)
        
        print("Directory creation completed successfully!")
        print(f"Total tickers processed: {total_tickers}")
        
    except Exception as e:
        print(f"Error during directory creation: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main() 