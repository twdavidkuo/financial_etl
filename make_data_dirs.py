import os
import pandas as pd

def make_data_dirs(tickers, base_dir: str = "data"):
    """
    Create directory structure for each company ticker:
    data/{ticker}/10k/balance_sheet
    data/{ticker}/10k/income_statement
    data/{ticker}/10k/cash_flow_statement
    ... (same for 10q and 8k)
    """
    subfolders = ["10k", "10q"]
    target_values = ["balance_sheet", "income_statement", "statement_of_equity",
                     "revenue", "basic_eps", "diluted_eps"]
    for ticker in tickers:
        ticker_dir = os.path.join(base_dir, ticker)
        for subfolder in subfolders:
            for val in target_values:
                path = os.path.join(ticker_dir, subfolder, val)
                os.makedirs(path, exist_ok=True)
                print(f"Created: {path}")

if __name__ == "__main__":
    # Read tickers from sp500_companies.csv
    tickers = pd.read_csv("sp500_companies.csv")['Symbol'].tolist()
    make_data_dirs(tickers) 


