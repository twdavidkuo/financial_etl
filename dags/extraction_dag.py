import os
from airflow.sdk import dag, task
from datetime import datetime, timedelta
import pandas as pd
from edgar import *
import json
import logging
import pendulum
from airflow.models import DagRun


    
def transform_statement(statement):
    import re
    date_pattern = re.compile(r'^\d{4}-\d{2}-\d{2}$')
    date_columns = [col for col in statement.columns if date_pattern.match(str(col))]
    
    # For concept
    statement1 = statement[['concept'] + date_columns]
    melted1 = statement1.melt(
        id_vars=['concept'],
        value_vars=date_columns,
        var_name='date',
        value_name='value'
    )
    pivot1 = melted1.pivot_table(index='date', columns='concept', values='value', aggfunc='first')
    pivot1 = pivot1.dropna(axis=1)
    pivot1 = pivot1.reset_index()  # 'date' becomes a column
    pivot1.columns.name = None     # Remove the concept index name
    pivot1.columns = [str(col) for col in pivot1.columns]  # Flatten columns in case of MultiIndex
    pivot1 = pivot1.reset_index(drop=True)  # Ensure default integer index

    # For label
    statement2 = statement[['label'] + date_columns]
    melted2 = statement2.melt(
        id_vars=['label'],
        value_vars=date_columns,
        var_name='date',
        value_name='value'
    )
    pivot2 = melted2.pivot_table(index='date', columns='label', values='value', aggfunc='first')
    pivot2 = pivot2.dropna(axis=1)
    pivot2 = pivot2.reset_index()  # 'date' becomes a column
    pivot2.columns.name = None     # Remove the label index name
    pivot2.columns = [str(col) for col in pivot2.columns]  # Flatten columns in case of MultiIndex
    pivot2 = pivot2.reset_index(drop=True)  # Ensure default integer index

    return pivot1, pivot2

def extract_financial_statements(ticker, form="10-K", base_dir=f"{os.getcwd()}/data", filing_date="2015-01-01:2025-12-31"):
    """
    This task is used to find the last execution date of the financial data extraction so that we can 
    update the filling date of the next execution.  For a given ticker and form type ("10-K" or "10-Q"), download all filings in the date range,
    parse balance_sheet, income_statement, statement_of_equity,
    transform and save them as CSVs in the correct directory structure.
    """
    # Map form to directory name
    form_dir = form.lower().replace("-", "")  # "10-K" -> "10k", "10-Q" -> "10q"
    company = Company(ticker)
    reports = company.get_filings(filing_date=filing_date, form=[form])
    
    for report in reports:
        xbrl = XBRL.from_filing(report)
        filing_date_str = report.filing_date  # e.g. '2023-09-30'
        
        for statement_name in [
            "balance_sheet",
            "income_statement",
            "statement_of_equity"
        ]:
            try:
                statement = getattr(xbrl.statements, statement_name)()
                df = statement.to_dataframe()
                concept_df, label_df = transform_statement(df)
                
                for kind, out_df in [("concept", concept_df), ("label", label_df)]:
                    out_dir = os.path.join(
                        base_dir, ticker, form_dir, statement_name
                    )
                    os.makedirs(out_dir, exist_ok=True)
                    out_path = os.path.join(
                        out_dir, f"{filing_date_str}_{kind}.csv"
                    )
                    out_df.to_csv(out_path, index=False)
                    print(f"Saved: {out_path}")
            except Exception as e:
                print(f"Skipping {statement_name} for {ticker} {filing_date_str}: {e}")

    

def extract_financial_facts(ticker, form="10-K", base_dir=f"{os.getcwd()}/data", filing_date="2015-01-01:2025-12-31"):
    """
    This task is used to extract the financial facts (basic/diluted EPS, revenue, net income, etc.) from the SEC's website.
    For a given ticker and form type, extract EarningsPerShareBasic, EarningsPerShareDiluted, and Revenue
    from all filings and save as CSVs in the correct directory structure.
    """
    form_dir = form.lower().replace("-", "")  # "10-K" -> "10k", "10-Q" -> "10q"
    company = Company(ticker)
    reports = company.get_filings(filing_date=filing_date, form=[form])

    for report in reports:
        xbrl = XBRL.from_filing(report)
        filing_date_str = report.filing_date  # e.g. '2023-09-30'
        
        for concept, subdir in [
            ("EarningsPerShareBasic", "basic_eps"),
            ("EarningsPerShareDiluted", "diluted_eps"),
            ("Revenue", "revenue")
        ]:
            try:
                df = xbrl.facts.get_facts_by_concept(concept)
                if df is None or df.empty:
                    print(f"No facts found for {concept} in {ticker} {filing_date_str}")
                    continue
                out_dir = os.path.join(
                    base_dir, ticker, form_dir, subdir
                )
                os.makedirs(out_dir, exist_ok=True)
                out_path = os.path.join(
                    out_dir, f"{filing_date_str}.csv"
                )
                df.to_csv(out_path, index=False)
                print(f"Saved: {out_path}")
            except Exception as e:
                print(f"Skipping {concept} for {ticker} {filing_date_str}: {e}")






@task
def get_tickers():
    """
    Get the list of tickers from the S&P 500 companies CSV file.
    """
    companies = pd.read_csv(f"{os.getcwd()}/sp500_companies.csv")
    return companies["Symbol"].tolist()


@task
def get_filing_data(base_dir: str = f"{os.getcwd()}/data"):
    """
    This task is used to get the filing date based on the most recent DAG run.
    It reads the extraction log file to determine the filing date range for extraction.
    """
    import json
    import os
    from datetime import datetime
    
    log_file_path = os.path.join(base_dir, "extraction_log.json")
    current_date = datetime.now().strftime("%Y-%m-%d")
    
    # 1. If extraction_log.json doesn't exist, create it and return default range
    if not os.path.exists(log_file_path):
        log_data = {
            "last_execution_date": current_date,
            "created_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat()
        }
        
        with open(log_file_path, 'w') as f:
            json.dump(log_data, f, indent=2)
        
        print(f"Created extraction log file: {log_file_path}")
        return {"filing_date_range": f"2015-01-01:{current_date}"}
    
    # 2. If extraction_log.json exists, read it and return the filing date range
    with open(log_file_path, "r") as f:
        log_data = json.load(f)
    
    previous_execution_date = log_data["last_execution_date"]
    
    print(f"Read extraction log file: {log_file_path}")
    return {"filing_date_range": f"{previous_execution_date}:{current_date}"}


@task
def extract_financial_data(filing_data_info, tickers):
    """
    This task is used to extract the financial data from the SEC's website.
    After successful extraction, it updates the extraction log with the current execution date.
    """
    import os
    import re
    import json
    from datetime import datetime
    from airflow.models import Variable
    
    identity = Variable.get("sec_identity")
    if not identity:
        raise ValueError("sec_identity environment variable is not set. Please set it with your identity (e.g., 'John Doe johndoe@example.com')")
    
    # Remove angle brackets from email format
    identity = re.sub(r'<(.+?)>', r'\1', identity)
    
    set_identity(identity)
    
    base_dir = f"{os.getcwd()}/data"
    
    try:
        # Extract financial data for all tickers
        for ticker in tickers:
            extract_financial_statements(ticker, form="10-K", base_dir=base_dir, filing_date=filing_data_info["filing_date_range"])
            extract_financial_facts(ticker, form="10-K", base_dir=base_dir, filing_date=filing_data_info["filing_date_range"])
            extract_financial_statements(ticker, form="10-Q", base_dir=base_dir, filing_date=filing_data_info["filing_date_range"])
            extract_financial_facts(ticker, form="10-Q", base_dir=base_dir, filing_date=filing_data_info["filing_date_range"])
        
        # Update extraction log after successful extraction
        log_file_path = os.path.join(base_dir, "extraction_log.json")
        current_date = datetime.now().strftime("%Y-%m-%d")
        
        if os.path.exists(log_file_path):
            with open(log_file_path, "r") as f:
                log_data = json.load(f)
        else:
            log_data = {
                "created_at": datetime.now().isoformat()
            }
        
        # Update the execution date to current date
        log_data["last_execution_date"] = current_date
        log_data["updated_at"] = datetime.now().isoformat()
        
        # Write updated log
        with open(log_file_path, 'w') as f:
            json.dump(log_data, f, indent=2)
        
        print(f"Updated extraction log file after successful extraction: {log_file_path}")
        print(f"Last execution date set to: {current_date}")
        
    except Exception as e:
        print(f"Error during financial data extraction: {e}")
        # Don't update the log file if extraction failed
        raise


@dag(
    dag_id="financial_data_extraction",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["extraction_dag"],
)
def financial_data_extraction():
    tickers = get_tickers()
    filing_data_info = get_filing_data()
    extract_financial_data(filing_data_info, ['AAPL']);
    
financial_data_extraction()