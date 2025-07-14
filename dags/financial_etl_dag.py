import os
from airflow.sdk import dag, task
from datetime import datetime
import pandas as pd
from edgar import *
import json
import logging
import pendulum



@task 
def find_tickers():
    """
    This task is used to find the tickers of the companies that have filed financial statements with the SEC.
    """
    companies = pd.read_csv("data/sp500_companies.csv")
    return companies["Symbol"].tolist()

    
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

def extract_financial_statements(ticker, form="10-K", base_dir="data", filing_date="2015-01-01:2025-12-31"):
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

    

def extract_financial_facts(ticker, form="10-K", base_dir="data", filing_date="2015-01-01:2025-12-31"):
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

@dag(
    dag_id="financial_data_extraction",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["data_extraction"],
)
def financial_data_extraction():
    tickers = find_tickers()

financial_data_extraction()