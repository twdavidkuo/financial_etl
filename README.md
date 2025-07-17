# Financial ETL Pipeline

A comprehensive Apache Airflow-based ETL pipeline for extracting financial data from SEC EDGAR filings. This pipeline automatically downloads, processes, and stores financial statements and key metrics for S&P 500 companies.

## Features

- **Automated Data Extraction**: Downloads 10-K and 10-Q filings from SEC EDGAR
- **Financial Statements**: Extracts balance sheets, income statements, and statements of equity
- **Key Metrics**: Captures EPS (basic/diluted), revenue, and other financial facts
- **Incremental Processing**: Tracks last execution date to avoid duplicate extractions
- **S&P 500 Coverage**: Processes all S&P 500 companies
- **Structured Output**: Saves data in organized CSV format with proper directory structure

## Project Structure

```
Financial_ETL/
├── dags/
│   └── extraction_dag.py         # Main Airflow DAG for financial data extraction
├── data/
│   ├── sp500_companies.csv       # S&P 500 company list
│   ├── extraction_log.json       # Execution tracking log
│   └── [TICKER]/                 # Company-specific data
│       ├── 10k/                  # 10-K filings
│       │   ├── balance_sheet/
│       │   ├── income_statement/
│       │   ├── statement_of_equity/
│       │   ├── basic_eps/
│       │   ├── diluted_eps/
│       │   └── revenue/
│       └── 10q/                  # 10-Q filings
│           ├── balance_sheet/
│           ├── income_statement/
│           ├── statement_of_equity/
│           ├── basic_eps/
│           ├── diluted_eps/
│           └── revenue/
├── models/
│   └── staging/                  # Staging area for processed data
├── airflow/                      # Airflow home directory
├── logs/                         # Airflow logs
├── venv/                         # Python virtual environment
├── requirements.txt              # Python dependencies
├── setup_env.py                  # Environment setup script
├── extraction.env                # Environment variables configuration
├── sp500_companies.csv           # S&P 500 companies list
└── README.md                     # This file
```

## Prerequisites

- Python 3.10+
- Apache Airflow 3.0.2+
- Access to SEC EDGAR API

## Installation

1. **Clone the repository**:
   ```bash
   git clone <repository-url>
   cd Financial_ETL
   ```

2. **Create and activate virtual environment**:
   ```bash
   python3.10 -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

4. **Install Airflow**:
   ```bash
   pip install "apache-airflow[celery]==3.0.2" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.0.2/constraints-3.9.txt"
   ```

5. **Set up environment**:
   ```bash
   # Run the environment setup script
   python setup_env.py
   ```
   
   This script will:
   - Load environment variables from `extraction.env`
   - Create the necessary data directory structure
   - Set up ticker-specific directories for all S&P 500 companies

6. **Set up Airflow**:
   ```bash
   airflow db init
   airflow users create \
       --username admin \
       --firstname Admin \
       --lastname User \
       --role Admin \
       --email admin@example.com \
       --password admin
   ```

## Configuration

### Environment Variables

The project uses `extraction.env` to manage all environment variables. This file contains:

```bash
# Airflow Configuration
AIRFLOW_HOME=/Users/kuot/Documents/Financial_ETL/airflow
AIRFLOW__CORE__DAGS_FOLDER=/Users/kuot/Documents/Financial_ETL/dags
AIRFLOW__EMAIL__FROM_EMAIL=David Kuo <davidkuotwk@gmail.com>

# Data Directory Configuration
OUTPUT_DATA_DIR=./data
```

**Important**: 
1. Update the `AIRFLOW_HOME` and `AIRFLOW__CORE__DAGS_FOLDER` paths in `extraction.env` to match your system
2. Update the `AIRFLOW__EMAIL__FROM_EMAIL` with your identity for SEC EDGAR API access
3. Set the `sec_identity` Airflow Variable with your identity (e.g., 'John Doe johndoe@example.com')

### Airflow Variables

Set the required Airflow variable for SEC API access:

```bash
airflow variables set sec_identity "Your Name your.email@example.com"
```

### Environment Setup

The `setup_env.py` script automatically:
- Loads environment variables from `extraction.env`
- Creates the data directory structure
- Sets up ticker-specific directories for all S&P 500 companies
- Provides fallback values if environment variables are not set

### Airflow Configuration

1. **Start Airflow webserver**:
   ```bash
   airflow webserver --port 8080
   ```

2. **Start Airflow scheduler**:
   ```bash
   airflow scheduler
   ```

## Usage

### Running the Pipeline

1. **Set up the environment** (if not already done):
   ```bash
   python setup_env.py
   ```

2. **Start Airflow services** (if not already running):
   ```bash
   airflow webserver --port 8080 &
   airflow scheduler &
   ```

3. **Access Airflow UI**: Open http://localhost:8080 in your browser

4. **Enable the DAG**: In the Airflow UI, find `financial_data_extraction` and toggle it on

5. **Trigger manual run** (optional): Click "Trigger DAG" to run immediately

### Pipeline Components

The pipeline consists of several tasks:

1. **`get_tickers`**: Reads S&P 500 company list from CSV
2. **`get_filing_data`**: Manages extraction log and determines filing date range
3. **`extract_financial_data`**: Downloads and processes financial data

### Data Extraction

The pipeline extracts the following data for each company:

#### Financial Statements
- **Balance Sheet**: Assets, liabilities, and equity
- **Income Statement**: Revenue, expenses, and net income
- **Statement of Equity**: Changes in shareholders' equity

#### Key Metrics
- **Basic EPS**: Earnings per share (basic)
- **Diluted EPS**: Earnings per share (diluted)
- **Revenue**: Total revenue

#### File Formats
- **Concept-based**: Raw financial concepts and values
- **Label-based**: Human-readable financial labels and values

## Data Output

### Directory Structure
```
data/
├── AAPL/
│   ├── 10k/
│   │   ├── balance_sheet/
│   │   │   ├── 2023-09-30_concept.csv
│   │   │   └── 2023-09-30_label.csv
│   │   ├── income_statement/
│   │   ├── statement_of_equity/
│   │   ├── basic_eps/
│   │   │   └── 2023-09-30.csv
│   │   ├── diluted_eps/
│   │   └── revenue/
│   └── 10q/
│       └── [similar structure]
```

### File Formats

#### Statement Files
- **Concept files**: Raw XBRL concepts with date columns
- **Label files**: Human-readable labels with date columns

#### Fact Files
- **Single CSV per filing date**: Contains concept, value, and metadata

## Dependencies

### Core Dependencies
- `edgartools`: SEC EDGAR data extraction library
- `pandas>=1.5.0`: Data manipulation and analysis
- `requests>=2.28.0`: HTTP library for API calls
- `python-dotenv>=0.19.0`: Environment variable management

### Airflow Dependencies
- `apache-airflow[celery]==3.0.2`: Workflow orchestration platform

## Monitoring and Logging

### Extraction Log
The pipeline maintains `data/extraction_log.json` to track:
- Last execution date
- Creation and update timestamps
- Execution history

### Airflow Logs
- Task-specific logs available in Airflow UI
- Error handling and retry mechanisms
- Execution status tracking

## Error Handling

The pipeline includes comprehensive error handling:

- **Missing Environment Variables**: Clear error messages for configuration issues
- **API Failures**: Graceful handling of SEC API timeouts and errors
- **Data Processing**: Skips problematic filings while continuing with others
- **File System**: Handles missing directories and file permission issues

## Current Configuration

### DAG Configuration
- **DAG ID**: `financial_data_extraction`
- **Schedule**: `@daily` (runs every day)
- **Start Date**: January 1, 2025
- **Catchup**: Disabled
- **Tags**: `["extraction_dag"]`

### Current Test Configuration
The DAG is currently configured to process only AAPL for testing purposes. To process all S&P 500 companies, update the last line in `extraction_dag.py`:

```python
# Current (test mode):
extract_financial_data(filing_data_info, ['AAPL']);

# For full S&P 500 processing:
extract_financial_data(filing_data_info, tickers);
```

## Customization

### Adding New Companies
1. Update `data/sp500_companies.csv` with new ticker symbols
2. The pipeline will automatically process new companies

### Adding New Financial Metrics
1. Modify `extract_financial_facts()` function
2. Add new concepts to the extraction list
3. Update directory structure as needed

### Modifying Extraction Date Range
The pipeline uses incremental processing based on the last execution date. To modify the date range:
1. Edit the `extraction_log.json` file
2. Update the `last_execution_date` field
3. Or delete the file to start fresh from 2015-01-01 