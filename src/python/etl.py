
import os
import argparse
import pandas as pd
import json
import requests
import pyodbc
from datetime import datetime
from .config import Settings
from .utils import ensure_dirs, valid_email, setup_logger
from .reporting import export_reports

def get_sql_connection():
    if Settings.TRUSTED_CONNECTION:
        conn_str = f"Driver={{{Settings.SQL_DRIVER}}};Server={{{Settings.SQL_SERVER}}};Database={{{Settings.SQL_DATABASE}}};Trusted_Connection=yes;"
    else:
        conn_str = f"Driver={{{Settings.SQL_DRIVER}}};Server={{{Settings.SQL_SERVER}}};Database={{{Settings.SQL_DATABASE}}};UID={{{Settings.SQL_USERNAME}}};PWD={{{Settings.SQL_PASSWORD}}};"
    return pyodbc.connect(conn_str)

def extract_from_csv():
    customers_path = os.path.join(Settings.INPUT_DIR, "customers.csv")
    transactions_path = os.path.join(Settings.INPUT_DIR, "transactions.csv")
    df_customers = pd.read_csv(customers_path)
    df_transactions = pd.read_csv(transactions_path)
    return df_customers, df_transactions

def extract_from_api_or_mock():
    if Settings.API_URL:
        resp = requests.get(Settings.API_URL, timeout=Settings.API_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
    else:
        mock_path = os.path.join(Settings.DATA_DIR, "mock_api_data.json")
        with open(mock_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    return pd.DataFrame(data)

def clean_and_join(df_customers, df_transactions, df_api):
    # Lowercase emails, drop dups in customers
    df_customers['Email'] = df_customers['Email'].str.lower().str.strip()
    df_customers = df_customers.drop_duplicates(subset=['CustomerId'])

    # Validate emails
    df_customers['EmailValid'] = df_customers['Email'].apply(valid_email)

    # Fix amounts and dates
    df_transactions['Amount'] = pd.to_numeric(df_transactions['Amount'], errors='coerce').fillna(0)
    df_transactions['CreatedAt'] = pd.to_datetime(df_transactions['CreatedAt'], errors='coerce')

    # Drop invalid rows
    df_transactions = df_transactions.dropna(subset=['CreatedAt'])
    df_transactions = df_transactions[df_transactions['Amount'] >= 0]

    # API enrichment
    df_api['Email'] = df_api['Email'].str.lower().str.strip()
    df_api = df_api.drop_duplicates(subset=['CustomerId'])

    # Prefer customer email; if invalid, use API email if valid
    df = df_transactions.merge(df_customers[['CustomerId','Name','Email','EmailValid']], on='CustomerId', how='left')
    df = df.merge(df_api[['CustomerId','Email']].rename(columns={'Email':'ApiEmail'}), on='CustomerId', how='left')

    df['FinalEmail'] = df.apply(lambda r: r['Email'] if r.get('EmailValid') else r.get('ApiEmail'), axis=1)
    df['FinalEmail'] = df['FinalEmail'].where(df['FinalEmail'].apply(valid_email), None)

    df_clean = df[['CustomerId','Name','FinalEmail','Amount','CreatedAt']].rename(columns={'FinalEmail':'Email'})
    df_clean = df_clean.dropna(subset=['CustomerId','Name','Amount','CreatedAt'])
    df_clean = df_clean.drop_duplicates(subset=['CustomerId','Amount','CreatedAt'])
    return df_clean

def load_to_sql(df_clean, logger):
    with get_sql_connection() as conn:
        cursor = conn.cursor()
        # Insert rows
        for _, r in df_clean.iterrows():
            cursor.execute(
                "INSERT INTO dbo.CleanData (CustomerId, Name, Email, Amount, CreatedAt, Source) VALUES (?, ?, ?, ?, ?, ?)",
                r['CustomerId'], r['Name'], r['Email'], float(r['Amount']), r['CreatedAt'].to_pydatetime(), "ETL"
            )
        conn.commit()
    logger.info("Loaded %d rows into dbo.CleanData", len(df_clean))

def main():
    parser = argparse.ArgumentParser(description="Business Process Automation ETL")
    parser.add_argument("--no-sql", action="store_true", help="Skip loading to SQL Server (for demo)")
    args = parser.parse_args()

    ensure_dirs(Settings.REPORTS_DIR, Settings.LOGS_DIR)
    log_path = os.path.join(Settings.LOGS_DIR, "automation.log")
    logger = setup_logger(log_path)
    logger.info("ETL started")

    df_customers, df_transactions = extract_from_csv()
    df_api = extract_from_api_or_mock()

    df_clean = clean_and_join(df_customers, df_transactions, df_api)
    csv_path, xlsx_path, summary_path = export_reports(df_clean)

    if not args.no_sql:
        try:
            load_to_sql(df_clean, logger)
        except Exception as e:
            logger.exception("Failed to load to SQL Server: %s", e)

    logger.info("ETL finished. Reports: %s | %s | %s", csv_path, xlsx_path, summary_path)
    print("Reports generated:", csv_path, xlsx_path, summary_path)

if __name__ == "__main__":
    main()
