import yfinance as yf
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, timedelta, timezone
import os

# Configuration
PROJECT_ID = "portfolio-risk-engine"
DATASET_ID = "bronze"
TABLE_ID = "raw_prices"
CREDENTIALS_PATH = "credentials/portfolio-risk-engine-65c639cdfbdf.json"

TICKERS = ["AAPL", "NVDA", "SPY", "BTC-USD", "SOL-USD", "GC=F"]
START_DATE = "2020-01-01"
END_DATE = datetime.today().strftime("%Y-%m-%d")

def get_bigquery_client():
    """Authenticates with GCP and returns a BigQuery client."""
    credentials = service_account.Credentials.from_service_account_file(
        CREDENTIALS_PATH,
        scopes = ["https://www.googleapis.com/auth/cloud-platform"]
    )
    client = bigquery.Client(
        credentials=credentials,
        project=PROJECT_ID
    )
    return client

def fetch_prices(tickers, start_date, end_date):
    """Fetches historical daily closing prices from Yahoo Finance."""
    all_data = []

    for ticker in tickers:
        print(f"Fetching data for {ticker}...")
        df = yf.download(ticker, start=start_date, end=end_date, auto_adjust=True)

        if df.empty:
            print(f"Notice: No data found for {ticker}. Skipping.")
            continue
        df = df[["Close"]].copy()
        df.columns = ["close_price"]
        df["ticker"] = ticker
        df["date"] = df.index
        df = df.reset_index(drop=True)
        all_data.append(df)

    combined = pd.concat(all_data, ignore_index=True)
    return combined

def prepare_dataframe(df):
    """prepares the dataframe for BigQuery ingestion."""
    df["date"] = pd.to_datetime(df["date"])
    df["close_price"] = df["close_price"].astype(float)
    df["ticker"] = df["ticker"].astype(str)
    df["ingestion_timestamp"] = datetime.now(timezone.utc)

    df = df.dropna(subset=["close_price"])
    df = df[["date", "ticker", "close_price", "ingestion_timestamp"]]
    return df

def load_to_bigquery(df, client):
    """Loads the prepared dataframe into the bronze layer in BigQuery."""
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema=[
            bigquery.SchemaField("date", "DATE"),
            bigquery.SchemaField("ticker", "STRING"),
            bigquery.SchemaField("close_price", "FLOAT"),
            bigquery.SchemaField("ingestion_timestamp", "TIMESTAMP"),
        ]
    )
    
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()

    print(f"Successfully loaded {len(df)} rows into {table_ref}.")

def main():
    print("Starting pipeline...")
    client = get_bigquery_client()
    raw_df = fetch_prices(TICKERS, START_DATE, END_DATE)
    prepared_df = prepare_dataframe(raw_df)
    load_to_bigquery(prepared_df, client)
    print("Pipeline complete.")

if __name__ == "__main__":
    main()