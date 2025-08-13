from prefect import flow, task
from datetime import datetime
from prefect_gcp import GcpCredentials
import pandas as pd
import json
import requests

# Tasks for Daily Ingestion
@task
def fetch_btc_daily():
    url = "https://api.coingecko.com/api/v3/simple/price"
    params = {
        "ids": "bitcoin",
        "vs_currencies": "php"
    }
    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()
    price = data["bitcoin"]["usd"]

    return price

@task
def create_df_daily(price: float):
    date = datetime.now().isoformat()
    data = {
        "date":[date],
        "btc_php":[price],
        "load_date":[date]
    }

    return pd.DataFrame(data)

@task
def load_to_bq_daily(df: pd.DataFrame):
    gcp_credentials_block = GcpCredentials.load("gcp-bgq-creds")
    df.to_gbq(
        destination_table = 'crypto_coins.btc_price',
        project_id = 'crypto-ingestion',
        credentials = gcp_credentials_block.get_credentials_from_service_account(),
        if_exists = 'append'
    )

# Tasks for Historical (1Y) Ingestion
@task
def fetch_btc_historical():
    # CoinGecko API endpoint for 1 year of BTC daily price
    url = "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart"
    params = {
        "vs_currency": "php",  # price in PHP
        "days": "365",          # last 365 days
        "interval": "daily"     # daily resolution
    }
    response = requests.get(url, params=params)
    data = response.json()

    return data

@task
def create_df_historical(data: dict):
    df = pd.DataFrame(data["prices"], columns=["date", "btc_php"])
    df["date"] = pd.to_datetime(df["date"], unit="ms").dt.date
    df["load_date"] = pd.to_datetime("today").normalize()
    df = df[["date", "btc_php", "load_date"]]

    return df

@task
def load_to_bq_historical(df: pd.DataFrame):
    gcp_credentials_block = GcpCredentials.load("gcp-bgq-creds")
    df.to_gbq(
        destination_table = 'crypto_coins.btc_price',
        project_id = 'crypto-ingestion',
        credentials = gcp_credentials_block.get_credentials_from_service_account(),
        if_exists = 'replace'
    )

@flow(name="BTC Price Ingestion Daily")
def ingestion_flow_daily():
    price = fetch_btc_daily()
    df = create_df_daily(price)
    load_to_bq_daily(df)

@flow(name="BTC Price Ingestion Historical")
def ingestion_flow_historical():
    data = fetch_btc_historical()
    df = create_df_historical(data)
    load_to_bq_historical(df)

if __name__ == "__main__":
    ingestion_flow_daily()
    ingestion_flow_historical()
