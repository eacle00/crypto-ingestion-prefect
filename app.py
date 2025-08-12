from prefect import flow, task
from datetime import datetime
from prefect_gcp import GcpCredentials
import pandas as pd
import json
import requests


@task
def fetch_btc():
    url = "https://api.coingecko.com/api/v3/simple/price"
    params = {
        "ids": "bitcoin",
        "vs_currencies": "usd"
    }
    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()
    price = data["bitcoin"]["usd"]

    return price

@task
def create_dataframe(price: float):
    timestamp = datetime.now().isoformat()
    data = {
        "Timestamp":[timestamp],
        "BTC_USD":[price]
    }

    return pd.DataFrame(data)

@task
def load_to_bq(df: pd.DataFrame):
    gcp_credentials_block = GcpCredentials.load("gcp-bgq-creds")
    df.to_gbq(
        destination_table = 'crypto_coins.btc_prices',
        project_id = 'crypto-ingestion',
        credentials = gcp_credentials_block.get_credentials_from_service_account(),
        if_exists = 'append'
    )
    

@flow(log_prints=True)
def ingestion_flow():
    price = fetch_btc()
    df = create_dataframe(price)
    load_to_bq(df)


if __name__ == "__main__":
    ingestion_flow()
