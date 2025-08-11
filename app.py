from prefect import flow, task
from datetime import datetime
from prefect_gcp import GcpCredentials
from pandas_gbq import to_gbq
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

    print("Price fetched")

    return price

@task
def create_dataframe(price: float):
    timestamp = datetime.now().isoformat()
    data = {
        "Timestamp":[timestamp],
        "BTC_USD":[price]
    }
    print("Successfully created dataframe")

    return pd.DataFrame(data)

@task
def load_to_bq(df: pd.DataFrame):
    gcp_credentials_block = GcpCredentials.load("my-gcp-creds")
    to_gbq(
        dataframe = df,
        destination_table = 'crypto_ingestion_0.btc_prices',
        project_id = gcp_credentials_block.project_id,
        credentials = gcp_credentials_block.get_credentials_from_service_account(),
        if_exists = 'append'
    )
    print("Data uploaded to BigQuery successfully!")

    
@flow
def ingestion_flow():
    price = fetch_btc()
    df = create_dataframe(price)
    load_to_bq(df)


if __name__ == "__main__":
    ingestion_flow()
