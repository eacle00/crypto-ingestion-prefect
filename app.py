from prefect import flow, task
from prefect.blocks.system import Secret
from google.oauth2 import service_account
from datetime import datetime
import pandas as pd
from pandas_gbq import to_gbq
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
    creds_block = Secret.load("my-gcp-creds")
    creds_dict = gcp_creds_block.value
    credentials = service_account.Credentials.from_service_account_info(creds_dict)
    to_gbq(
        dataframe = df,
        destination_table = 'crypto_ingestion_0.btc_prices',
        project_id = creds_dict["project_id"],
        credentials = credentials,
        if_exists = 'append'
    )
    print("Data uploaded successfully!")

    
@flow
def ingestion_flow():
    price = fetch_btc()
    df = create_dataframe(price)
    load_to_bq(df)


if __name__ == "__main__":
    ingestion_flow()
