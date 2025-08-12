from prefect import flow, task
from datetime import datetime
from prefect_gcp import GcpCredentials
#from pandas_gbq import to_gbq
from prefect_gcp.bigquery import BigQueryWarehouse
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

# @task
# def create_dataframe(price: float):
#     timestamp = datetime.now().isoformat()
#     data = {
#         "Timestamp":[timestamp],
#         "BTC_USD":[price]
#     }

#     return pd.DataFrame(data)

@task
def load_to_bq(price: float):
    # gcp_credentials_block = GcpCredentials.load("my-gcp-creds")
    # print(gcp_credentials_block.project)
    # print(gcp_credentials_block.get_credentials_from_service_account())
    # to_gbq(
    #     dataframe = df,
    #     destination_table = 'crypto_ingestion_0.btc_prices',
    #     project_id = 'crypto-ingestion-468703',
    #     credentials = gcp_credentials_block.get_credentials_from_service_account(),
    #     if_exists = 'append'
    # )
    # print("Data uploaded to BigQuery successfully!")
    timestamp = datetime.now().isoformat()
    bigquery_warehouse_block = BigQueryWarehouse.load("bgq-wh")
    create_sql = """
    CREATE TABLE IF NOT EXISTS `crypto_ingestion_0.btc_prices` (
        Timestamp TIMESTAMP,
        BTC_USD FLOAT
    )
    """
    insert_sql = f"""
    INSERT INTO `crypto_ingestion_0.btc_prices` (Timestamp, BTC_USD)
    VALUES ({timestamp},{price})
    """
    
    bigquery_warehouse_block.execute(create_sql)
    bigquery_warehouse_block.execute(insert_sql)


@flow(log_prints=True)
def ingestion_flow():
    price = fetch_btc()
    # df = create_dataframe(price)
    load_to_bq(price)


if __name__ == "__main__":
    ingestion_flow()
