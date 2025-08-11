from prefect import flow, task
import requests
from datetime import datetime

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
def load_btc(price: float):
    timestamp = datetime.now().isoformat()
    
@flow
def ingestion_flow():
    price = fetch_btc()
    load_btc(price)

if __name__ == "__main__":
    hello_flow()
