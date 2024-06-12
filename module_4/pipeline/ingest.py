import requests
from io import BytesIO

import pandas as pd

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader


@data_loader
def ingest_files() -> pd.DataFrame:
    url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-08.parquet'
    
    response = requests.get(url)

    if response.status_code != 200:
        raise Exception(response.text)

    df = pd.read_parquet(BytesIO(response.content))
    
    return df