import os
from datetime import datetime

import pandas as pd
from pyarrow import fs


def dt(hour, minute, second=0):
    return datetime(2022, 1, 1, hour, minute, second)


# Create the dataframe as in Q3
data = [
    (None, None, dt(1, 1), dt(1, 10)),
    (1, 1, dt(1, 2), dt(1, 10)),
    (1, None, dt(1, 2, 0), dt(1, 2, 59)),
    (3, 4, dt(1, 2, 0), dt(2, 2, 1)),
]

columns = [
    "PULocationID",
    "DOLocationID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
]
df = pd.DataFrame(data, columns=columns)

# Get the S3 endpoint URL from the environment variable
s3_endpoint_url = os.getenv("S3_ENDPOINT_URL", "http://localhost:4566")

# Initialize pyarrow S3FileSystem with the Localstack endpoint
s3 = fs.S3FileSystem(endpoint_override=s3_endpoint_url)

# Define bucket and key
bucket = "homework-6"
key = "in/2023-01.parquet"

# Save the dataframe to a parquet file in Localstack S3
df.to_parquet(
    f"{bucket}/{key}", engine="pyarrow", filesystem=s3, index=False, compression=None
)

print("Dataframe saved to Localstack S3.")
