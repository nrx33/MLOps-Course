import pandas as pd
import mlflow
import warnings
import uuid
import numpy as np
import argparse
import io
import boto3
import os

warnings.filterwarnings("ignore", category=FutureWarning)  # to suppress FutureWarnings

# Set the MLflow S3 endpoint URL for LocalStack
os.environ["MLFLOW_S3_ENDPOINT_URL"] = "http://localhost:4566"

def read_dataframe(filename: str):
    df = pd.read_parquet(filename)
    df['duration'] = df.lpep_dropoff_datetime - df.lpep_pickup_datetime
    df.duration = df.duration.dt.total_seconds() / 60
    df = df[(df.duration >= 1) & (df.duration <= 60)]
    df['ride_id'] = generate_uuid(len(df))
    return df

def prepare_dictionaries(df: pd.DataFrame):
    categorical = ['PULocationID', 'DOLocationID']
    df[categorical] = df[categorical].astype(str)
    df['PU_DO'] = df['PULocationID'] + '_' + df['DOLocationID']
    categorical = ['PU_DO']
    numerical = ['trip_distance']
    dicts = df[categorical + numerical].to_dict(orient='records')
    return dicts

def generate_uuid(n):
    ride_id = [str(uuid.uuid4()) for _ in range(n)]
    return ride_id

def load_model(run_id):
    bucket_name = "model-bucket"
    s3_path = f"s3://{bucket_name}/runs/{run_id}/artifacts/model"
    model = mlflow.pyfunc.load_model(s3_path)
    return model

def apply_model(input_file, run_id, output_file, bucket_name):
    print(f'Reading the data from {input_file}...')
    df = read_dataframe(input_file)
    dicts = prepare_dictionaries(df)

    print(f'Loading the model from RUN ID {run_id}...')
    model = load_model(run_id)
    
    print('Applying the model...')
    y_pred = model.predict(dicts)

    df_result = pd.DataFrame()
    df_result['ride_id'] = df['ride_id']
    df_result['lpep_pickup_datetime'] = df['lpep_pickup_datetime']
    df_result['PULocationID'] = df['PULocationID']
    df_result['DOLocationID'] = df['DOLocationID']
    df_result['actual_duration'] = round(df['duration'], 2)
    df_result['predicted_duration'] = np.round(y_pred, 2)
    df_result['diff'] = round(df_result['actual_duration'] - df_result['predicted_duration'], 2)
    df_result['model_version'] = run_id

    # Save the DataFrame to a buffer
    buffer = io.BytesIO()
    df_result.to_parquet(buffer, index=False)
    buffer.seek(0)

    # Upload the output file to S3
    s3 = boto3.client('s3', endpoint_url='http://localhost:4566')  # Use LocalStack endpoint
    s3.put_object(Bucket=bucket_name, Key=output_file, Body=buffer.getvalue())
    s3_output_path = f"s3://{bucket_name}/{output_file}"
    print(f'Saved the results to {s3_output_path} in S3')

def run(year, month, taxi_type, run_id):
    input_file = f'https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_tripdata_{year:04d}-{month:02d}.parquet'
    output_file = f'output/{taxi_type}/{year:04d}-{month:02d}.parquet'
    
    MLFLOW_TRACKING_URI = 'http://127.0.0.1:5000'
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    
    bucket_name = "model-bucket"

    apply_model(
        input_file, 
        run_id, 
        output_file,
        bucket_name
    )

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Process some taxi datasets")
    parser.add_argument("taxi_type", help="Taxi type")
    parser.add_argument("year", type=int, help="Year")
    parser.add_argument("month", type=int, help="Month")
    parser.add_argument("RUN_ID", help="Run ID")
    args = parser.parse_args()

    taxi_type = args.taxi_type
    year = args.year
    month = args.month
    run_id = args.RUN_ID

    run(year, month, taxi_type, run_id)
