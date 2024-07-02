import sys
import pickle
import pandas as pd
import os
from pyarrow import fs
import time

def read_data(path, is_s3):
    if is_s3:
        options = {
            'client_kwargs': {
                'endpoint_url': os.getenv('S3_ENDPOINT_URL')
            }
        } if os.getenv('S3_ENDPOINT_URL') else None
        return pd.read_parquet(path, storage_options=options)
    else:
        return pd.read_parquet(path)

def prepare_data(df, features):
    df['duration'] = (df.tpep_dropoff_datetime - df.tpep_pickup_datetime).dt.total_seconds() / 60
    df = df[(df.duration >= 1) & (df.duration <= 60)].copy()
    df[features] = df[features].fillna(-1).astype('int').astype('str')
    return df

def get_input_path(year, month):
    s3_path = os.getenv('INPUT_FILE_PATTERN', f's3://homework-6/in/{year:04d}-{month:02d}.parquet').format(year=year, month=month)
    s3 = fs.S3FileSystem(endpoint_override="http://localhost:4566") if os.getenv('S3_ENDPOINT_URL') else None
    bucket, key = s3_path[5:].split("/", 1)
    if s3 and s3.get_file_info([f"{bucket}/{key}"])[0].type == fs.FileType.File:
        print(f"Using S3 bucket: s3://{bucket}/{key}")
        return f"s3://{bucket}/{key}", True
    default_input = f'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year:04d}-{month:02d}.parquet'
    print(f"File not available in S3 bucket,\nusing Cloudfront: {default_input}")
    return default_input, False

def get_output_path(year, month):
    output_pattern = os.getenv('OUTPUT_FILE_PATTERN', 's3://homework-6/out/{year:04d}-{month:02d}.parquet')
    return output_pattern.format(year=year, month=month)

def save_data(df, output_path):
    s3 = fs.S3FileSystem(endpoint_override="http://localhost:4566")
    bucket, key = output_path.replace("s3://", "").split("/", 1)
    # Write the dataframe to the specified key in the bucket
    df.to_parquet(f"{bucket}/{key}", filesystem=s3, engine='pyarrow', index=False)
    print(f"Results saved to s3://{bucket}/{key}")

def main(year, month):
    categorical = ['PULocationID', 'DOLocationID']
    with open('model.bin', 'rb') as f_in:
        dv, lr = pickle.load(f_in)
    path, is_s3 = get_input_path(year, month)
    df = prepare_data(read_data(path, is_s3), categorical)
    df['ride_id'] = f'{year:04d}/{month:02d}_' + df.index.astype('str')
    X_val = dv.transform(df[categorical].to_dict(orient='records'))
    y_pred = lr.predict(X_val)
    print('predicted mean duration:', y_pred.mean())
    df_result = pd.DataFrame({'ride_id': df['ride_id'], 'predicted_duration': y_pred})
    output_path = get_output_path(year, month)
    save_data(df_result, output_path)

if __name__ == '__main__':
    start_time = time.time()
    main(int(sys.argv[1]), int(sys.argv[2]))
    print(f"Script executed in {time.time() - start_time:.2f} seconds")
