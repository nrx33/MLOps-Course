# import libraries
import pandas as pd
import mlflow
import warnings
import uuid
import numpy as np
import argparse

warnings.filterwarnings("ignore", category=FutureWarning) # to suppress FutureWarnings

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

def load_model(RUN_ID):
    logged_model = f'runs:/{RUN_ID}/model'
    model = mlflow.pyfunc.load_model(logged_model)
    return model

def apply_model(input_file, RUN_ID, output_file):
    print(f'Reading the data from {input_file}...')
    df = read_dataframe(input_file)
    dicts = prepare_dictionaries(df)

    print(f'Loading the model from RUN ID {RUN_ID}...')
    model = load_model(RUN_ID)
    
    print('Applying the model...')
    y_pred = model.predict(dicts)

    df_result = pd.DataFrame()
    df_result['ride_id'] = df['ride_id']
    df_result['lpep_pickup_datetime'] = df['lpep_pickup_datetime']
    df_result['PULocationID'] = df['PULocationID']
    df_result['DOLocationID'] = df['DOLocationID']
    df_result['actual_duration'] = round(df['duration'],2)
    df_result['predicted_duration'] = np.round(y_pred, 2)
    df_result['diff'] = round(df_result['actual_duration'] - df_result['predicted_duration'], 2)
    df_result['model_version'] = RUN_ID

    print(f'Saving the results to {output_file}...')
    df_result.to_parquet(output_file, index=False)

def run(year, month, taxi_type, RUN_ID):
    input_file = f'https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_tripdata_{year:04d}-{month:02d}.parquet'
    output_file = f'output/{taxi_type}/{year:04d}-{month:02d}.parquet'
    
    MLFLOW_TRACKING_URI = 'http://127.0.0.1:5000'
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)

    apply_model(
        input_file, 
        RUN_ID, 
        output_file
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
    RUN_ID = args.RUN_ID

    run(year, month, taxi_type, RUN_ID)
