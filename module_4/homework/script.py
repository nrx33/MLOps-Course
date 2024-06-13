import pickle
import pandas as pd
import warnings
import numpy as np
import os
import sys

# remove warnings
warnings.filterwarnings("ignore")

# read the data
def read_data(filename):
    df = pd.read_parquet(filename)
    df['duration'] = df.tpep_dropoff_datetime - df.tpep_pickup_datetime
    df['duration'] = df.duration.dt.total_seconds() / 60
    df = df[(df.duration >= 1) & (df.duration <= 60)].copy()
    categorical = ['PULocationID', 'DOLocationID']
    df[categorical] = df[categorical].fillna(-1).astype('int').astype('str')
    return df, categorical

# load the model
def load_model(filename):
    # Load the model and the vectorizer
    with open(filename, 'rb') as f_in:
        dv, model = pickle.load(f_in)
    return dv, model

# predict the data
def predict(df, categorical, dv, model):
    dicts = df[categorical].to_dict(orient='records')
    X_val = dv.transform(dicts)
    y_pred = model.predict(X_val)
    print(f'Q5: The mean predicted duration is {np.mean(y_pred)}')
    return y_pred

# output the results
def output_results(year, month, y_pred, df):
    df_result = pd.DataFrame()
    df_result['predicted_duration'] = y_pred
    df_result['ride_id'] = f'{year}/{month}_' + df.index.astype('str')
    df_result.to_parquet(
        f'output_yellow_tripdata_{year}-{month}.parquet',
        engine='pyarrow',
        compression=None,
        index=False
    )

if __name__ == '__main__':
    # get inputs
    year = str(sys.argv[1]).zfill(4)
    month = str(sys.argv[2]).zfill(2)
                
    # read the data
    df, categorical = read_data(f'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month}.parquet')

    # load the model
    dv, model = load_model('model.bin')

    # predict the data
    y_pred = predict(df, categorical, dv, model)

    # output the results
    output_results(year, month, y_pred, df)