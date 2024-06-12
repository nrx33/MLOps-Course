if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

import pandas as pd
import mlflow
import uuid
import numpy as np

MLFLOW_TRACKING_URI = 'http://127.0.0.1:5000'
RUN_ID = "3cbf46c116d7466c8934f1ca53e34cd5"

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

def load_model(run_id: str):
    logged_model = f'runs:/{run_id}/model'
    model = mlflow.pyfunc.load_model(logged_model)
    return model

@transformer
def transform(data, **kwargs):
    """
    Transformer block for applying a model to taxi data.

    Args:
        data: The input DataFrame
        mlflow_tracking_uri: The MLflow tracking URI
        run_id: The MLflow run ID

    Returns:
        DataFrame containing the processed results
    """
    df = data
    mlflow_tracking_uri = kwargs.get('mlflow_tracking_uri', MLFLOW_TRACKING_URI)
    run_id = kwargs.get('run_id', RUN_ID)

    df['ride_id'] = generate_uuid(len(df))
    df['duration'] = (df['lpep_dropoff_datetime'] - df['lpep_pickup_datetime']).dt.total_seconds() / 60
    df = df[(df['duration'] >= 1) & (df['duration'] <= 60)]
    dicts = prepare_dictionaries(df)

    mlflow.set_tracking_uri(mlflow_tracking_uri)
    model = load_model(run_id)
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

    df_result.to_parquet('output_data.parquet', index=False)

    return df_result

@test
def test_output(output) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
    assert isinstance(output, pd.DataFrame), 'The output is not a DataFrame'
    assert 'ride_id' in output.columns, 'The output DataFrame does not contain the ride_id column'
