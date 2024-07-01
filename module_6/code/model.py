import json
import boto3
import base64
import os
import mlflow

os.environ["MLFLOW_S3_ENDPOINT_URL"] = "http://localhost:4566"

def load_model(run_id):
    s3_path = f's3://model-bucket/runs/{run_id}/artifacts/model'
    model = mlflow.pyfunc.load_model(s3_path)
    return model

def base64_decode(encoded_data):
    decoded_data = base64.b64decode(encoded_data).decode('utf-8')
    ride_event = json.loads(decoded_data)
    return ride_event

class ModelService():
    def __init__(self, model, prediction_stream_name, test_run, model_version=None, callbacks=None):
        self.model = model
        self.prediction_stream_name = prediction_stream_name
        self.test_run = test_run
        self.model_version = model_version
        self.callbacks = callbacks or []

        self.kinesis_client = boto3.client(
            'kinesis',
            endpoint_url='http://localhost:4566',  # LocalStack endpoint
            region_name='us-east-2',  # Match the region in your config
            aws_access_key_id='test',  # Use dummy credentials for LocalStack
            aws_secret_access_key='test'
        )

    def prepare_features(self, ride):
        features = {}
        features['PU_DO'] = f"{ride['PULocationID']}_{ride['DOLocationID']}"
        features['trip_distance'] = ride['trip_distance']
        return features

    def predict(self, features):
        pred = self.model.predict(features)
        return float(pred[0])

    def lambda_handler(self, event):
        predictions_events = []

        for record in event['Records']:
            encoded_data = record['kinesis']['data']
            ride_event = base64_decode(encoded_data)
            ride = ride_event['ride']
            ride_id = ride_event['ride_id']
            features = self.prepare_features(ride)
            prediction = self.predict(features)

            prediction_event = {
                'model': 'ride_duration_prediction_model',
                'version': self.model_version,
                'prediction': {
                    'ride_duration': prediction,
                    'ride_id': ride_id
                }
            }

            for callback in self.callbacks:
                callback(prediction_event)

            predictions_events.append(prediction_event)

        return {
            'predictions': predictions_events
        }

class KinesisCallback:
    def __init__(self, kinesis_client, prediction_stream_name):
        self.kinesis_client = kinesis_client
        self.prediction_stream_name = prediction_stream_name

    def put_record(self, prediction_event):
        ride_id = prediction_event['prediction']['ride_id']

        self.kinesis_client.put_record(
            StreamName=self.prediction_stream_name,
            Data=json.dumps(prediction_event),
            PartitionKey=str(ride_id),
        )

def create_kinesis_client():
    return boto3.client(
        'kinesis',
        endpoint_url='http://localhost:4566',  # LocalStack endpoint
        region_name='us-east-2',  # Match the region in your config
        aws_access_key_id='test',  # Use dummy credentials for LocalStack
        aws_secret_access_key='test'
    )

def init(prediction_stream_name: str, run_id: str, test_run: bool):
    model = load_model(run_id)
    callbacks = []

    if not test_run:
        kinesis_client = create_kinesis_client()
        kinesis_callback = KinesisCallback(kinesis_client, prediction_stream_name)
        callbacks.append(kinesis_callback.put_record)

    model_service = ModelService(
        model,
        prediction_stream_name,
        test_run,
        model_version=run_id,
        callbacks=callbacks)

    return model_service
