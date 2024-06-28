import json
import boto3
import base64
import os
import mlflow

os.environ["MLFLOW_S3_ENDPOINT_URL"] = "http://localhost:4566"
s3_path = 's3://model-bucket/runs/3cbf46c116d7466c8934f1ca53e34cd5/artifacts/model'
model = mlflow.pyfunc.load_model(s3_path)

TEST_RUN = os.getenv('DRY_RUN', 'False') == 'True'

kinesis_client = boto3.client(
    'kinesis',
    endpoint_url='http://localhost:4566',  # LocalStack endpoint
    region_name='us-east-2',  # Match the region in your config
    aws_access_key_id='test',  # Use dummy credentials for LocalStack
    aws_secret_access_key='test'
)

PREDICTIONS_STREAM_NAME = os.getenv('PREDICTIONS_STREAM_NAME', 'ride_predictions')

def prepare_features(ride):
    features = {}
    features['PU_DO'] = '%s_%s' % (ride['PULocationID'], ride['DOLocationID'])
    features['trip_distance'] = ride['trip_distance']
    return features

def predict(features):
    pred = model.predict(features)
    return float(pred[0])

def lambda_handler(event, context):

    predictions_events = []

    for record in event['Records']:
        encoded_data = record['kinesis']['data']
        decoded_data = base64.b64decode(encoded_data).decode('utf-8')
        ride_event = json.loads(decoded_data)
        
        ride = ride_event['ride']
        ride_id = ride_event['ride_id']
        features = prepare_features(ride)
        prediction = predict(features)

        prediction_event = {
            'model': 'ride_duration_prediction_model',
            'version': '123',
            'prediction': {
                'ride_duration': prediction,
                'ride_id': ride_id
            }
        }

        if not TEST_RUN:
            kinesis_client.put_record(
                StreamName=PREDICTIONS_STREAM_NAME,
                Data=json.dumps(prediction_event),
                PartitionKey=str(ride_id)
            )

        predictions_events.append(prediction_event)

    return {
        'predictions': predictions_events
    }
