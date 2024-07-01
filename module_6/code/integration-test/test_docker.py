import os
import json

import requests
from deepdiff import DeepDiff

# Adjust the path as needed
event_json_path = os.path.join(os.path.dirname(__file__), 'event.json')

# Read the file content
with open(event_json_path, 'rt', encoding='utf-8') as f_in:
    event_content = f_in.read()
    event = json.loads(event_content)

url = 'http://localhost:8080/2015-03-31/functions/function/invocations'
actual_response = requests.post(url, json=event, timeout=4).json()
print("Actual Response:", json.dumps(actual_response, indent=2))

expected_response = {
    'predictions': [
        {
            'model': 'ride_duration_prediction_model',
            'version': '3cbf46c116d7466c8934f1ca53e34cd5',
            'prediction': {'ride_duration': 18.17, 'ride_id': 256},
        }
    ]
}

diff = DeepDiff(actual_response, expected_response, significant_digits=2)
print(f'diff={diff}')

assert 'type_changes' not in diff
assert 'values_changed' not in diff
