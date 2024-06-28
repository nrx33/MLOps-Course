import json
import uuid
import datetime
import sys
import io
import base64
from lambda_function import lambda_handler

# Redirect stdout to capture prints
old_stdout = sys.stdout
new_stdout = io.StringIO()
sys.stdout = new_stdout

# Generate a mock RequestId and timestamp for the simulation
request_id = str(uuid.uuid4())
start_time = datetime.datetime.now()
end_time = start_time + datetime.timedelta(milliseconds=1.41)
duration = (end_time - start_time).total_seconds() * 1000

# Load the test event from the JSON file
with open('./test_event.json', 'r') as file:
    test_event = json.load(file)

# Define a mock context (can be None or a mock object)
class MockContext:
    function_name = "test-lambda-function"
    memory_limit_in_mb = 128
    invoked_function_arn = "arn:aws:lambda:aws-region:acct-id:function:test-lambda-function"
    aws_request_id = request_id

mock_context = MockContext()

# Call the lambda_handler function with the test event
result = lambda_handler(test_event, mock_context)

# Capture and format the lambda function output
lambda_output = new_stdout.getvalue()
sys.stdout = old_stdout  # Reset stdout

# Format the event and result as JSON strings with indentation
formatted_event = json.dumps(test_event, indent=4)
formatted_result = json.dumps(result, indent=4)

# Print the output to match the desired format
print("Execution result")
print()
print("Test Event Name")
print("test-kinesis")
print()
print("Response")
print(formatted_result)
print()
print("Function Logs")
print(f"START RequestId: {request_id} Version: $LATEST")
print(formatted_event)
print()
print(lambda_output)  # This will include any print statements from the lambda_handler
print(f"END RequestId: {request_id}")
print(f"REPORT RequestId: {request_id} Duration: {duration:.2f} ms")
print()
print("Request ID")
print(request_id)
