import requests

event = {
    "Records": [
        {
            "kinesis": {
                "kinesisSchemaVersion": "1.0",
                "partitionKey": "1",
                "sequenceNumber": "49653421388016108682042300602588475846347478292047593474",
                "data": "ewogICAgInJpZGUiOiB7CiAgICAgICAgIlBVTG9jYXRpb25JRCI6IDEzMCwKICAgICAgICAiRE9Mb2NhdGlvbklEIjogMjA1LAogICAgICAgICJ0cmlwX2Rpc3RhbmNlIjogMy42NgogICAgfSwKICAgICJyaWRlX2lkIjogMjU2Cn0=",
                "approximateArrivalTimestamp": 1719574237.314
            },
            "eventSource": "aws:kinesis",
            "eventVersion": "1.0",
            "eventID": "shardId-000000000000:49653421388016108682042300602588475846347478292047593474",
            "eventName": "aws:kinesis:record",
            "invokeIdentityArn": "arn:aws:iam::000000000000:role/lambda-kinesis-role",
            "awsRegion": "us-east-2",
            "eventSourceARN": "arn:aws:kinesis:us-east-2:000000000000:stream/ride_events"
        }
    ]
}

url = 'http://localhost:8080/2015-03-31/functions/function/invocations'
response = requests.post(url, json=event)
print(response.json())