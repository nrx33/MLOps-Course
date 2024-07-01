docker build -t stream-model-duration:v8 .

docker run -it --rm --network="host" -e PREDICTIONS_STREAM_NAME="ride_predictions" -e RUN_ID="3cbf46c116d7466c8934f1ca53e34cd5" -e TEST_RUN="True" -e AWS_ACCESS_KEY_ID="test" -e AWS_SECRET_ACCESS_KEY="test" -e AWS_DEFAULT_REGION="us-east-2" -e S3_ENDPOINT_URL="http://localhost:4566" stream-model-duration:v8

awslocal kinesis get-shard-iterator   --stream-name ride_predictions_kinesis   --shard-id shardId-000000000000   --shard-iterator-type TRIM_HORIZON   --query 'ShardIterator'   --output text

awslocal kinesis get-records --shard-iterator "AAAAAAAAAAF7OkmpnOOopgwgamIw5cKY0Ha1qnmy78n0ashEIpZTE7ATLunn1083HC/rPWwFn8Yu1SAon88vPLVBuowfubMRYh84XtFuFaR0y0Rc8RkoJkGctsIkkcvQztezWeIQ+/eT7KyjiHbqNvHUdtUAWF21Rdn1UGBJd6wVqwmy4vAiZE7/wFNS+okSICFL2dFyTJcvSg+6U54Edv7nPo5fI+s4"

echo "eyJtb2RlbCI6ICJyaWRlX2R1cmF0aW9uX3ByZWRpY3Rpb25fbW9kZWwiLCAidmVyc2lvbiI6ICIzY2JmNDZjMTE2ZDc0NjZjODkzNGYxY2E1M2UzNGNkNSIsICJwcmVkaWN0aW9uIjogeyJyaWRlX2R1cmF0aW9uIjogMTguMTY4OTQ1NzI2NDA1MzI2LCAicmlkZV9pZCI6IDI1Nn19" | base64 -d
