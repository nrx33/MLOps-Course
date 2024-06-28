docker run -it --rm \
--network="host" \
-e PREDICTIONS_STREAM_NAME="ride_predictions" \
-e RUN_ID="3cbf46c116d7466c8934f1ca53e34cd5" \
-e TEST_RUN="True" \
-e AWS_ACCESS_KEY_ID="test" \
-e AWS_SECRET_ACCESS_KEY="test" \
-e AWS_DEFAULT_REGION="us-east-2" \
-e S3_ENDPOINT_URL="http://localhost:4566" \
stream-model-duration:v1
