#!/usr/bin/env bash

cd "$(dirname "$0")"

LOCAL_TAG=$(date +"%Y-%m-%d-%H-%M")
export LOCAL_IMG_TAG="stream-model-duration:${LOCAL_TAG}"

# Base name for the stream
BASE_STREAM_NAME="ride_predictions_kinesis"

# List all existing streams and extract the version numbers
existing_streams=$(awslocal kinesis list-streams --query 'StreamNames[*]' --output text)
highest_version=0

for stream in $existing_streams; do
  if [[ $stream =~ ${BASE_STREAM_NAME}_v([0-9]+) ]]; then
    version=${BASH_REMATCH[1]}
    if (( version > highest_version )); then
      highest_version=$version
    fi
  fi
done

# Increment the version number for the new stream
next_version=$((highest_version + 1))
export PREDICTIONS_STREAM_NAME="${BASE_STREAM_NAME}_v${next_version}"

docker build -t ${LOCAL_IMG_TAG} ..

docker-compose up -d

sleep 1

# Create the new stream
awslocal kinesis create-stream \
  --stream-name $PREDICTIONS_STREAM_NAME \
  --shard-count 1
echo "Stream $PREDICTIONS_STREAM_NAME created."

export PIPENV_VERBOSITY=-1

pipenv run python test_docker.py

ERROR_CODE=$?

if [ $ERROR_CODE != 0 ]; then
    docker-compose logs
    docker-compose down
    exit $ERROR_CODE
fi

pipenv run python test_kinesis.py

ERROR_CODE=$?

if [ $ERROR_CODE != 0 ]; then
    docker-compose logs
    docker-compose down
    exit $ERROR_CODE
fi

docker-compose down