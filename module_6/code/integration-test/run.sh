#!/usr/bin/env bash

cd "$(dirname "$0")"

LOCAL_TAG=$(date +"%Y-%m-%d-%H-%M")
export LOCAL_IMG_TAG="stream-model-duration:${LOCAL_TAG}"
export PREDICTIONS_STREAM_NAME="ride_predictions_kinesis_v9"

docker build -t ${LOCAL_IMG_TAG} ..

docker-compose up -d

sleep 1

awslocal kinesis create-stream \
  --stream-name $PREDICTIONS_STREAM_NAME \
  --shard-count 1


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
