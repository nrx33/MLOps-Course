#!/bin/bash

export INPUT_FILE_PATTERN="s3://homework-6/in/{year:04d}-{month:02d}.parquet"
export OUTPUT_FILE_PATTERN="s3://homework-6/out/{year:04d}-{month:02d}.parquet"
export S3_ENDPOINT_URL="http://localhost:4566"

cd "$(dirname "$0")" || exit

# Activate your Python environment if needed
if [ -z "$PIPENV_ACTIVE" ]; then
    pipenv shell
fi

# Run your Python script
if python create_test_dataset.py && cd .. && pylint --recursive=y . && pytest ./tests && black . && isort . && python batch.py 2022 01; then
    echo "Integration Test Passed!"
else
    echo "Integration Test Failed!"
fi
