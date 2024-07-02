#!/usr/bin/env python
# coding: utf-8

import os
import sys
import pandas as pd
from pyarrow import fs

def get_input_path(year, month):
    default_input = f'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year:04d}-{month:02d}.parquet'
    s3_path = os.getenv('INPUT_FILE_PATTERN', f's3://homework-6/in/{year:04d}-{month:02d}.parquet').format(year=year, month=month)
    
    s3 = fs.S3FileSystem(endpoint_override="http://localhost:4566")
    bucket, key = s3_path[5:].split("/", 1)
    
    if s3.get_file_info([f"{bucket}/{key}"])[0].type == fs.FileType.File:
        print(f"Using S3 file: s3://{bucket}/{key}")
        return f"{bucket}/{key}", True
    else:
        print(f"S3 file not found, using CloudFront URL: {default_input}")
        return default_input, False

def read_data(path, is_s3):
    if is_s3:
        return pd.read_parquet(path, filesystem=fs.S3FileSystem(endpoint_override="http://localhost:4566"))
    return pd.read_parquet(path)

def main(year, month):
    path, is_s3 = get_input_path(year, month)
    df = read_data(path, is_s3)
    print(df.head())

if __name__ == '__main__':
    main(int(sys.argv[1]), int(sys.argv[2]))
