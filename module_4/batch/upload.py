import boto3
import os

# Configure Boto3 to use LocalStack
s3_client = boto3.client('s3', endpoint_url='http://localhost:4566')

def upload_file(bucket_name, object_name, file_path):
    try:
        s3_client.upload_file(file_path, bucket_name, object_name)
        print(f"Uploaded {file_path} to s3://{bucket_name}/{object_name}")
    except Exception as e:
        print(f"Error uploading {file_path} to s3://{bucket_name}/{object_name}: {e}")

if __name__ == "__main__":
    bucket_name = "model-bucket"
    run_id = "3cbf46c116d7466c8934f1ca53e34cd5"
    base_path = "/workspaces/MLOps-Course/module_4/web_service_mlflow/mlartifacts/1/3cbf46c116d7466c8934f1ca53e34cd5/artifacts/model"
    s3_base_prefix = f"runs/{run_id}/artifacts/model"

    # Create the bucket if it doesn't exist
    try:
        s3_client.create_bucket(Bucket=bucket_name)
        print(f"Bucket '{bucket_name}' created.")
    except s3_client.exceptions.BucketAlreadyExists:
        print(f"Bucket '{bucket_name}' already exists.")
    except s3_client.exceptions.BucketAlreadyOwnedByYou:
        print(f"Bucket '{bucket_name}' already owned by you.")

    # Manually upload each file to the correct location
    files_to_upload = {
        "MLmodel": f"{s3_base_prefix}/MLmodel",
        "conda.yaml": f"{s3_base_prefix}/conda.yaml",
        "model.pkl": f"{s3_base_prefix}/model.pkl",
        "python_env.yaml": f"{s3_base_prefix}/python_env.yaml",
        "requirements.txt": f"{s3_base_prefix}/requirements.txt",
        "metadata/MLmodel": f"{s3_base_prefix}/metadata/MLmodel",
        "metadata/conda.yaml": f"{s3_base_prefix}/metadata/conda.yaml",
        "metadata/python_env.yaml": f"{s3_base_prefix}/metadata/python_env.yaml",
        "metadata/requirements.txt": f"{s3_base_prefix}/metadata/requirements.txt"
    }

    for local_file, s3_path in files_to_upload.items():
        full_local_path = os.path.join(base_path, local_file)
        if os.path.exists(full_local_path):
            upload_file(bucket_name, s3_path, full_local_path)
        else:
            print(f"File not found: {full_local_path}")
