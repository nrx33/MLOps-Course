import boto3

# Configure Boto3 to use LocalStack
s3_client = boto3.client('s3', endpoint_url='http://localhost:4566')

def delete_all_objects(bucket_name):
    try:
        # List all objects in the bucket
        response = s3_client.list_objects_v2(Bucket=bucket_name)
        if 'Contents' in response:
            for obj in response['Contents']:
                print(f"Deleting {obj['Key']}")
                s3_client.delete_object(Bucket=bucket_name, Key=obj['Key'])
        else:
            print(f"No objects found in bucket '{bucket_name}'")
    except Exception as e:
        print(f"Failed to delete objects in bucket '{bucket_name}'. Error: {str(e)}")

if __name__ == "__main__":
    bucket_name = "model-bucket"
    delete_all_objects(bucket_name)
