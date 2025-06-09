import boto3

def upload_to_s3(local_path, bucket, s3_path, endpoint="http://localhost:4566"):
    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id="dummy",
        aws_secret_access_key="dummy",
        region_name="us-east-1"
    )
    s3.upload_file(local_path, bucket, s3_path)
    print(f"[SUCCESS] {local_path} -> s3a://{bucket}/{s3_path}")