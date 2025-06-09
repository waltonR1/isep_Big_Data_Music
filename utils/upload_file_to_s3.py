import os
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
    print(f"[SUCCESS] {local_path} -> s3://{bucket}/{s3_path}")


if __name__ == "__main__":
    upload_to_s3("D:/PythonProjects/Big_Data_Music/data/raw/spotify/trackLists/20250608/trackLists.json", "raw-data-music", "spotify/trackLists/20250608/trackLists.json")
    upload_to_s3("D:/PythonProjects/Big_Data_Music/data/raw/spotify/trackLists/20250609/trackLists.json", "raw-data-music", "spotify/trackLists/20250609/trackLists.json")