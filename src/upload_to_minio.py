import os
import sys

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()


def _require_env(name):
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(
            f"Missing required environment variable: {name}. "
            "Copy .env.example to .env and fill in your MinIO credentials."
        )
    return value


MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ROOT_USER = _require_env("MINIO_ROOT_USER")
MINIO_ROOT_PASSWORD = _require_env("MINIO_ROOT_PASSWORD")
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "lakehouse")

LOCAL_FILE = os.path.join("data", "raw", "yellow_tripdata_2023-01.parquet")
S3_KEY = "raw/nyc-taxi/year=2023/month=01/yellow_tripdata_2023-01.parquet"


def get_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ROOT_USER,
        aws_secret_access_key=MINIO_ROOT_PASSWORD,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def ensure_bucket(client, bucket):
    try:
        client.head_bucket(Bucket=bucket)
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchBucket"):
            client.create_bucket(Bucket=bucket)
        else:
            raise


def object_exists(client, bucket, key):
    try:
        client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchKey"):
            return False
        raise


def main():
    if not os.path.isfile(LOCAL_FILE):
        print(f"Local file not found: {LOCAL_FILE}", file=sys.stderr)
        sys.exit(1)

    client = get_client()
    ensure_bucket(client, MINIO_BUCKET)

    if object_exists(client, MINIO_BUCKET, S3_KEY):
        print(f"Skipping upload: s3://{MINIO_BUCKET}/{S3_KEY} already exists")
        return

    print(f"Uploading {LOCAL_FILE} -> s3://{MINIO_BUCKET}/{S3_KEY}")
    client.upload_file(LOCAL_FILE, MINIO_BUCKET, S3_KEY)
    print("Upload complete")


if __name__ == "__main__":
    main()
