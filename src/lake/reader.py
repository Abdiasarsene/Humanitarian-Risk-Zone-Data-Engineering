# lake/reader.py

import boto3
from botocore.exceptions import ClientError

# Config MinIO (ou import depuis config.py/env)
MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_SECURE = False  # HTTP pour dev

def get_minio_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        use_ssl=MINIO_SECURE
    )

def read_object_from_lake(bucket_name: str, object_name: str) -> bytes:
    """
    Lit un objet brut depuis MinIO et retourne son contenu en bytes.
    """
    client = get_minio_client()

    try:
        response = client.get_object(Bucket=bucket_name, Key=object_name)
        data = response['Body'].read()
        return data
    except ClientError as e:
        raise RuntimeError(f"Impossible de lire {object_name} depuis {bucket_name}: {e}")