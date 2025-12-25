# lake/writer.py

import boto3
from botocore.exceptions import ClientError

# Config MinIO
MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_SECURE = False

def get_minio_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        use_ssl=MINIO_SECURE
    )

def write_object_to_lake(bucket_name: str, object_name: str, data: bytes):
    """
    Écrit un objet brut dans MinIO.
    """
    client = get_minio_client()

    try:
        # Crée le bucket si besoin
        if not client.list_buckets()['Buckets']:
            client.create_bucket(Bucket=bucket_name)
        client.put_object(Bucket=bucket_name, Key=object_name, Body=data)
        print(f"[INFO] Objet {object_name} écrit dans bucket {bucket_name}")
    except ClientError as e:
        raise RuntimeError(f"Impossible d'écrire {object_name} dans {bucket_name}: {e}")
