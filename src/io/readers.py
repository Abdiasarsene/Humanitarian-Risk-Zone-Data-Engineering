# io/readers.py
import pandas as pd
import io as pyio
import boto3
from botocore.exceptions import ClientError

# Configuration MinIO (tu peux la passer depuis config.py/env)
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

def read_file_from_lake(bucket_name: str, object_name: str) -> pd.DataFrame:
    """
    Lit un fichier depuis MinIO et retourne un DataFrame pandas.
    Supporte CSV, Parquet, Excel.
    """
    client = get_minio_client()

    try:
        response = client.get_object(Bucket=bucket_name, Key=object_name)
        data = response['Body'].read()
    except ClientError as e:
        raise RuntimeError(f"Impossible de lire {object_name} depuis {bucket_name}: {e}")

    # Détection automatique du format
    if object_name.endswith(".csv"):
        return pd.read_csv(pyio.BytesIO(data))
    elif object_name.endswith(".parquet"):
        return pd.read_parquet(pyio.BytesIO(data))
    elif object_name.endswith(".xlsx") or object_name.endswith(".xls"):
        return pd.read_excel(pyio.BytesIO(data))
    else:
        raise ValueError(f"Format non supporté pour le fichier {object_name}")