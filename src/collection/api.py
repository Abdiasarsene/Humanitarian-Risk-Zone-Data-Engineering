# collection/api.py

import requests
from lake.writer import write_object_to_lake

def fetch_api_data(url: str, params: dict, bucket_name: str, object_name: str):
    """
    Récupère des données depuis une API et les écrit dans MinIO.
    """
    response = requests.get(url, params=params)
    response.raise_for_status()
    data_bytes = response.content

    write_object_to_lake(bucket_name, object_name, data_bytes)
    print(f"[INFO] Données API stockées dans {bucket_name}/{object_name}")
