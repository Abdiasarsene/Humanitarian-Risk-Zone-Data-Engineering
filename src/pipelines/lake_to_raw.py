# pipelines/lake_to_raw.py

from lake.reader import read_object_from_lake
from lake.writer import write_object_to_lake
from io.readers import read_file_from_lake
from io.loaders import load_dataframe_to_postgres
import boto3
import os

BUCKET_NAME = "bronze"

def run_lake_to_raw():
    # Connexion MinIO
    client = boto3.client(
        "s3",
        endpoint_url="http://localhost:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
        use_ssl=False
    )

    # Lister tous les objets dans le bucket
    response = client.list_objects_v2(Bucket=BUCKET_NAME)
    if 'Contents' not in response:
        print("[INFO] Aucun fichier trouvé dans le bucket")
        return

    for obj in response['Contents']:
        object_name = obj['Key']
        print(f"[INFO] Traitement de {object_name}")

        try:
            # Lire et transformer en DataFrame
            df = read_file_from_lake(bucket_name=BUCKET_NAME, object_name=object_name)

            # Nom de table = basename sans extension
            table_name = os.path.splitext(os.path.basename(object_name))[0]

            # Charger dans raw
            load_dataframe_to_postgres(df, table_name=table_name, schema="raw")
        except Exception as e:
            print(f"[ERROR] Échec traitement {object_name}: {e}")

    print("[INFO] Ingestion de tous les fichiers terminée.")

if __name__ == "__main__":
    run_lake_to_raw()