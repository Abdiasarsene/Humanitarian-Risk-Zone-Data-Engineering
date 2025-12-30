# collection/cdc.py
import psycopg2
from lake.writer import write_object_to_lake
import pandas as pd
import io

def fetch_cdc_table(conn_str: str, table_name: str, bucket_name: str, object_name: str):
    """
    Récupère la table source et l'écrit sous forme CSV dans MinIO.
    """
    conn = psycopg2.connect(conn_str)
    df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
    conn.close()

    # Convertit en CSV bytes
    csv_bytes = df.to_csv(index=False).encode('utf-8')

    write_object_to_lake(bucket_name, object_name, csv_bytes)
    print(f"[INFO] Table {table_name} copiée dans {bucket_name}/{object_name}")
