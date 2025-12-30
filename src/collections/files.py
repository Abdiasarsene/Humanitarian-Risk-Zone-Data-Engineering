# collection/files.py
from lake.writer import write_object_to_lake

def copy_local_file_to_lake(local_path: str, bucket_name: str, object_name: str):
    """
    Copie un fichier local dans MinIO.
    """
    with open(local_path, "rb") as f:
        data_bytes = f.read()
    write_object_to_lake(bucket_name, object_name, data_bytes)
    print(f"[INFO] Fichier {local_path} copié vers {bucket_name}/{object_name}")