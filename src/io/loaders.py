# io/loaders.py

import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime

# PostgreSQL config (tu peux la passer depuis config.py/env)
POSTGRES_URI = "postgresql+psycopg2://admin:admin@localhost:5432/warehouse"

def load_dataframe_to_postgres(df: pd.DataFrame, table_name: str, schema: str = "raw", add_metadata: bool = True):
    """
    Charge un DataFrame dans PostgreSQL, dans le schéma spécifié.
    Ajoute des colonnes techniques si add_metadata=True.
    """
    engine = create_engine(POSTGRES_URI)

    df_to_load = df.copy()

    if add_metadata:
        df_to_load["__loaded_at"] = datetime.utcnow()

    # Crée le schéma s'il n'existe pas
    with engine.connect() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema};"))

    # Charge dans la table
    df_to_load.to_sql(table_name, engine, schema=schema, if_exists="append", index=False)
    print(f"[INFO] {len(df_to_load)} lignes chargées dans {schema}.{table_name}")