
import pandas as pd

def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    # Suppression des doublons
    df = df.drop_duplicates()
    
    # Remplacer les valeurs manquantes par 0 ou placeholder
    df = df.fillna({"sale_amount": 0, "customer_name": "Unknown"})
    
    # Normalisation des types
    df["sale_date"] = pd.to_datetime(df["sale_date"], errors="coerce")
    
    return df
