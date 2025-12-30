def validate_dataframe(df):
    # Vérifie que les colonnes essentielles existent
    required_columns = ["sale_date", "sale_amount", "customer_name"]
    for col in required_columns:
        if col not in df.columns:
            raise ValueError(f"Colonne manquante : {col}")

    # Vérifie qu'il n'y a pas de valeurs négatives dans sale_amount
    if (df["sale_amount"] < 0).any():
        raise ValueError("Valeurs négatives détectées dans sale_amount")

    return True
