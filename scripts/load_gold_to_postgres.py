import pandas as pd
import s3fs
import json
import numpy as np
from sqlalchemy import create_engine

# ======================================================
# 1. CONFIGURATION
# ======================================================

# -------- MinIO / S3 --------
S3_ENDPOINT = "http://localhost:9000"
S3_KEY = "minioadmin"
S3_SECRET = "minioadmin"

# -------- PostgreSQL --------
PG_USER = "postgres"
PG_PASSWORD = "postgres"
PG_HOST = "localhost"
PG_PORT = "5432"
PG_DB = "dvdrental"
PG_SCHEMA = "public"

# -------- Tables GOLD --------
GOLD_TABLES = {
    "fact_rental_gold": "s3://gold/fact_rental_gold.parquet",
    "dim_time": "s3://gold/dim_time.parquet",
    "gold_kpi_category": "s3://gold/gold_kpi_category.parquet"
}

# ======================================================
# 2. UTILS
# ======================================================

def convert_array_columns_to_json(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convertit toutes les colonnes contenant des listes ou numpy arrays
    en JSON string pour compatibilité PostgreSQL.
    """
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, (list, np.ndarray))).any():
            df[col] = df[col].apply(
                lambda x: json.dumps(list(x)) if isinstance(x, (list, np.ndarray)) else x
            )
    return df


# ======================================================
# 3. CONNEXIONS
# ======================================================

print("🔌 Connexion à MinIO...")
fs = s3fs.S3FileSystem(
    key=S3_KEY,
    secret=S3_SECRET,
    client_kwargs={"endpoint_url": S3_ENDPOINT}
)

print("🔌 Connexion à PostgreSQL...")
engine = create_engine(
    f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"
)

# ======================================================
# 4. LOAD GOLD → POSTGRES
# ======================================================

for table_name, s3_path in GOLD_TABLES.items():
    print(f"\n📥 Lecture de {table_name} depuis MinIO...")
    
    df = pd.read_parquet(s3_path, filesystem=fs)
    print(f"   ➜ {len(df)} lignes chargées")

    print("🧹 Conversion des colonnes ARRAY → JSON...")
    df = convert_array_columns_to_json(df)

    print(f"📤 Chargement de {table_name} dans PostgreSQL...")
    df.to_sql(
        table_name,
        engine,
        schema=PG_SCHEMA,
        if_exists="replace",   # replace = warehouse refresh
        index=False,
        method="multi",
        chunksize=1000
    )

    print(f"✅ {table_name} chargé avec succès")

print("\n🎉 TOUTES LES TABLES GOLD ONT ÉTÉ CHARGÉES DANS POSTGRES")
