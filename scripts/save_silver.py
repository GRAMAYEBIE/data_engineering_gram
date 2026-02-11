"""
save_silver.py
---------------
This script:
1️⃣ Loads Bronze tables from MinIO
2️⃣ Transforms them into a Silver layer (cleaned and enriched)
3️⃣ Saves Silver tables locally (silver_save/)
4️⃣ Uploads Silver tables to MinIO (bucket: silver)
"""

import os
import pandas as pd
import s3fs
from dotenv import load_dotenv

# -------------------------------
# Load environment variables
# -------------------------------
load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
AWS_ENDPOINT_URL = os.getenv("AWS_ENDPOINT_URL", "http://localhost:9000")

# -------------------------------
# Connect to MinIO
# -------------------------------
fs = s3fs.S3FileSystem(
    key=AWS_ACCESS_KEY_ID,
    secret=AWS_SECRET_ACCESS_KEY,
    client_kwargs={'endpoint_url': AWS_ENDPOINT_URL}
)

# -------------------------------
# Create local folder for Silver
# -------------------------------
os.makedirs("silver_save", exist_ok=True)

# -------------------------------
# Load Bronze tables from MinIO
# -------------------------------
print("📥 Loading Bronze tables from MinIO...")

tables = {}
bronze_path = "s3://bronze/dvdrental/"

for f in fs.ls(bronze_path):
    table_name = f.split("/")[-1].replace(".parquet", "")
    tables[table_name] = pd.read_parquet(f, filesystem=fs)
    print(f"✅ Loaded '{table_name}' ({tables[table_name].shape[0]} rows)")

# -------------------------------
# Transform Film Table to Silver
# -------------------------------
def transform_to_silver(tables_dict):
    df = tables_dict['film_category'].merge(tables_dict['category'], on='category_id', how='left') \
                                     .merge(tables_dict['film'], on='film_id', how='left')
    
    cols_to_keep = [
        'film_id', 'title', 'name', 'release_year', 'rental_duration', 
        'rental_rate', 'length', 'replacement_cost', 'rating', 'special_features'
    ]
    
    df_silver = df[cols_to_keep].copy()
    df_silver.rename(columns={'name': 'category'}, inplace=True)
    
    # Clean types
    df_silver['title'] = df_silver['title'].str.title()
    df_silver['category'] = df_silver['category'].astype('category')
    
    return df_silver

df_silver_film = transform_to_silver({
    'film_category': tables['film_category'],
    'category': tables['category'],
    'film': tables['film']
})

# -------------------------------
# Enrich Fact Table
# -------------------------------
fact_rental = tables['rental'].merge(
    tables['inventory'][['inventory_id', 'film_id', 'store_id']],
    on='inventory_id',
    how='left'
)

df_fact_silver = fact_rental.merge(
    df_silver_film[['film_id', 'title', 'category', 'rental_rate', 'replacement_cost']],
    on='film_id',
    how='left'
)

# Calculate business metrics
df_fact_silver['rental_yield'] = (df_fact_silver['rental_rate'] / df_fact_silver['replacement_cost']) * 100
df_fact_silver['rental_date'] = pd.to_datetime(df_fact_silver['rental_date'])
df_fact_silver['return_date'] = pd.to_datetime(df_fact_silver['return_date'])
df_fact_silver['actual_rental_duration'] = (df_fact_silver['return_date'] - df_fact_silver['rental_date']).dt.days

# -------------------------------
# Save Silver locally
# -------------------------------
df_fact_silver.to_parquet("silver_save/fact_rental.parquet", index=False)
df_silver_film.to_parquet("silver_save/dim_film.parquet", index=False)
print("✅ Silver tables saved locally in 'silver_save/'")

# -------------------------------
# Save Silver in MinIO
# -------------------------------
if not fs.exists("silver"):
    fs.mkdir("silver")

df_fact_silver.to_parquet("s3://silver/fact_rental.parquet", filesystem=fs, index=False)
df_silver_film.to_parquet("s3://silver/dim_film.parquet", filesystem=fs, index=False)
print("✅ Silver tables uploaded to MinIO bucket 'silver'")

print("🎉 Silver layer is ready for Gold layer creation!")
