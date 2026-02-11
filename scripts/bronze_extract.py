"""
Bronze Layer Extraction - dvdrental
-----------------------------------
Extracts raw data from PostgreSQL and uploads it to MinIO (S3 compatible)
as Parquet files. 

Features:
- Adds ingestion metadata
- Dynamically creates the bucket if it does not exist
- One table = one Parquet file
"""

import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
from datetime import datetime, timezone
import s3fs

# ----------------------------
# Load environment variables
# ----------------------------
load_dotenv()

PG_URI = (
    f"postgresql://{os.getenv('PG_USER')}:"
    f"{os.getenv('PG_PASSWORD')}@"
    f"{os.getenv('PG_HOST')}:{os.getenv('PG_PORT')}/"
    f"{os.getenv('PG_DB')}"
)

BRONZE_BUCKET = os.getenv("BRONZE_BUCKET")
BRONZE_PATH = f"s3://{BRONZE_BUCKET}/dvdrental/"

# ----------------------------
# PostgreSQL connection
# ----------------------------
engine = create_engine(PG_URI)

# ----------------------------
# Tables to extract
# ----------------------------
TABLES = [
    "actor", "address", "category", "city", "country", 
    "customer", "film", "film_actor", "film_category",
    "inventory", "language", "payment", "rental", 
    "staff", "store"
]

# ----------------------------
# Check and create bucket
# ----------------------------
fs = s3fs.S3FileSystem()

if not fs.exists(BRONZE_BUCKET):
    print(f"📦 Bucket '{BRONZE_BUCKET}' not found. Creating it...")
    fs.mkdir(BRONZE_BUCKET)
    print(f"✅ Bucket '{BRONZE_BUCKET}' created.")
else:
    print(f"✅ Bucket '{BRONZE_BUCKET}' exists.")

# ----------------------------
# Extraction function
# ----------------------------
def extract_table(table_name: str):
    print(f"📥 Extracting table: {table_name}")

    # Read table from PostgreSQL
    df = pd.read_sql(f"SELECT * FROM {table_name}", engine)

    # Add metadata
    df["_ingestion_timestamp"] = datetime.now(timezone.utc)
    df["_source_system"] = "postgres.dvdrental"

    # Output path
    output_path = f"{BRONZE_PATH}{table_name}.parquet"

    # Save as Parquet to MinIO
    df.to_parquet(
        output_path,
        engine="pyarrow",
        index=False
    )

    print(f"✅ Saved to {output_path}")


# ----------------------------
# Main function
# ----------------------------
def main():
    print("🚀 Starting Bronze extraction")

    for table in TABLES:
        extract_table(table)

    print("🎉 Bronze layer successfully created in MinIO")


if __name__ == "__main__":
    main()
