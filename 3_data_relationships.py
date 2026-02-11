import pandas as pd
import s3fs
import os
from dotenv import load_dotenv

# 1. Setup
load_dotenv()
fs = s3fs.S3FileSystem(
    key=os.getenv("AWS_ACCESS_KEY_ID"),
    secret=os.getenv("AWS_SECRET_ACCESS_KEY"),
    client_kwargs={'endpoint_url': os.getenv("AWS_ENDPOINT_URL")}
)

BRONZE_PATH = f"s3://{os.getenv('BRONZE_BUCKET')}/dvdrental/"
SILVER_EXPORT_PATH = "exports/tables/"

def build_silver_layer():
    print("="*60)
    print("STEP 3: BUILDING DATA RELATIONSHIPS (SILVER LAYER)")
    print("="*60)

    # ensure export directory exists
    os.makedirs(SILVER_EXPORT_PATH, exist_ok=True)

    try:
        # 2. Loading Core Tables
        print("📥 Loading tables from Bronze (MinIO)...")
        rental = pd.read_parquet(f"{BRONZE_PATH}rental.parquet", filesystem=fs)
        inventory = pd.read_parquet(f"{BRONZE_PATH}inventory.parquet", filesystem=fs)
        film = pd.read_parquet(f"{BRONZE_PATH}film.parquet", filesystem=fs)
        customer = pd.read_parquet(f"{BRONZE_PATH}customer.parquet", filesystem=fs)

        # 3. Data Cleaning & Handling Missing Values
        print("🧹 Cleaning data and handling NULLs...")
        
        # Convert dates
        rental['rental_date'] = pd.to_datetime(rental['rental_date'])
        rental['return_date'] = pd.to_datetime(rental['return_date'])
        
        # Logic: If return_date is missing, it means the movie is still out.
        # Imputing with rental_date + 7 days to allow duration calculations.
        null_count = rental['return_date'].isnull().sum()
        rental['return_date'] = rental['return_date'].fillna(rental['rental_date'] + pd.Timedelta(days=7))
        print(f"✔️ Imputed {null_count} missing return dates.")

        # 4. Creating Relationships (The "Star" Merge)
        print("🔗 Merging tables (Rental -> Inventory -> Film -> Customer)...")
        
        # Fact Table creation
        silver_rentals = rental.merge(inventory, on='inventory_id', how='left')
        silver_rentals = silver_rentals.merge(film, on='film_id', how='left')
        
        # Adding customer details for richer analysis
        customer_cols = ['customer_id', 'first_name', 'last_name', 'email']
        silver_rentals = silver_rentals.merge(customer[customer_cols], on='customer_id', how='left')

        # 5. Exporting to Local "Silver" Storage
        print(f"💾 Exporting Silver tables to {SILVER_EXPORT_PATH}...")
        
        # Save the main fact table (Gold ready)
        silver_rentals.to_parquet(f"{SILVER_EXPORT_PATH}fact_rentals.parquet", index=False)
        
        # Save cleaned dimension table
        film.to_parquet(f"{SILVER_EXPORT_PATH}dim_film.parquet", index=False)

        print("\n✅ Silver Layer Created Successfully!")
        print(f"📊 Total Merged Records: {len(silver_rentals)}")
        print("="*60)

    except Exception as e:
        print(f"❌ Error during Silver layer build: {e}")
        print("💡 Tip: Make sure Docker is running and Bronze data exists in MinIO.")

if __name__ == "__main__":
    build_silver_layer()