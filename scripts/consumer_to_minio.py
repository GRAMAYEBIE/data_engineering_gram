import json
import boto3
import pandas as pd
from kafka import KafkaConsumer
from datetime import datetime
from sqlalchemy import create_engine

# ======================================================
# 1. CONFIGURATION
# ======================================================

# -------- MinIO / S3 --------
S3_ENDPOINT = "http://localhost:9000"
S3_KEY = "minioadmin"
S3_SECRET = "minioadmin"
BUCKET_NAME = "bronze"

# -------- PostgreSQL --------
PG_USER = "postgres"
PG_PASSWORD = "postgres"
PG_HOST = "localhost"
PG_PORT = "5432"
PG_DB = "dvdrental"

# -------- Kafka --------
KAFKA_TOPIC = "dvd_rentals"
KAFKA_SERVER = "localhost:9092"

# ======================================================
# 2. INITIALIZATION
# ======================================================

print("🔌 Connecting to MinIO...")
s3_client = boto3.client(
    's3',
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=S3_KEY,
    aws_secret_access_key=S3_SECRET
)

print("🔌 Connecting to PostgreSQL...")
pg_engine = create_engine(f'postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}')

print("🔌 Connecting to Kafka...")
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=[KAFKA_SERVER],
    auto_offset_reset='latest',
    api_version=(0, 10, 1),
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# ======================================================
# 3. STREAMING LOOP
# ======================================================

print(f"\n🚀 Hybrid Consumer started!")
print(f"📡 Listening to topic: '{KAFKA_TOPIC}'...")
print(f"📊 Targets: Postgres (table: fact_rental_gold) & MinIO (bucket: {BUCKET_NAME})")
print("-" * 50)

try:
    for message in consumer:
        data = message.value
        
        # --- STEP 1: Archive to MinIO (Bronze) ---
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        file_key = f"streamed_data/rental_{timestamp}.json"
        
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=file_key,
            Body=json.dumps(data, indent=4)
        )
        
        # --- STEP 2: Direct Insert to Postgres (Gold) ---
        # Convert the single dictionary message to a DataFrame
        df_new = pd.DataFrame([data])
        
        # Use 'append' to add new rows without deleting historical data
        df_new.to_sql('fact_rental_gold', pg_engine, if_exists='append', index=False)
        
        # Success Log
        movie_title = data.get('title', 'Unknown Movie')
        amount = data.get('amount', 0.0)
        print(f"✅ Processed Sale: {movie_title} ({amount}€) | Saved to S3 & Postgres")

except KeyboardInterrupt:
    print("\n🛑 Stopping Consumer...")