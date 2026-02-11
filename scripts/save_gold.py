import pandas as pd
import s3fs

print("🚀 Starting GOLD layer creation...")

# ----------------------------
# MinIO config
# ----------------------------
fs = s3fs.S3FileSystem(
    key="minioadmin",
    secret="minioadmin",
    client_kwargs={"endpoint_url": "http://localhost:9000"},
)

SILVER_BUCKET = "silver"
GOLD_BUCKET = "gold"

# ----------------------------
# Ensure GOLD bucket exists
# ----------------------------
if not fs.exists(GOLD_BUCKET):
    print(f"🪣 Bucket '{GOLD_BUCKET}' not found. Creating it...")
    fs.mkdir(GOLD_BUCKET)
else:
    print(f"🪣 Bucket '{GOLD_BUCKET}' already exists")

# ----------------------------
# Load SILVER tables
# ----------------------------
print("📥 Loading Silver tables from MinIO...")

fact_rental = pd.read_parquet(
    f"s3://{SILVER_BUCKET}/fact_rental.parquet",
    filesystem=fs
)

dim_film = pd.read_parquet(
    f"s3://{SILVER_BUCKET}/dim_film.parquet",
    filesystem=fs
)

print("✅ Silver tables loaded")
print("fact_rental shape:", fact_rental.shape)
print("dim_film shape:", dim_film.shape)

# ----------------------------
# Build TIME dimension
# ----------------------------
print("🕒 Building Time dimension...")

dim_time = (
    fact_rental[['rental_date']]
    .dropna()
    .drop_duplicates()
    .assign(
        date=lambda df: df['rental_date'].dt.date,
        year=lambda df: df['rental_date'].dt.year,
        month=lambda df: df['rental_date'].dt.month,
        day=lambda df: df['rental_date'].dt.day,
        day_of_week=lambda df: df['rental_date'].dt.day_name(),
        week=lambda df: df['rental_date'].dt.isocalendar().week
    )
    .drop(columns=['rental_date'])
    .reset_index(drop=True)
)

print("✅ dim_time created")

# ----------------------------
# Build FACT_RENTAL_GOLD
# ----------------------------
print("📊 Building fact_rental_gold...")

fact_rental_gold = fact_rental.merge(
    dim_film,
    on="film_id",
    how="left",
    suffixes=("", "_film")
)

# Normalize column names
if "rental_rate" not in fact_rental_gold.columns and "rental_rate_film" in fact_rental_gold.columns:
    fact_rental_gold["rental_rate"] = fact_rental_gold["rental_rate_film"]

if "category" not in fact_rental_gold.columns and "category_film" in fact_rental_gold.columns:
    fact_rental_gold["category"] = fact_rental_gold["category_film"]

required_cols = {
    "film_id",
    "category",
    "rental_rate",
    "length",
    "replacement_cost"
}

missing = required_cols - set(fact_rental_gold.columns)
if missing:
    raise ValueError(f"❌ Missing columns in fact_rental_gold: {missing}")

print("✅ fact_rental_gold created")
print("fact_rental_gold shape:", fact_rental_gold.shape)

# ----------------------------
# Business Aggregations (GOLD)
# ----------------------------
print("📈 Creating aggregated business metrics...")

gold_kpi_category = (
    fact_rental_gold
    .groupby("category", dropna=False)
    .agg(
        total_rentals=("rental_id", "count"),
        avg_rental_rate=("rental_rate", "mean"),
        avg_film_length=("length", "mean"),
        avg_replacement_cost=("replacement_cost", "mean")
    )
    .reset_index()
    .round(2)
)

print("✅ gold_kpi_category created")

# ----------------------------
# Save GOLD to MinIO
# ----------------------------
print("💾 Saving GOLD layer to MinIO...")

fact_rental_gold.to_parquet(
    f"s3://{GOLD_BUCKET}/fact_rental_gold.parquet",
    filesystem=fs,
    index=False
)

dim_time.to_parquet(
    f"s3://{GOLD_BUCKET}/dim_time.parquet",
    filesystem=fs,
    index=False
)

gold_kpi_category.to_parquet(
    f"s3://{GOLD_BUCKET}/gold_kpi_category.parquet",
    filesystem=fs,
    index=False
)

print("🎉 GOLD layer successfully created and stored in MinIO!")
