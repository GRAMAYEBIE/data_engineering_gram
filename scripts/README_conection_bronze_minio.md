dvd-data-project/
│
├── scripts/
│   ├── bronze_extract.py        # Extraction des tables PostgreSQL vers MinIO (Bronze layer)
│   └── check_bronze.py          # Vérification de la connexion et lecture des fichiers Bronze
│
├── notebooks/
│   └── exploration.ipynb        # Notebook pour exploration, lecture Bronze, préparation Silver/Gold
│
├── .env                         # Config PostgreSQL et MinIO
├── requirements.txt             # Dépendances Python
└── README.md                    # Documentation en anglais


# DVDRental Project – Bronze Layer & MinIO Setup

## Project Structure

- `scripts/bronze_extract.py`  
  Extracts PostgreSQL tables to MinIO as the Bronze layer (raw data).

- `scripts/check_bronze.py`  
  Test script to connect to MinIO and read Bronze Parquet files.

- `notebooks/`  
  Jupyter notebooks for exploration and testing the Bronze layer.

- `.env`  
  Contains PostgreSQL and MinIO configuration (access keys, endpoint, bucket).

- `requirements.txt`  
  Python dependencies.

- `README.md`  
  This documentation.

---

## Setup Instructions

1. **Create and activate the virtual environment**

```bash
python -m venv venv
venv\Scripts\activate

2. **install dependecies**

```bash
pip install -r requirements.txt


Configure .env with your PostgreSQL and MinIO credentials.

3. **Run the Bronze extraction script:**

```bash
python scripts/bronze_extract.py

This will create the Bronze bucket in MinIO (if it doesn't exist) and upload all tables as Parquet files under s3://bronze/dvdrental/.

4.**Connecting MinIO to Jupyter**

Use the following setup in your notebook to read the Parquet files:

import pandas as pd
import s3fs
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

fs = s3fs.S3FileSystem(
    key=os.getenv("AWS_ACCESS_KEY_ID"),
    secret=os.getenv("AWS_SECRET_ACCESS_KEY"),
    client_kwargs={'endpoint_url': os.getenv("AWS_ENDPOINT_URL")}
)

# Example: read the 'film' table
df = pd.read_parquet(f"s3://{os.getenv('BRONZE_BUCKET')}/dvdrental/film.parquet", filesystem=fs)
print(df.head())


This allows you to explore and verify the raw data directly from Jupyter.

Each table from PostgreSQL is saved as a separate Parquet file in MinIO.

Ensure MinIO is running before running the extraction or accessing files from Jupyter.

Keep the .env file private, it contains sensitive credentials.

This setup forms the Bronze layer, ready for Silver/Gold transformations later.