import psycopg2
from sqlalchemy import create_engine
import os

# Configuration de la connexion (identique à ton .env)
DB_URL = "postgresql://postgres:postgres@localhost:5432/dvdrental"
SQL_FILE_PATH = "sql/refresh_views.sql"

def refresh_materialized_views():
    print("🚀 Starting Materialized Views Refresh...")
    try:
        # Connexion directe pour exécuter du SQL brut
        conn = psycopg2.connect(DB_URL)
        conn.autocommit = True
        cursor = conn.cursor()

        # Lecture du fichier SQL que tu as créé
        if os.path.exists(SQL_FILE_PATH):
            with open(SQL_FILE_PATH, 'r') as f:
                sql_commands = f.read()
            
            # Exécution du refresh
            cursor.execute(sql_commands)
            print("✅ All Materialized Views have been refreshed successfully!")
        else:
            print(f"❌ Error: {SQL_FILE_PATH} not found.")

        cursor.close()
        conn.close()
    except Exception as e:
        print(f"⚠️ refresh_views failed: {e}")

if __name__ == "__main__":
    refresh_materialized_views()