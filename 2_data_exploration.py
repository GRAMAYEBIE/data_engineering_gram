import pandas as pd
from sqlalchemy import create_engine
import numpy as np

# Connexion à la base de données restaurée
# Adapte le host si tu es sur Docker (localhost ou postgres)
DB_URL = 'postgresql://postgres:postgres@localhost:5432/dvdrental'
engine = create_engine(DB_URL)

def run_consistent_eda():
    print("="*70)
    print("🚀 STEP 2: ADVANCED EXPLORATORY DATA ANALYSIS (SOURCE AUDIT)")
    print("="*70)

    # 1. ANALYSE FINANCIÈRE (Table Payment)
    print("\n💰 [FINANCE] Revenue & Pricing Analysis")
    try:
        df_pay = pd.read_sql("SELECT amount, payment_date FROM payment", engine)
        stats = {
            "Total Revenue": f"{df_pay['amount'].sum():,.2f} $",
            "Average Ticket": f"{df_pay['amount'].mean():.2f} $",
            "Max Transaction": f"{df_pay['amount'].max():.2f} $",
            "Zero-Value Payments": len(df_pay[df_pay['amount'] <= 0])
        }
        for k, v in stats.items():
            print(f"  - {k}: {v}")
    except Exception as e: print(f"  ❌ Error on Payment: {e}")

    # 2. ANALYSE DU CATALOGUE (Table Film)
    print("\n🎬 [CATALOG] Inventory & Diversity")
    try:
        query_film = """
        SELECT rating, rental_rate, replacement_cost, length 
        FROM film
        """
        df_film = pd.read_sql(query_film, engine)
        print(f"  - Average Movie Length: {df_film['length'].mean():.1f} min")
        print(f"  - Replacement Cost Risk: {df_film['replacement_cost'].sum():,.0f} $ (Total Asset Value)")
        print(f"  - Rating Distribution:\n{df_film['rating'].value_counts(normalize=True).mul(100).round(1).astype(str) + '%'}")
    except Exception as e: print(f"  ❌ Error on Film: {e}")

    # 3. ANALYSE COMPORTEMENTALE (Table Rental)
    print("\n👤 [BEHAVIOR] Rental Dynamics")
    try:
        query_rental = """
        SELECT count(*) as total, 
               extract(dow from rental_date) as day_of_week
        FROM rental 
        GROUP BY day_of_week
        """
        df_rent = pd.read_sql(query_rental, engine)
        # 0 = Sunday, 6 = Saturday
        peak_day = df_rent.loc[df_rent['total'].idxmax(), 'day_of_week']
        print(f"  - Peak Activity Day: Day {int(peak_day)} (Weekly cycle detected)")
    except Exception as e: print(f"  ❌ Error on Rental: {e}")

    # 4. DATA INTEGRITY CHECK (Audit Global)
    print("\n🛡️ [INTEGRITY] Global Quality Audit")
    tables = ["customer", "address", "city", "country"]
    for t in tables:
        count = pd.read_sql(f"SELECT count(*) FROM {t}", engine).iloc[0,0]
        nulls = pd.read_sql(f"SELECT count(*) FROM {t} WHERE {t}_id IS NULL", engine).iloc[0,0]
        status = "✅ CLEAN" if nulls == 0 else "⚠️ WARNING (Nulls detected)"
        print(f"  - Table {t:10} | Records: {count:4} | Status: {status}")

    print("\n" + "="*70)
    print("✅ CONSISTENT EDA COMPLETED: Data source is ready for Pipeline.")
    print("="*70)

if __name__ == "__main__":
    run_consistent_eda()