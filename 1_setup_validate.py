import pandas as pd
from sqlalchemy import create_engine, inspect
import datetime

# 1. Connection to PostgreSQL
DB_PARAMS = {
    "user": "postgres",
    "password": "postgres",
    "host": "127.0.0.1",
    "port": "5432",
    "database": "dvdrental"
}

def run_initial_validation():
    conn_str = f"postgresql://{DB_PARAMS['user']}:{DB_PARAMS['password']}@{DB_PARAMS['host']}:{DB_PARAMS['port']}/{DB_PARAMS['database']}"
    engine = create_engine(conn_str)
    
    print("="*60)
    print(f"PART B: INITIAL DATABASE VALIDATION REPORT - {datetime.datetime.now().strftime('%Y-%m-%d')}")
    print("="*60)

    # 2. List all available tables
    inspector = inspect(engine)
    all_tables = inspector.get_table_names()
    print(f"\n[STEP 1 & 2] Connection Successful. Found {len(all_tables)} tables.")
    print(f"Tables List: {', '.join(all_tables)}")

    # 3. Validate existence of critical tables
    critical_tables = ['rental', 'payment', 'inventory', 'film', 'customer', 'staff', 'store']
    found = [t for t in critical_tables if t in all_tables]
    missing = [t for t in critical_tables if t not in all_tables]
    
    print(f"\n[STEP 3] Critical Table Validation:")
    if not missing:
        print("✅ SUCCESS: All critical source tables are present.")
    else:
        print(f"❌ WARNING: Missing source tables: {missing}")

    # 4. Report row counts
    print(f"\n[STEP 4] Row Counts (Source Data Volume):")
    row_data = []
    for table in found:
        count = pd.read_sql(f"SELECT COUNT(*) FROM {table}", engine).iloc[0, 0]
        row_data.append({"Table Name": table, "Total Rows": count})
    
    df_rows = pd.DataFrame(row_data)
    print(df_rows.to_string(index=False))

    # 5. Identify data quality issues
    print(f"\n[STEP 5] Data Quality Issues Identification:")
    if 'rental' in all_tables:
        # Check for missing return dates
        df_rental = pd.read_sql("SELECT rental_date, return_date FROM rental", engine)
        nulls = df_rental['return_date'].isnull().sum()
        print(f"- Missing Data: {nulls} records in 'rental' have no 'return_date' (Active rentals).")
        # Check for date consistency
        print(f"- Range Issue: Dataset contains historical data from {df_rental['rental_date'].min().year}.")
    
    # 6. Produce structured validation report (CSV)
    df_rows.to_csv("initial_validation_report.csv", index=False)
    print(f"\n[STEP 6] Structured report saved as: initial_validation_report.csv")
    print("="*60)

if __name__ == "__main__":
    run_initial_validation()