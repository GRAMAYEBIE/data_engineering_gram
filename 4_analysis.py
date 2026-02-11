import pandas as pd
import os

# Paths
INPUT_PATH = "exports/tables/fact_rentals.parquet"
OUTPUT_FOLDER = "exports/results/"

def run_business_analysis():
    print("="*65)
    print("STEP 4: ADVANCED BUSINESS ANALYSIS & KPI GENERATION")
    print("="*65)

    # 1. Create results folder if it doesn't exist
    if not os.path.exists(OUTPUT_FOLDER):
        os.makedirs(OUTPUT_FOLDER)

    # 2. Load the Silver data (from Step 3)
    if not os.path.exists(INPUT_PATH):
        print(f"❌ Error: {INPUT_PATH} not found. Please run Step 3 first!")
        return

    df = pd.read_parquet(INPUT_PATH)
    print(f"✅ Data loaded: {len(df)} transactions available for analysis.")

    # --- KPI 1: TOP 10 MOST POPULAR FILMS ---
    print("\n📊 Computing Top 10 Most Rented Films...")
    top_films = df['title'].value_counts().head(10).reset_index()
    top_films.columns = ['film_title', 'rental_count']
    top_films.to_csv(f"{OUTPUT_FOLDER}top_10_popular_films.csv", index=False)

    # --- KPI 2: REVENUE BY CATEGORY ---
    # We use rental_rate as a proxy for revenue per rental
    if 'rental_rate' in df.columns:
        print("💰 Computing Revenue by Film Rating...")
        revenue_dist = df.groupby('rating')['rental_rate'].sum().sort_values(ascending=False).reset_index()
        revenue_dist.to_csv(f"{OUTPUT_FOLDER}revenue_by_rating.csv", index=False)

    # --- KPI 3: RENTAL DURATION ANALYSIS ---
    print("⏱️  Computing Average Rental Duration...")
    # Calculate duration in days
    df['duration_days'] = (df['return_date'] - df['rental_date']).dt.days
    avg_duration = df['duration_days'].mean()
    
    # --- KPI 4: TOP 5 CUSTOMERS (REVENUE) ---
    print("💎 Identifying Top 5 High-Value Customers...")
    top_customers = df.groupby(['first_name', 'last_name', 'email'])['rental_rate'].sum().sort_values(ascending=False).head(5).reset_index()
    top_customers.to_csv(f"{OUTPUT_FOLDER}top_5_customers.csv", index=False)
    
    
    # Save a small summary text file
    with open(f"{OUTPUT_FOLDER}general_metrics.txt", "w") as f:
        f.write(f"Total Rentals: {len(df)}\n")
        f.write(f"Average Rental Duration: {avg_duration:.2f} days\n")

    print(f"\n✅ Analysis complete! 3 files generated in {OUTPUT_FOLDER}")
    print("="*65)

if __name__ == "__main__":
    run_business_analysis()