# DVDRental Silver Layer Analysis

## Business Context

The DVDRental dataset represents a typical video rental business.  
It contains information about:

- **Customers**: who rents movies
- **Films**: available movies in the store
- **Inventory**: which copy of a film is in which store
- **Stores**: store locations and their staff
- **Rentals**: rental transactions with dates and return times
- **Categories**: film genres for business insights

The main business goal is to **analyze rentals and film performance** in order to better understand:

- Popular movies and categories  
- Store performance  
- Customer behavior  

---

## Silver Layer: Data Preparation

The Silver layer represents **cleaned and enriched data** ready for analysis.  

Key points:

1. **Fact Table** (`fact`):

   - Core transactional data (rentals)
   - Columns include: `rental_id`, `inventory_id`, `customer_id`, `rental_date`, `return_date`, `film_id`, `store_id`, `title`, `rental_rate`, `length`, etc.
   - Enriched with film and inventory information
   - Verified with the `silver_check()` function to ensure key columns are present and row counts are consistent.

2. **Dimension Tables**:

   - **Film Category** (`dim_film_cat`): combines `film_category`, `category`, and `film` to provide film genres for analysis.
   - **Customer** (`dim_customer_silver`): selected customer attributes.
   - **Store** (`dim_store_silver`): store metadata (location and address).

---

## Exploratory Data Analysis (EDA)

We performed EDA on the Silver fact table:

1. **Rental Distribution**  
   Analyzed the number of rentals over time.

2. **Rental Duration**  
   Calculated `rental_duration` in days (`return_date - rental_date`) to understand rental patterns.

3. **Top 10 Films**  
   Identified the most rented films using bar charts.

4. **Store Performance**  
   Compared stores based on the number of rentals.

5. **Top Film Categories**  
   Analyzed which categories are most popular among customers.

All visualizations are produced using **Pandas**, **Matplotlib**, and **Seaborn** to provide business-relevant insights.

---

## Business Insights

- **Most popular films** drive the majority of revenue.  
- **Store 1 vs Store 2** performance can guide staff allocation and promotions.  
- **Category preferences** can inform marketing and stocking decisions.  
- Rental durations help in **predicting inventory demand**.

---

## Workflow Notes

- The **Bronze layer** contains raw Parquet files stored in MinIO.  
- The **Silver layer** is constructed in Pandas with **cleaned and enriched tables**.  
- `silver_check()` ensures the integrity of the fact table before analysis.  
- All further analysis or transformations for the Gold layer (star schema) should be based on this Silver layer.

---

## Usage Example

```python
# Verify Silver fact table
silver_check()

# Explore top rented films
top_films = fact['title'].value_counts().head(10)
sns.barplot(x=top_films.values, y=top_films.index)
plt.title("Top 10 Most Rented Films")
plt.show()
