--Revenu par catégorie

DROP MATERIALIZED VIEW IF EXISTS mv_revenue_by_category;

CREATE MATERIALIZED VIEW mv_revenue_by_category AS
SELECT
    category,
    COUNT(*)                    AS total_rentals,
    SUM(rental_rate)            AS total_revenue,
    AVG(rental_rate)            AS avg_rental_rate,
    AVG(actual_rental_duration) AS avg_rental_duration
FROM fact_rental_gold
GROUP BY category;

--Revenu mensuel (time series)

DROP MATERIALIZED VIEW IF EXISTS mv_revenue_monthly;

CREATE MATERIALIZED VIEW mv_revenue_monthly AS
SELECT
    dt.year,
    dt.month,
    COUNT(fr.rental_id) AS total_rentals,
    SUM(fr.rental_rate) AS total_revenue
FROM fact_rental_gold fr
JOIN dim_time dt
    ON fr.rental_date::date = dt.date
GROUP BY dt.year, dt.month;


--Top films par revenu

DROP MATERIALIZED VIEW IF EXISTS mv_top_films;

CREATE MATERIALIZED VIEW mv_top_films AS
SELECT
    title,
    category,
    COUNT(*)            AS total_rentals,
    SUM(rental_rate)    AS total_revenue
FROM fact_rental_gold
GROUP BY
    title,
    category
ORDER BY
    total_revenue DESC;


--KPI globaux (executive view)

DROP MATERIALIZED VIEW IF EXISTS mv_kpi_global;

CREATE MATERIALIZED VIEW mv_kpi_global AS
SELECT
    COUNT(DISTINCT rental_id)        AS total_rentals,
    COUNT(DISTINCT customer_id)      AS total_customers,
    COUNT(DISTINCT film_id)          AS total_films,
    SUM(rental_rate)                 AS total_revenue,
    AVG(actual_rental_duration)      AS avg_rental_duration
FROM fact_rental_gold;


--index

CREATE INDEX idx_mv_category ON mv_revenue_by_category(category);
CREATE INDEX idx_mv_monthly_year_month ON mv_revenue_monthly(year, month);
CREATE INDEX idx_mv_top_films_revenue ON mv_top_films(total_revenue);

