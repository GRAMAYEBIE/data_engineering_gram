-- setup.sql
-- Ce script initialise la base de données dvdrental au premier lancement de Docker

-- 1. Création de la table 'Gold' pour le streaming (Fact Table)
CREATE TABLE IF NOT EXISTS public.fact_rental_gold (
    rental_id SERIAL PRIMARY KEY,
    rental_date TIMESTAMP,
    return_date TIMESTAMP,
    title VARCHAR(255),
    amount DECIMAL(10,2),
    customer_name VARCHAR(255),
    category VARCHAR(100),
    store_id INT
);

-- 2. Création d'une table Dimension Temps (très utile pour les graphiques Power BI)
CREATE TABLE IF NOT EXISTS public.dim_time (
    time_key TIMESTAMP PRIMARY KEY,
    hour INT,
    day INT,
    month INT,
    year INT,
    weekday VARCHAR(20)
);

-- 3. Insertion d'une ligne de test pour vérifier que la table fonctionne
INSERT INTO public.fact_rental_gold (rental_date, title, amount, customer_name, category)
VALUES (CURRENT_TIMESTAMP, 'System Boot Check', 0.00, 'System', 'Setup');

-- 4. Attribution des droits (pour éviter les erreurs de connexion)
ALTER TABLE public.fact_rental_gold OWNER TO postgres;