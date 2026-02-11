#!/bin/bash

# Script to verify that all tables are in the same dvdrental database

set -e

echo "=========================================="
echo "Database Consistency Verification"
echo "=========================================="
echo ""

# Check if containers are running
if ! docker ps | grep -q "day8-postgres"; then
    echo "Error: PostgreSQL container is not running"
    echo "Please start containers: docker-compose up -d"
    exit 1
fi

echo "Checking tables in dvdrental database..."
echo ""

# List all tables
TABLES=$(docker exec day8-postgres psql -U postgres -d dvdrental -tAc "
SELECT table_name 
FROM information_schema.tables 
WHERE table_schema = 'public'
ORDER BY table_name;
")

if [ -z "$TABLES" ]; then
    echo "⚠ No tables found in dvdrental database"
    echo "  The database may not be restored yet."
    echo "  Run: ./scripts/restore_dvd_rental.sh"
    exit 1
fi

echo "Found tables in dvdrental database:"
echo "$TABLES" | nl

echo ""
echo "DVD Rental tables (expected):"
DVD_TABLES=$(echo "$TABLES" | grep -E "^(customer|film|rental|payment|inventory|category|actor|staff|store)$" || true)
if [ -n "$DVD_TABLES" ]; then
    echo "$DVD_TABLES" | nl
else
    echo "  ⚠ No DVD Rental tables found"
fi

echo ""
echo "CSV imported tables (if any):"
CSV_TABLES=$(echo "$TABLES" | grep -vE "^(customer|film|rental|payment|inventory|category|actor|staff|store|film_category|film_actor|address|city|country|language)$" || true)
if [ -n "$CSV_TABLES" ]; then
    echo "$CSV_TABLES" | nl
    echo ""
    echo "✓ CSV tables are in the same database as DVD Rental data"
else
    echo "  (No CSV imported tables found yet)"
    echo ""
    echo "To import CSV files, use:"
    echo "  python scripts/create_tables_from_csv.py data/ --batch --database dvdrental"
fi

echo ""
echo "=========================================="
echo "Database: dvdrental"
echo "Total tables: $(echo "$TABLES" | wc -l | tr -d ' ')"
echo "=========================================="

