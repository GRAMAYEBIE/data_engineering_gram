#!/bin/bash

# Script to load CSV files into PostgreSQL using PGAdmin's import feature
# This script copies CSV files into the container and provides instructions

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
DATA_DIR="$PROJECT_ROOT/data"

echo "=========================================="
echo "CSV Import via PGAdmin Guide"
echo "=========================================="
echo ""

# Check if containers are running
if ! docker ps | grep -q "day8-postgres"; then
    echo "Error: PostgreSQL container is not running"
    echo "Please start containers: docker-compose up -d"
    exit 1
fi

if ! docker ps | grep -q "day8-pgadmin"; then
    echo "Error: PGAdmin container is not running"
    echo "Please start containers: docker-compose up -d"
    exit 1
fi

echo "✓ Containers are running"
echo ""

# Check for CSV files
if [ ! -d "$DATA_DIR" ] || [ -z "$(ls -A $DATA_DIR/*.csv 2>/dev/null)" ]; then
    echo "⚠ No CSV files found in $DATA_DIR"
    echo "  Place your CSV files in the data/ directory"
    exit 1
fi

echo "Found CSV files:"
ls -lh "$DATA_DIR"/*.csv | awk '{print "  - " $9 " (" $5 ")"}'
echo ""

echo "=========================================="
echo "Method 1: Using PGAdmin Web Interface"
echo "=========================================="
echo ""
echo "1. Open PGAdmin: http://localhost:5050"
echo "   Login: admin@example.com / admin"
echo ""
echo "2. Connect to server 'Day8 PostgreSQL'"
echo ""
echo "3. Navigate to: Servers → Day8 PostgreSQL → Databases → dvdrental → Schemas → public"
echo ""
echo "4. Right-click on 'Tables' → Create → Table"
echo ""
echo "5. After creating table structure, right-click on the table → Import/Export Data"
echo ""
echo "6. In the Import dialog:"
echo "   - Filename: Use the file browser or enter: /data/your_file.csv"
echo "   - Format: CSV"
echo "   - Header: Yes (if your CSV has headers)"
echo "   - Delimiter: ,"
echo "   - Quote: \""
echo ""
echo "=========================================="
echo "Method 2: Using SQL COPY Command in PGAdmin"
echo "=========================================="
echo ""
echo "1. Open PGAdmin Query Tool"
echo "2. Run the following SQL (replace table_name and filename):"
echo ""
echo "COPY table_name FROM '/data/customers.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',');"
echo ""
echo "Note: Files are mounted at /data/ inside the container"
echo ""
echo "=========================================="
echo "Method 3: Using psql from Host Machine"
echo "=========================================="
echo ""
echo "You can also use the create_tables_from_csv.py script:"
echo ""
echo "  python scripts/create_tables_from_csv.py data/customers.csv customers --database dvdrental"
echo ""
echo "Or import all CSV files:"
echo ""
echo "  python scripts/create_tables_from_csv.py data/ --batch --database dvdrental"
echo ""
echo "=========================================="

