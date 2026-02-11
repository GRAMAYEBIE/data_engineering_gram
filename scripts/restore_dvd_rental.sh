#!/bin/bash

# Script to restore DVD Rental database from existing ZIP backup file
# Assumes dvdrental.zip is already available in the backups/ directory

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BACKUP_ZIP="$PROJECT_ROOT/backups/dvdrental.zip"
BACKUP_TAR="$PROJECT_ROOT/backups/dvdrental.tar"

echo "=========================================="
echo "DVD Rental Database Restoration Script"
echo "=========================================="
echo ""

# Check if ZIP file exists
if [ ! -f "$BACKUP_ZIP" ]; then
    echo "Error: Backup ZIP file not found: $BACKUP_ZIP"
    echo ""
    echo "Please ensure dvdrental.zip is in the backups/ directory"
    echo "Expected location: $BACKUP_ZIP"
    exit 1
fi

echo "Step 1: Extracting ZIP file..."
if [ ! -f "$BACKUP_TAR" ]; then
    if command -v unzip &> /dev/null; then
        echo "  Extracting $BACKUP_ZIP..."
        unzip -q -o "$BACKUP_ZIP" -d "$PROJECT_ROOT/backups/"
        echo "✓ Extraction completed"
    else
        echo "Error: unzip is not available. Please install unzip or extract manually."
        exit 1
    fi
else
    echo "✓ TAR file already exists, skipping extraction"
fi

# Verify TAR file exists
if [ ! -f "$BACKUP_TAR" ]; then
    echo "Error: TAR file not found after extraction: $BACKUP_TAR"
    echo "Please check the ZIP file contents"
    exit 1
fi

echo "Step 2: Checking PostgreSQL container status..."
if ! docker ps | grep -q "day8-postgres"; then
    echo "Error: PostgreSQL container is not running"
    echo "Please start the containers first: docker-compose up -d"
    exit 1
fi

echo "✓ PostgreSQL container is running"

echo "Step 3: Checking if database already exists..."
DB_EXISTS=$(docker exec day8-postgres psql -U postgres -tAc "SELECT 1 FROM pg_database WHERE datname='dvdrental'" 2>/dev/null || echo "0")

if [ "$DB_EXISTS" = "1" ]; then
    echo "⚠ Database 'dvdrental' already exists"
    read -p "Do you want to drop and recreate it? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        echo "Dropping existing database..."
        docker exec day8-postgres psql -U postgres -c "DROP DATABASE IF EXISTS dvdrental;"
        echo "✓ Database dropped"
    else
        echo "Restoration cancelled. Existing database preserved."
        exit 0
    fi
fi

echo "Step 4: Creating database..."
docker exec day8-postgres psql -U postgres -c "CREATE DATABASE dvdrental;"
echo "✓ Database created"

echo "Step 5: Restoring database from backup..."
echo "  This may take a few minutes..."

# Copy backup file to container
docker cp "$BACKUP_TAR" day8-postgres:/tmp/dvdrental.tar

# Restore the database
docker exec day8-postgres pg_restore -U postgres -d dvdrental -v /tmp/dvdrental.tar || {
    echo "Error: Restoration failed"
    docker exec day8-postgres rm -f /tmp/dvdrental.tar
    exit 1
}

# Clean up
docker exec day8-postgres rm -f /tmp/dvdrental.tar

echo "✓ Database restoration completed"

echo "Step 6: Verifying restoration..."
TABLE_COUNT=$(docker exec day8-postgres psql -U postgres -d dvdrental -tAc "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public';" 2>/dev/null || echo "0")
echo "✓ Found $TABLE_COUNT tables in the database"

echo ""
echo "=========================================="
echo "✓ DVD Rental database restoration complete!"
echo "=========================================="
echo ""
echo "Database connection details:"
echo "  Host: localhost"
echo "  Port: 5432"
echo "  Database: dvdrental"
echo "  Username: postgres"
echo "  Password: postgres"
echo ""
echo "You can now:"
echo "1. Connect via psql: psql -h localhost -U postgres -d dvdrental"
echo "2. Access PGAdmin at: http://localhost:5050"
echo "3. Start querying the database!"
