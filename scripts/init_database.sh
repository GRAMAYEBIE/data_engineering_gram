#!/bin/bash

# Initialization script that runs when PostgreSQL container starts
# Handles automatic database restoration from ZIP backup file

set -e

BACKUP_ZIP="/docker-entrypoint-initdb.d/backups/dvdrental.zip"
BACKUP_TAR="/docker-entrypoint-initdb.d/backups/dvdrental.tar"
EXTRACT_DIR="/docker-entrypoint-initdb.d/backups"

echo "=========================================="
echo "PostgreSQL Initialization Script"
echo "=========================================="
echo ""

# Wait for PostgreSQL to be ready
until pg_isready -U postgres; do
    echo "Waiting for PostgreSQL to be ready..."
    sleep 2
done

echo "✓ PostgreSQL is ready"

# Check if database already exists
DB_EXISTS=$(psql -U postgres -tAc "SELECT 1 FROM pg_database WHERE datname='dvdrental'" 2>/dev/null || echo "0")

if [ "$DB_EXISTS" = "1" ]; then
    echo "✓ Database 'dvdrental' already exists, skipping restoration"
    exit 0
fi

# Check for backup files (ZIP or TAR)
if [ -f "$BACKUP_ZIP" ]; then
    echo "Step 1: Extracting ZIP backup file..."
    if command -v unzip &> /dev/null; then
        unzip -q -o "$BACKUP_ZIP" -d "$EXTRACT_DIR"
        echo "✓ ZIP file extracted"
    else
        echo "⚠ unzip not available, trying to use existing TAR file"
    fi
fi

# Check if TAR file exists (either extracted or already present)
if [ ! -f "$BACKUP_TAR" ]; then
    echo "⚠ Backup file not found: $BACKUP_TAR"
    echo "  Database will be created but not restored."
    echo "  Please ensure dvdrental.zip or dvdrental.tar is in the backups/ directory"
    echo "  Or run ./scripts/restore_dvd_rental.sh manually after container starts"
    psql -U postgres -c "CREATE DATABASE dvdrental;" || true
    exit 0
fi

echo "Step 2: Creating database..."
psql -U postgres -c "CREATE DATABASE dvdrental;"
echo "✓ Database created"

echo "Step 3: Restoring database from backup..."
echo "  This may take a few minutes..."

pg_restore -U postgres -d dvdrental -v "$BACKUP_TAR" || {
    echo "⚠ Restoration encountered issues, but continuing..."
    echo "  You can try manual restoration: ./scripts/restore_dvd_rental.sh"
}

echo "✓ Initialization complete"
