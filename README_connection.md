This project contains the PostgreSQL DVD Rental sample database and a Docker setup for easy initialization and restoration.

DVD-DATA-PROJECT/

├── postgres/

│   ├── dvdrental.tar         ← PostgreSQL DVD Rental backup

│   └── restore.sql           ← Optional SQL script for restoring

├── docker-compose.yml        ← Docker Compose configuration

└── README.md                 ← This file

## Automatic Restoration with Docker

If the dvdrental.tar file is present in the postgres/ folder, starting the containers will automatically restore the database:

- docker-compose up -d

The initialization process will:

- Create the dvdrental database in PostgreSQL (postgres-source container).

- Restore all tables and data from dvdrental.tar.

## Manual Restoration (Optional)

If automatic restoration does not work, you can restore manually using Docker commands what i did :

# Copy the backup file into the PostgreSQL container

- docker cp postgres/dvdrental.tar postgres-source:/dvdrental.tar

# Drop the database if it exists and create a new one

- docker exec -i -u postgres postgres-source psql -U postgres -c "DROP DATABASE IF EXISTS dvdrental;"

- docker exec -i -u postgres postgres-source psql -U postgres -c "CREATE DATABASE dvdrental;"

# Restore the backup

- docker exec -i -u postgres postgres-source pg_restore --verbose --no-owner --no-privileges -d dvdrental /dvdrental.tar

# Verify tables exist

- docker exec -it -u postgres postgres-source psql -U postgres -d dvdrental -c "\dt"

After this, the database dvdrental will be fully restored with all tables (actor, customer, rental, etc.).

File Format

The backup file should be either:

A TAR archive named dvdrental.tar

Or a ZIP file containing dvdrental.tar

The provided scripts handle both formats automatically

Verifying the Database

You can verify the restoration with a simple SQL query:

- docker exec -it -u postgres postgres-source psql -U postgres -d dvdrental -c "SELECT COUNT(*) FROM customer;"

 count 
-------
   599

Connecting pgAdmin

- Open pgAdmin (usually http://localhost:5050 in your browser).

- Add a new server:

### Host name/address: postgres-source

### Port: 5432

### Username: postgres

### Password: postgres 

- Expand: Databases → dvdrental → Schemas → public → Tables to see all restored tables.

- If tables don’t appear, right-click Tables → Refresh.