# ETL-CSV-DATALOADER

This project implements a simple ETL tool in C#. It imports specific columns from a CSV file into a single, flat MS SQL table. It uses bulk insertion for efficiency, detects and logs duplicate records, handles invalid rows, and converts times from EST to UTC.

## Overview

- **Technologies**: .NET (C#), MS SQL Server, Docker/Docker Compose.
- **Goal**: Create a CLI-based ETL pipeline that reads from a CSV, cleans and inserts the data into SQL Server, and logs duplicates/invalid rows.
- **Targeted Queries**:
  - Highest average tip by `PULocationID`.
  - Top 100 fares by `trip_distance`.
  - Top 100 fares by trip duration (computed as a persisted column).
  - Queries filtering by `PULocationID`.

## Key Points

1. **Used .NET Core**  
   Ensures cross-platform compatibility. The code can run on any OS that supports .NET Core.

2. **Table Schema**  
   - Columns stored:  
     `tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, store_and_fwd_flag, PULocationID, DOLocationID, fare_amount, tip_amount`  
   - Additional computed column `TripDurationMinutes` to optimize queries for “longest fares in terms of time.”

3. **Database Reset in Migration**  
   The database is dropped and recreated each time the migration script runs, ensuring a clean, reproducible state.

4. **Separate Docker for Migration**  
   The schema is created using a dedicated migration container so that the ETL container does not need an ORM or direct SQL migrations.

5. **Efficient Bulk Insert**  
   The ETL uses `SqlBulkCopy` to insert records in batches for performance. 

6. **Duplicate Detection**  
   Duplicate rows are identified by `(pickup_datetime, dropoff_datetime, passenger_count)` and written into `duplicates.csv`.

7. **Invalid Rows**  
   Each row that cannot be parsed or fails validation is added to `invalid_rows.csv`. No default values or silent ignores are used.

8. **Conversion to UTC**  
   The input data is originally in EST. The program converts all datetime columns to UTC before inserting.

9. **Handling Large Files**  
   For very large data files (e.g. 10GB), we would switch to batch or streaming reads (avoiding memory overflows), and possibly adjust the approach to handle duplicates with a staging table or partial indexing.

10. **Environment Variables**  
   All environment variables (like `CONNECTION_STRING`, `SA_PASSWORD`) are stored in a `.env` file and read by Docker Compose.

## How It Works

1. **Docker Compose**  
   - `db`: Runs MS SQL Server 2019.  
   - `migrate`: Executes the SQL script to drop/create the database and build the table schema.  
   - `etl`: Runs the .NET CLI ETL tool (`Program.Main`), reads CSV from the `Data` folder, and inserts into the migrated database.

2. **ETL Execution**  
   - Reads CSV line by line, only storing required columns.  
   - Strips whitespace from text fields.  
   - Converts `store_and_fwd_flag`: `N -> No`, `Y -> Yes`.  
   - Converts times from EST to UTC.  
   - Detects duplicates and appends them to `duplicates.csv`.  
   - Detects invalid rows, logs them to `invalid_rows.csv`.  
   - Uses bulk insert (batch size configurable) for efficiency.

3. **Testing**  
   - A separate .NET project contains integration tests (using Testcontainers for MS SQL).  
   - The tests confirm correct CSV loading, duplicate detection, and invalid row handling.

## Prerequisites

- Docker, Docker Compose
- .NET SDK (if building locally; not strictly required to run via Docker)

## Usage

1. **Copy or clone** the repository locally.
2. **Prepare `.env`** with environment variables:
```SA_PASSWORD=YourStrong!Passw0rd CONNECTION_STRING=Server=db;Database=ETL_DataLoaderDB;User=SA;TrustServerCertificate=True;Password=YourStrong!Passw0rd;```
3. **Place CSV** file(s) in the `Data/` folder (or update paths as needed).
4. **Run**:
```docker-compose up --build -d``` IN ETL-CSV-DATALOADER Folder (where docker-compose.yml exists)!!! 
- `db` starts MSSQL.
- `migrate` runs the SQL migration script, dropping and recreating the database.
- `etl` runs the ETL program, reading the CSV, converting data, and inserting into `db`.
5. **Check logs** for final statistics on inserted rows, duplicates, and invalid ones.
6. **Check output files** `duplicates.csv` and `invalid_rows.csv` in the `Data/` folder, if created.

## Schema and Scripts

- **SQL Migration**: [scripts/migration.sql](scripts/migration.sql) (dropped DB, created `ETL_DataLoaderDB`, created table with computed column `TripDurationMinutes`, added indexes).
- **Result**: 
- The table `TaxiTrips` is created with appropriate data types, plus a computed column for trip duration.

## Row Counts

After running with the provided data, the table row count is printed to the console. This depends on the actual CSV size and content, but typically **the final console output** shows:
```Done! Inserted : XXXX Duplicates : YYYY Invalid : ZZZZ```
where `XXXX` is the total inserted.

## Assumptions

- Using .NET Core for cross-platform support.
- Dropping and recreating the DB each time for test clarity.
- Using `TripDurationMinutes` as a persisted column for faster queries.
- The environment is orchestrated via Docker Compose for consistent local testing.
- Invalid or duplicate rows are never silently fixed or ignored – they go into separate CSVs.
- For very large files, further optimizations or streaming would be applied as needed.
