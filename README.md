# Taxi Trips Architecture Medallion Pipeline

Implementation of Medallion Architecture (Bronze/Silver/Gold) using PySpark and Delta Lake.

## Features
- **Bronze**: Download and ingest raw Parquet (NYC Taxi trips)
- **Silver**: Clean, deduplicate, filter invalid records
- **Gold**: Daily metrics (trips, revenue, avg distance) + top zones
- Local Spark cluster with Docker Compose

## Dataset
- Source: NYC Taxi yellow trips (automatic download Parquet)

## Tools
- PySpark
- Delta Lake (ACID transactions)
- Docker Compose (Spark master/worker)

## How to Run
1. Clone repo

2. docker-compose up -d

3. pip install -r requirements.txt

4. python scripts/main.py

## Output
- data/bronze/raw Parquet
- data/silver/clean Parquet
- data/gold/metrics Parquet
