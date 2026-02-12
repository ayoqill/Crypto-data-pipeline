# Crypto Market Data Engineering Pipeline

An end-to-end data engineering pipeline that ingests cryptocurrency market data from the CoinGecko API, stores raw JSON in object storage, transforms it into structured tables, and builds analytical metrics using Airflow orchestration.

Architecture

API → MinIO (Bronze) → Postgres (Silver) → Postgres (Gold)

Bronze: Raw JSON stored in MinIO (daily partitioned)

Silver: Cleaned daily market snapshot table

Gold: Derived analytical metrics (returns, moving averages)
---
Tech Stack

Apache Airflow (Orchestration)

PostgreSQL (Data warehouse layer)

MinIO (Object storage)

Docker & Docker Compose (Containerization)

CoinGecko API (Data source)
