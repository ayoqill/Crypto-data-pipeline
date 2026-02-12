# Crypto Market Data Engineering Pipeline

An end-to-end data engineering pipeline that ingests cryptocurrency market data from the CoinGecko API, stores raw JSON in object storage, transforms it into structured tables, and builds analytical metrics using Airflow orchestration.

Architecture

- API → MinIO (Bronze) → Postgres (Silver) → Postgres - crypto daily metrics (Gold)

- Bronze: Raw JSON stored in MinIO (daily partitioned)

- Silver: Cleaned daily market snapshot table

- Gold: Derived analytical metrics (returns, moving averages)

---
Tech Stack

- Apache Airflow (Orchestration)

- PostgreSQL (Data warehouse layer)

- MinIO (Object storage)

- Docker (Containerization)

- CoinGecko API (Data source)

---
Design Principles

- Layered architecture (Bronze / Silver / Gold)

- Idempotent daily loads

- Separation of raw and derived data

- Re-runnable without duplication

- Dockerized, reproducible environment


<img width="1912" height="922" alt="orchestration" src="https://github.com/user-attachments/assets/73aabc09-ca07-45a5-9814-9a625894a031" />

<img width="1896" height="932" alt="minio real" src="https://github.com/user-attachments/assets/aceaa056-da00-4c36-b6a4-66a233478101" />

<img width="1917" height="935" alt="real sillver layer " src="https://github.com/user-attachments/assets/42af5cd5-efe9-45de-ad48-212bf22a1b22" />

<img width="1918" height="936" alt="real gold layer" src="https://github.com/user-attachments/assets/4512041b-1a29-4dc2-9378-caaf30d3d2f6" />

<img width="1918" height="1078" alt="docker coingeko" src="https://github.com/user-attachments/assets/54d21fef-2293-4b5f-87d2-eeb39f0b1737" />







