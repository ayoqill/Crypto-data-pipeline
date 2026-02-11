import os
import psycopg2
from psycopg2.extras import execute_values

UPSERT_SQL = """
INSERT INTO crypto_daily_market
(coin_id, symbol, name, date, price_usd, market_cap_usd, volume_24h_usd, source)
VALUES %s
ON CONFLICT (coin_id, date)
DO UPDATE SET
  symbol = EXCLUDED.symbol,
  name = EXCLUDED.name,
  price_usd = EXCLUDED.price_usd,
  market_cap_usd = EXCLUDED.market_cap_usd,
  volume_24h_usd = EXCLUDED.volume_24h_usd,
  ingested_at = NOW();
"""

def get_pg_conn():
    # Use the same Postgres as Airflow metadata (simple for student project).
    # In a "production" setup you'd separate them.
    host = os.getenv("PG_HOST", "postgres")
    db = os.getenv("POSTGRES_DB", "cryptodb")
    user = os.getenv("POSTGRES_USER", "aqil")
    pwd = os.getenv("POSTGRES_PASSWORD", "marcoreus")
    port = int(os.getenv("PG_PORT", 5432))
    return psycopg2.connect(host=host, dbname=db, user=user, password=pwd, port=port)

def upsert_daily(rows: list[dict]) -> int:
    if not rows:
        return 0

    values = [
        (
            r["coin_id"], r["symbol"], r["name"], r["date"],
            r["price_usd"], r["market_cap_usd"], r["volume_24h_usd"],
            "coingecko"
        )
        for r in rows
    ]

    with get_pg_conn() as conn:
        with conn.cursor() as cur:
            execute_values(cur, UPSERT_SQL, values, page_size=200)
    return len(values)
