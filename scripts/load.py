import os
import logging
import psycopg2
from psycopg2.extras import execute_values
from typing import List, Dict, Any

logger = logging.getLogger(__name__)

UPSERT_SQL = """
INSERT INTO crypto_daily_market
(coin_id, symbol, name, date, price_usd, market_cap_usd, volume_24h_usd, price_change_24h_pct, source)
VALUES %s
ON CONFLICT (coin_id, date)
DO UPDATE SET
  symbol = EXCLUDED.symbol,
  name = EXCLUDED.name,
  price_usd = EXCLUDED.price_usd,
  market_cap_usd = EXCLUDED.market_cap_usd,
  volume_24h_usd = EXCLUDED.volume_24h_usd,
  price_change_24h_pct = EXCLUDED.price_change_24h_pct,
  ingested_at = NOW();
"""

def get_pg_conn():
    host = os.getenv("PG_HOST", "postgres")
    db = os.getenv("POSTGRES_DB", "airflow")
    user = os.getenv("POSTGRES_USER", "airflow")
    pwd = os.getenv("POSTGRES_PASSWORD", "airflow")
    port = int(os.getenv("PG_PORT", "5432"))

    return psycopg2.connect(host=host, dbname=db, user=user, password=pwd, port=port)

def upsert_daily(rows: List[Dict[str, Any]]) -> int:
    if not rows:
        logger.info("No rows to upsert.")
        return 0

    values = [
        (
            r["coin_id"],
            r.get("symbol"),
            r.get("name"),
            r["date"],
            r.get("price_usd", 0.0),
            r.get("market_cap_usd", 0.0),
            r.get("volume_24h_usd", 0.0),
            r.get("price_change_24h_pct", 0.0),
            "coingecko",
        )
        for r in rows
    ]

    try:
        with get_pg_conn() as conn:
            with conn.cursor() as cur:
                execute_values(cur, UPSERT_SQL, values, page_size=200)
        logger.info("Upserted %s rows into crypto_daily_market.", len(values))
        return len(values)

    except Exception as e:
        logger.exception("Postgres upsert failed: %s", e)
        raise
