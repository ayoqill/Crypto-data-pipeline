-- Database schema for cryptocurrency data pipeline
-- Crypto daily market snapshot (one row per coin per day)

CREATE TABLE IF NOT EXISTS crypto_daily_market (
  coin_id TEXT NOT NULL,
  symbol TEXT,
  name TEXT,
  date DATE NOT NULL,

  price_usd DOUBLE PRECISION,
  market_cap_usd DOUBLE PRECISION,
  volume_24h_usd DOUBLE PRECISION,

  source TEXT DEFAULT 'coingecko',
  ingested_at TIMESTAMPTZ DEFAULT NOW(),

  PRIMARY KEY (coin_id, date)
);
