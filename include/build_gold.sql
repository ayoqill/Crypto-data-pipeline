INSERT INTO crypto_daily_metrics (coin_id, date, price_usd, return_1d_pct, ma_7, ma_30, updated_at)
SELECT
  coin_id,
  date,
  price_usd,
  CASE
    WHEN LAG(price_usd) OVER (PARTITION BY coin_id ORDER BY date) IS NULL THEN NULL
    WHEN LAG(price_usd) OVER (PARTITION BY coin_id ORDER BY date) = 0 THEN NULL
    ELSE
      (price_usd - LAG(price_usd) OVER (PARTITION BY coin_id ORDER BY date))
      / LAG(price_usd) OVER (PARTITION BY coin_id ORDER BY date) * 100
  END AS return_1d_pct,
  AVG(price_usd) OVER (PARTITION BY coin_id ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS ma_7,
  AVG(price_usd) OVER (PARTITION BY coin_id ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS ma_30,
  NOW()
FROM crypto_daily_market
ON CONFLICT (coin_id, date)
DO UPDATE SET
  price_usd = EXCLUDED.price_usd,
  return_1d_pct = EXCLUDED.return_1d_pct,
  ma_7 = EXCLUDED.ma_7,
  ma_30 = EXCLUDED.ma_30,
  updated_at = NOW();
