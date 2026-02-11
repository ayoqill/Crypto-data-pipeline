from datetime import date
from typing import Any, Dict, List


def transform_market_json(rows: List[Dict[str, Any]], snapshot_date: date) -> List[Dict[str, Any]]:
    """
    Transform raw CoinGecko /coins/markets response into structured rows
    ready for loading into Postgres.
    """

    if not isinstance(rows, list):
        raise TypeError("Expected list of dicts from CoinGecko API (/coins/markets).")

    transformed: List[Dict[str, Any]] = []

    for r in rows:
        if not isinstance(r, dict):
            continue  # skip malformed record

        coin_id = r.get("id")
        if not coin_id:
            continue  # skip invalid row

        transformed.append({
            "coin_id": coin_id,
            "symbol": r.get("symbol"),
            "name": r.get("name"),
            "date": snapshot_date.isoformat(),

            "price_usd": float(r.get("current_price") or 0.0),
            "market_cap_usd": float(r.get("market_cap") or 0.0),
            "volume_24h_usd": float(r.get("total_volume") or 0.0),

            # Optional field (make sure your SQL table includes this column if you use it)
            "price_change_24h_pct": float(r.get("price_change_percentage_24h") or 0.0),
        })

    return transformed