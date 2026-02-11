from datetime import date

def transform_market_json(rows: list[dict], snapshot_date: date) -> list[dict]:
    out = []
    for r in rows:
        out.append({
            "coin_id": r.get("id"),
            "symbol": r.get("symbol"),
            "name": r.get("name"),
            "date": snapshot_date.isoformat(),
            "price_usd": r.get("current_price"),
            "market_cap_usd": r.get("market_cap"),
            "volume_24h_usd": r.get("total_volume"),
        })
    return out
