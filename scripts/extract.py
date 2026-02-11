import os
import requests
from datetime import datetime, timezone

COINGECKO_BASE = "https://api.coingecko.com/api/v3"

def fetch_market_data(coin_ids: list[str], vs_currency: str = "usd") -> tuple[dict, dict]:
    """
    Returns: (payload_json, meta)
    """
    url = f"{COINGECKO_BASE}/coins/markets"
    params = {
        "vs_currency": vs_currency,
        "ids": ",".join(coin_ids),
        "order": "market_cap_desc",
        "per_page": len(coin_ids),
        "page": 1,
        "sparkline": "false",
        "price_change_percentage": "24h",
    }

    headers = {}
    api_key = os.getenv("COINGECKO_API_KEY")
    # If you have a key/provider, put it here. If not, it still works for basic usage.
    # Some setups use: headers["x-cg-pro-api-key"] = api_key
    if api_key:
        headers["x-cg-pro-api-key"] = api_key

    t0 = datetime.now(timezone.utc)
    r = requests.get(url, params=params, headers=headers, timeout=30)
    meta = {
        "requested_at_utc": t0.isoformat(),
        "status_code": r.status_code,
        "url": r.url,
    }

    if r.status_code == 429:
        raise RuntimeError("Rate limit hit (429). Try fewer coins or schedule less often.")
    r.raise_for_status()
    return r.json(), meta
