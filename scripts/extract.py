import os
import json
import time
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

import requests


COINGECKO_BASE = "https://api.coingecko.com/api/v3"
PROJECT_ROOT = Path(__file__).resolve().parents[1]
SAMPLE_PATH = PROJECT_ROOT / "include" / "sample_response.json"

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def _is_true(val: str | None) -> bool:
    return (val or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def _get_int_env(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except ValueError:
        return default


def fetch_market_data(coin_ids: List[str], vs_currency: str = "usd") -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Returns: (payload_json, meta)
    payload_json is expected to be a LIST of dicts (from /coins/markets)
    """

    # ✅ Validate input early
    if not coin_ids:
        raise ValueError("coin_ids is empty. Provide at least 1 coin id (e.g. ['bitcoin','ethereum']).")

    use_mock = _is_true(os.getenv("USE_MOCK"))
    requested_at = datetime.now(timezone.utc).isoformat()

    # ✅ MOCK MODE
    if use_mock:
        if not SAMPLE_PATH.exists():
            raise FileNotFoundError(
                f"USE_MOCK=true but sample file not found: {SAMPLE_PATH}\n"
                f"Create include/sample_response.json by saving a real API response."
            )

        logger.info("Mock mode ON. Loading sample response from %s", SAMPLE_PATH)
        payload = json.loads(SAMPLE_PATH.read_text(encoding="utf-8"))

        # ✅ Validate response type
        if not isinstance(payload, list):
            raise TypeError("Mock JSON must be a list (same as /coins/markets response).")

        meta = {
            "mode": "mock",
            "requested_at_utc": requested_at,
            "loaded_from": str(SAMPLE_PATH),
            "coins": coin_ids,
        }
        return payload, meta

    # ✅ REAL MODE (with retry)
    url = f"{COINGECKO_BASE}/coins/markets"
    params = {
        "vs_currency": vs_currency,
        "ids": ",".join(coin_ids),
        "order": "market_cap_desc",
        "per_page": len(coin_ids),
        "page": 1,
        "sparkline": "false",
        "price_change_percentage": "24h",
        # Demo key style you got:
        "x_cg_demo_api_key": os.getenv("COINGECKO_API_KEY", "").strip(),
    }

    max_retries = _get_int_env("API_MAX_RETRIES", 3)
    backoff_seconds = _get_int_env("API_BACKOFF_SECONDS", 2)

    last_error: str | None = None

    for attempt in range(1, max_retries + 1):
        try:
            logger.info("Calling CoinGecko (%s/%s) coins=%s", attempt, max_retries, len(coin_ids))
            r = requests.get(url, params=params, timeout=30)

            meta = {
                "mode": "api",
                "requested_at_utc": requested_at,
                "status_code": r.status_code,
                "url": r.url,
            }

            # ✅ Rate limit: do NOT aggressive retry
            if r.status_code == 429:
                retry_after = r.headers.get("Retry-After")
                msg = f"Rate limit hit (429). Retry-After={retry_after}. Tip: USE_MOCK=true while developing."
                logger.warning(msg)
                raise RuntimeError(msg)

            # ✅ Other non-OK responses
            if not r.ok:
                body_preview = (r.text or "")[:200]
                raise RuntimeError(f"CoinGecko error {r.status_code}: {body_preview}")

            payload = r.json()

            # ✅ Validate response type
            if not isinstance(payload, list):
                raise TypeError("Unexpected response type: expected list from /coins/markets.")

            logger.info("CoinGecko success. rows=%s", len(payload))
            return payload, meta

        except RuntimeError as e:
            # If it's 429, no point retrying fast.
            last_error = str(e)
            if "429" in last_error:
                raise

            logger.warning("Attempt %s failed: %s", attempt, last_error)

        except requests.RequestException as e:
            last_error = f"Request failed: {e}"
            logger.warning("Attempt %s failed: %s", attempt, last_error)

        # backoff before next retry (except last attempt)
        if attempt < max_retries:
            sleep_time = backoff_seconds * attempt
            logger.info("Retrying in %ss...", sleep_time)
            time.sleep(sleep_time)

    raise RuntimeError(f"All retries failed. Last error: {last_error}")
