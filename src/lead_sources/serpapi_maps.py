"""Google Maps lead search via SerpAPI."""

from __future__ import annotations

import os
from typing import Any, Mapping, Sequence
from urllib.parse import parse_qs, urlparse

import pandas as pd
import requests

SERPAPI_ENDPOINT = "https://serpapi.com/search.json"
SERPAPI_PAGE_SIZE = 20


def search_google_maps(query: str, max_results: int = 50) -> list[dict[str, Any]]:
    """Search businesses on Google Maps via SerpAPI and return structured leads."""
    if not query.strip():
        raise ValueError("query must be a non-empty string")
    if max_results <= 0:
        return []

    api_key = _get_serpapi_api_key()
    leads: list[dict[str, Any]] = []
    seen_place_ids: set[str] = set()
    seen_starts: set[int] = set()

    start = 0
    while len(leads) < max_results and start not in seen_starts:
        seen_starts.add(start)
        payload = _request_google_maps_page(api_key=api_key, query=query, start=start)
        results = payload.get("local_results", [])
        if not isinstance(results, list) or not results:
            break

        for item in results:
            if not isinstance(item, Mapping):
                continue

            lead = _build_lead(item, source_query=query)
            place_id = str(lead.get("place_id") or "")

            # Keep unique businesses when place id is available.
            if place_id and place_id in seen_place_ids:
                continue
            if place_id:
                seen_place_ids.add(place_id)

            leads.append(lead)
            if len(leads) >= max_results:
                break

        if len(leads) >= max_results:
            break

        start += SERPAPI_PAGE_SIZE
    return leads[:max_results]


def save_leads_to_csv(leads: Sequence[Mapping[str, Any]], filepath: str) -> None:
    """Save leads to CSV using pandas."""
    df = pd.DataFrame(list(leads))
    df.to_csv(filepath, index=False)


def _get_serpapi_api_key() -> str:
    api_key = os.getenv("SERPAPI_API_KEY")
    if not api_key:
        raise RuntimeError("SERPAPI_API_KEY is not set")
    return api_key


def _request_google_maps_page(api_key: str, query: str, start: int) -> dict[str, Any]:
    params = {
        "engine": "google_maps",
        "type": "search",
        "q": query,
        "start": start,
        "api_key": api_key,
    }
    response = requests.get(SERPAPI_ENDPOINT, params=params, timeout=30)
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise RuntimeError("Unexpected SerpAPI response format")
    return payload

    raw_start = pagination.get("next_page_token")
    if isinstance(raw_start, int):
        return raw_start
    if isinstance(raw_start, str) and raw_start.isdigit():
        return int(raw_start)

    next_url = pagination.get("next")
    if not isinstance(next_url, str):
        return None

    parsed = urlparse(next_url)
    query = parse_qs(parsed.query)
    start_values = query.get("start", [])
    if not start_values:
        return None

    value = start_values[0]
    if isinstance(value, str) and value.isdigit():
        return int(value)
    return None


def _build_lead(item: Mapping[str, Any], source_query: str) -> dict[str, Any]:
    return {
        "name": item.get("title"),
        "address": item.get("address"),
        "phone": item.get("phone"),
        "website": str(item.get("website")).strip() if item.get("website") else None,
        "rating": _to_float(item.get("rating")),
        "reviews": _to_int(item.get("reviews")),
        "category": item.get("type"),
        "place_id": item.get("place_id") or item.get("data_id") or item.get("cid"),
        "source_query": source_query,
    }


def _to_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
