"""Website enrichment utilities for lead records."""

from __future__ import annotations

import re
import time
from typing import Any
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    )
}


def fetch_homepage(url: str, timeout: int = 15) -> tuple[int | None, str | None, str | None]:
    """Fetch a website homepage and return (status_code, html, error_message)."""
    normalized_url = _normalize_url(url)
    if not normalized_url:
        return (None, None, "invalid url")

    try:
        response = requests.get(
            normalized_url,
            headers=DEFAULT_HEADERS,
            timeout=timeout,
            allow_redirects=True,
        )
        return (response.status_code, response.text, None)
    except requests.RequestException as exc:
        return (None, None, str(exc))


def extract_visible_text(html: str, max_chars: int = 4000) -> str:
    """Extract visible text from HTML and return a whitespace-normalized snippet."""
    soup = BeautifulSoup(html, "html.parser")

    for tag_name in ("script", "style", "noscript"):
        for tag in soup.find_all(tag_name):
            tag.decompose()

    text = soup.get_text(separator=" ", strip=True)
    cleaned = re.sub(r"\s+", " ", text).strip()
    return cleaned[:max_chars]


def detect_signals(html: str, text: str) -> dict[str, Any]:
    """Detect simple conversion and stack signals from page content."""
    html_lower = html.lower()
    text_lower = text.lower()
    combined = f"{text_lower} {html_lower}"

    booking_terms = ("book", "appointment", "schedule")
    contact_terms = ("contact", "<form", "wpforms", "gravityforms")
    chat_terms = ("intercom", "drift", "tawk", "livechat")

    tech_hints: list[str] = []
    tech_markers = {
        "wordpress": ("wp-content", "wp-includes", "wordpress"),
        "shopify": ("cdn.shopify.com", "shopify", "shopify-section"),
        "wix": ("wix.com", "wixstatic.com"),
        "squarespace": ("squarespace", "static1.squarespace.com"),
        "webflow": ("webflow", "webflow.io"),
    }
    for tech_name, markers in tech_markers.items():
        if any(marker in combined for marker in markers):
            tech_hints.append(tech_name)

    return {
        "has_booking": any(term in combined for term in booking_terms),
        "has_contact_form": any(term in combined for term in contact_terms),
        "has_chat_widget": any(term in combined for term in chat_terms),
        "tech_hints": tech_hints,
    }


def enrich_lead(lead: dict[str, Any]) -> dict[str, Any]:
    """Enrich a single lead with homepage content and simple website signals."""
    enriched = dict(lead)
    website_value = enriched.get("website")
    website = str(website_value).strip() if website_value is not None else ""

    if not website:
        enriched.update(
            {
                "homepage_text": "",
                "fetch_status": None,
                "fetch_error": "no website",
                "has_booking": False,
                "has_contact_form": False,
                "has_chat_widget": False,
                "tech_hints": [],
            }
        )
        return enriched

    status_code, html, error_message = fetch_homepage(website)
    homepage_text = extract_visible_text(html) if html else ""
    signals = detect_signals(html or "", homepage_text)

    enriched.update(
        {
            "homepage_text": homepage_text,
            "fetch_status": status_code,
            "fetch_error": error_message,
            "has_booking": bool(signals.get("has_booking", False)),
            "has_contact_form": bool(signals.get("has_contact_form", False)),
            "has_chat_widget": bool(signals.get("has_chat_widget", False)),
            "tech_hints": list(signals.get("tech_hints", [])),
        }
    )
    return enriched


def enrich_leads(
    leads: list[dict[str, Any]],
    limit: int | None = None,
    sleep_seconds: float = 1.0,
) -> list[dict[str, Any]]:
    """Enrich multiple leads with optional limit and throttling."""
    max_items = len(leads) if limit is None else max(0, min(limit, len(leads)))
    enriched_leads: list[dict[str, Any]] = []

    for index, lead in enumerate(leads[:max_items]):
        try:
            enriched_leads.append(enrich_lead(lead))
        except Exception as exc:  # Defensive guard to keep batch processing alive.
            fallback = dict(lead)
            fallback.update(
                {
                    "homepage_text": "",
                    "fetch_status": None,
                    "fetch_error": f"enrich error: {exc}",
                    "has_booking": False,
                    "has_contact_form": False,
                    "has_chat_widget": False,
                    "tech_hints": [],
                }
            )
            enriched_leads.append(fallback)

        if sleep_seconds > 0 and index < max_items - 1:
            time.sleep(sleep_seconds)

    return enriched_leads


def _normalize_url(url: str) -> str | None:
    raw = url.strip()
    if not raw:
        return None

    if not re.match(r"^[a-zA-Z][a-zA-Z0-9+\-.]*://", raw):
        raw = f"https://{raw}"

    parsed = urlparse(raw)
    if not parsed.scheme or not parsed.netloc:
        return None
    if parsed.scheme not in {"http", "https"}:
        return None
    return raw
