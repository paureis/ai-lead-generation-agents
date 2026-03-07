"""Website enrichment utilities for lead records."""

from __future__ import annotations
from src.enrichment.tech_stack import detect_tech_stack_from_html

import re
import time
from typing import Any
from urllib.parse import urljoin, urlparse

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

def extract_emails_from_text(text: str) -> list[str]:
    """Extract email addresses from raw text/html."""
    if not text.strip():
        return []

    pattern = r"[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}"
    matches = re.findall(pattern, text, flags=re.IGNORECASE)

    cleaned = []
    seen = set()

    for email in matches:
        normalized = email.strip().lower().rstrip(".,;:)")
        if normalized not in seen:
            seen.add(normalized)
            cleaned.append(normalized)

    return cleaned


def find_contact_links(html: str, base_url: str, max_links: int = 2) -> list[str]:
    """Find likely contact/about page links from homepage HTML."""
    if not html.strip() or not base_url.strip():
        return []

    soup = BeautifulSoup(html, "html.parser")
    candidates = []
    seen = set()

    keywords = (
        "contact",
        "about",
        "team",
        "location",
        "locations",
        "get-in-touch",
        "connect",
    )

    for a_tag in soup.find_all("a", href=True):
        href = str(a_tag.get("href") or "").strip()
        link_text = str(a_tag.get_text(" ", strip=True) or "").lower()
        combined = f"{href.lower()} {link_text}"

        if not any(keyword in combined for keyword in keywords):
            continue

        absolute_url = urljoin(base_url, href)
        parsed = urlparse(absolute_url)

        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            continue

        normalized = absolute_url.split("#")[0]
        if normalized not in seen:
            seen.add(normalized)
            candidates.append(normalized)

        if len(candidates) >= max_links:
            break

    return candidates


def choose_best_contact_email(emails: list[str]) -> str:
    """Choose the best outreach email from a list of discovered emails."""
    if not emails:
        return ""

    priority_prefixes = ["info@", "contact@", "hello@", "office@", "admin@", "support@"]

    unique_emails = []
    seen = set()
    for email in emails:
        normalized = str(email).strip().lower()
        if normalized and normalized not in seen:
            seen.add(normalized)
            unique_emails.append(normalized)

    for prefix in priority_prefixes:
        for email in unique_emails:
            if email.startswith(prefix):
                return email

    return unique_emails[0]


def extract_contact_emails_from_site(
    website: str,
    homepage_html: str,
    homepage_text: str,
) -> dict[str, Any]:
    """Extract contact emails from homepage and likely contact/about pages."""
    discovered_emails = []
    contact_page_url = ""

    homepage_candidates = extract_emails_from_text(homepage_html) + extract_emails_from_text(homepage_text)
    for email in homepage_candidates:
        if email not in discovered_emails:
            discovered_emails.append(email)

    contact_links = find_contact_links(homepage_html, website, max_links=2)

    for link in contact_links:
        status_code, html, _ = fetch_homepage(link)
        if not html:
            continue

        if not contact_page_url:
            contact_page_url = link

        page_text = extract_visible_text(html)
        page_emails = extract_emails_from_text(html) + extract_emails_from_text(page_text)

        for email in page_emails:
            if email not in discovered_emails:
                discovered_emails.append(email)

    best_contact_email = choose_best_contact_email(discovered_emails)

    return {
        "contact_emails": discovered_emails,
        "best_contact_email": best_contact_email,
        "contact_page_url": contact_page_url,
    }

def detect_seo_signals(html: str) -> dict[str, Any]:
    """Detect lightweight on-page SEO signals from homepage HTML."""
    if not html.strip():
        return {
            "has_title": False,
            "has_meta_description": False,
            "has_h1": False,
            "has_image_alt_text": False,
            "seo_summary": "Could not inspect on-page SEO signals.",
        }

    soup = BeautifulSoup(html, "html.parser")

    title_tag = soup.find("title")
    meta_description_tag = soup.find("meta", attrs={"name": re.compile(r"^description$", re.I)})
    h1_tag = soup.find("h1")

    images = soup.find_all("img")
    has_image_alt_text = any(
        str(img.get("alt") or "").strip()
        for img in images
    )

    has_title = bool(title_tag and str(title_tag.get_text(strip=True)))
    has_meta_description = bool(
        meta_description_tag and str(meta_description_tag.get("content") or "").strip()
    )
    has_h1 = bool(h1_tag and str(h1_tag.get_text(strip=True)))

    missing_items: list[str] = []
    present_items: list[str] = []

    if has_title:
        present_items.append("title tag")
    else:
        missing_items.append("title tag")

    if has_meta_description:
        present_items.append("meta description")
    else:
        missing_items.append("meta description")

    if has_h1:
        present_items.append("H1 heading")
    else:
        missing_items.append("H1 heading")

    if has_image_alt_text:
        present_items.append("image alt text")
    else:
        missing_items.append("image alt text")

    def _join_readable(items: list[str]) -> str:
        if not items:
            return ""
        if len(items) == 1:
            return items[0]
        if len(items) == 2:
            return f"{items[0]} and {items[1]}"
        return f"{', '.join(items[:-1])}, and {items[-1]}"

    if not missing_items:
        seo_summary = f"Has {_join_readable(present_items)}."
    elif not present_items:
        seo_summary = f"Missing {_join_readable(missing_items)}."
    else:
        seo_summary = (
            f"Has {_join_readable(present_items)} but missing {_join_readable(missing_items)}."
        )

    return {
        "has_title": has_title,
        "has_meta_description": has_meta_description,
        "has_h1": has_h1,
        "has_image_alt_text": has_image_alt_text,
        "seo_summary": seo_summary,
    }


def build_feature_summary(
    has_booking: bool,
    has_contact_form: bool,
    has_chat_widget: bool,
) -> dict[str, Any]:
    """Build structured missing-feature data and a human-readable summary."""
    feature_labels = {
        "booking": "booking",
        "contact_form": "contact form",
        "live_chat": "live chat",
    }

    present_features: list[str] = []
    missing_features: list[str] = []

    if has_booking:
        present_features.append(feature_labels["booking"])
    else:
        missing_features.append("booking")

    if has_contact_form:
        present_features.append(feature_labels["contact_form"])
    else:
        missing_features.append("contact_form")

    if has_chat_widget:
        present_features.append(feature_labels["live_chat"])
    else:
        missing_features.append("live_chat")

    def _join_readable(items: list[str]) -> str:
        if not items:
            return ""
        if len(items) == 1:
            return items[0]
        if len(items) == 2:
            return f"{items[0]} and {items[1]}"
        return f"{', '.join(items[:-1])}, and {items[-1]}"

    missing_readable = [feature_labels[item] for item in missing_features]
    present_readable = present_features

    if not missing_features:
        feature_summary = f"Has {_join_readable(present_readable)}."
    elif not present_features:
        feature_summary = f"Missing {_join_readable(missing_readable)}."
    else:
        feature_summary = (
            f"Has {_join_readable(present_readable)} but missing "
            f"{_join_readable(missing_readable)}."
        )

    return {
        "missing_features": missing_features,
        "feature_summary": feature_summary,
    }


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
        feature_data = build_feature_summary(
            has_booking=False,
            has_contact_form=False,
            has_chat_widget=False,
        )
        enriched.update(
            {
                "homepage_text": "",
                "fetch_status": None,
                "fetch_error": "no website",
                "has_booking": False,
                "has_contact_form": False,
                "has_chat_widget": False,
                "tech_hints": [],
                "tech_stack": "",
                "has_title": False,
                "has_meta_description": False,
                "has_h1": False,
                "has_image_alt_text": False,
                "seo_summary": "Could not inspect on-page SEO signals.",
                "missing_features": feature_data["missing_features"],
                "feature_summary": feature_data["feature_summary"],
                "contact_emails": [],
                "best_contact_email": "",
                "contact_page_url": "",
            }
        )
        return enriched

    status_code, html, error_message = fetch_homepage(website)
    tech_stack = detect_tech_stack_from_html(html or "")
    homepage_text = extract_visible_text(html) if html else ""
    signals = detect_signals(html or "", homepage_text)
    seo_signals = detect_seo_signals(html or "")
    contact_data = extract_contact_emails_from_site(website, html or "", homepage_text)
    
    

    has_booking = bool(signals.get("has_booking", False))
    has_contact_form = bool(signals.get("has_contact_form", False))
    has_chat_widget = bool(signals.get("has_chat_widget", False))
    feature_data = build_feature_summary(
        has_booking=has_booking,
        has_contact_form=has_contact_form,
        has_chat_widget=has_chat_widget,
    )

    enriched.update(
        {
            "homepage_text": homepage_text,
            "fetch_status": status_code,
            "fetch_error": error_message,
            "has_booking": has_booking,
            "has_contact_form": has_contact_form,
            "has_chat_widget": has_chat_widget,
            "tech_hints": list(signals.get("tech_hints", [])),
            "tech_stack": ", ".join(tech_stack) if tech_stack else "",
            "has_title": bool(seo_signals.get("has_title", False)),
            "has_meta_description": bool(seo_signals.get("has_meta_description", False)),
            "has_h1": bool(seo_signals.get("has_h1", False)),
            "has_image_alt_text": bool(seo_signals.get("has_image_alt_text", False)),
            "seo_summary": seo_signals.get("seo_summary", ""),
            "missing_features": feature_data["missing_features"],
            "feature_summary": feature_data["feature_summary"],
            "contact_emails": list(contact_data.get("contact_emails", [])),
            "best_contact_email": str(contact_data.get("best_contact_email", "") or ""),
            "contact_page_url": str(contact_data.get("contact_page_url", "") or ""),
        }
    )
    return enriched


def enrich_leads(
    leads: list[dict[str, Any]],
    limit: int | None = None,
    sleep_seconds: float = 0.0,
    max_workers: int = 6,
) -> list[dict[str, Any]]:
    """Enrich multiple leads with optional limit and parallel fetching."""
    from concurrent.futures import ThreadPoolExecutor, as_completed

    max_items = len(leads) if limit is None else max(0, min(limit, len(leads)))
    selected_leads = leads[:max_items]

    results: list[dict[str, Any] | None] = [None] * len(selected_leads)

    def _safe_enrich(index: int, lead: dict[str, Any]) -> tuple[int, dict[str, Any]]:
        try:
            return index, enrich_lead(lead)
        except Exception as exc:
            feature_data = build_feature_summary(
                has_booking=False,
                has_contact_form=False,
                has_chat_widget=False,
            )
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
                    "tech_stack": "",
                    "has_title": False,
                    "has_meta_description": False,
                    "has_h1": False,
                    "has_image_alt_text": False,
                    "seo_summary": "Could not inspect on-page SEO signals.",
                    "missing_features": feature_data["missing_features"],
                    "feature_summary": feature_data["feature_summary"],
                    "contact_emails": [],
                    "best_contact_email": "",
                    "contact_page_url": "",
                }
            )
            return index, fallback

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(_safe_enrich, idx, lead)
            for idx, lead in enumerate(selected_leads)
        ]

        for future in as_completed(futures):
            index, enriched = future.result()
            results[index] = enriched

    return [r for r in results if r is not None]


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