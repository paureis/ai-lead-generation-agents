"""Personalized outreach generation for scored leads."""

from __future__ import annotations

import json
import os
import time
from typing import Any

from dotenv import load_dotenv
from openai import OpenAI

SYSTEM_PROMPT = (
    "You are an expert B2B sales copywriter writing personalized cold outreach for local businesses. "
    "Return only strict JSON with keys: subject, email, cta, followup_1, followup_2. "
    "Constraints: subject <= 8 words, email <= 120 words, cta <= 12 words, "
    "followup_1 <= 90 words, followup_2 <= 90 words. "
    "Use a human tone, be specific, mention a relevant site/signal observation, offer one clear benefit, "
    "use a soft CTA, avoid spammy language including guarantee/free money/excessive hype, and do not use markdown."
)


def generate_outreach(lead: dict[str, Any], model: str = "gpt-4o-mini") -> dict[str, str]:
    """Generate personalized outreach copy for a single lead."""
    client = _get_openai_client()
    user_prompt = _build_user_prompt(lead)

    response = client.chat.completions.create(
        model=model,
        response_format={"type": "json_object"},
        temperature=0.5,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt},
        ],
    )

    content = (response.choices[0].message.content or "").strip()
    parsed = _parse_json(content)
    return _normalize_outreach(parsed)


def generate_outreach_batch(
    leads: list[dict[str, Any]],
    limit: int | None = None,
    sleep_seconds: float = 0.5,
) -> list[dict[str, Any]]:
    """Generate outreach for a lead batch, appending outreach fields to each row."""
    max_items = len(leads) if limit is None else max(0, min(limit, len(leads)))
    enriched: list[dict[str, Any]] = []

    for index, lead in enumerate(leads[:max_items]):
        row = dict(lead)
        try:
            outreach = generate_outreach(lead)
            row.update(outreach)
            row["outreach_error"] = None
        except Exception as exc:  # Defensive: continue batch on model/API failures.
            row.update(
                {
                    "subject": "",
                    "email": "",
                    "cta": "",
                    "followup_1": "",
                    "followup_2": "",
                    "outreach_error": str(exc),
                }
            )
        enriched.append(row)

        if sleep_seconds > 0 and index < max_items - 1:
            time.sleep(sleep_seconds)

    return enriched


def _get_openai_client() -> OpenAI:
    load_dotenv()
    if not os.getenv("OPENAI_API_KEY"):
        raise RuntimeError("OPENAI_API_KEY is not set")
    return OpenAI()


def _build_user_prompt(lead: dict[str, Any]) -> str:
    homepage_text = str(lead.get("homepage_text") or "")[:1200]

    payload = {
        "name": lead.get("name"),
        "website": lead.get("website"),
        "rating": lead.get("rating"),
        "reviews": lead.get("reviews"),
        "category": lead.get("category"),
        "homepage_text": homepage_text,
        "has_booking": lead.get("has_booking"),
        "has_contact_form": lead.get("has_contact_form"),
        "has_chat_widget": lead.get("has_chat_widget"),
        "tech_hints": lead.get("tech_hints"),
        "score": lead.get("score"),
        "reasoning": lead.get("reasoning"),
        "opportunity": lead.get("opportunity"),
        "icebreaker": lead.get("icebreaker"),
        "confidence": lead.get("confidence"),
    }

    return (
        "Generate personalized outreach for this lead.\n\n"
        "Rules:\n"
        "- Be concise, natural, and specific.\n"
        "- Mention something relevant from the site or business.\n"
        "- Focus on one clear benefit tied to the opportunity.\n"
        "- Avoid hype or spammy phrasing.\n"
        "- Return only JSON with keys: subject, email, cta, followup_1, followup_2.\n\n"
        f"Lead data:\n{json.dumps(payload, ensure_ascii=False)}"
    )


def _parse_json(content: str) -> dict[str, Any]:
    if not content:
        raise RuntimeError("OpenAI response content is empty")
    try:
        parsed = json.loads(content)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Invalid JSON from model: {exc}") from exc
    if not isinstance(parsed, dict):
        raise RuntimeError("Model response JSON is not an object")
    return parsed


def _normalize_outreach(raw: dict[str, Any]) -> dict[str, str]:
    return {
        "subject": _truncate_words(str(raw.get("subject", "")).strip(), 8),
        "email": _truncate_words(str(raw.get("email", "")).strip(), 120),
        "cta": _truncate_words(str(raw.get("cta", "")).strip(), 12),
        "followup_1": _truncate_words(str(raw.get("followup_1", "")).strip(), 90),
        "followup_2": _truncate_words(str(raw.get("followup_2", "")).strip(), 90),
    }


def _truncate_words(text: str, max_words: int) -> str:
    words = text.split()
    if len(words) <= max_words:
        return text
    return " ".join(words[:max_words])
