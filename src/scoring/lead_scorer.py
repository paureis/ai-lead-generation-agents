"""Lead scoring with OpenAI."""

from __future__ import annotations

import json
import os
import time
from typing import Any

from dotenv import load_dotenv
from openai import OpenAI

SYSTEM_PROMPT = (
    "You are a B2B growth consultant evaluating local business websites and lead quality. "
    "Given lead context, return only strict JSON with these keys: "
    "score (integer 1-10), reasoning (short), opportunity (short), "
    "icebreaker (one sentence), offer (short), confidence (low|medium|high). "
    "Do not include markdown or extra keys."
)


def score_lead(lead: dict[str, Any], model: str = "gpt-4o-mini") -> dict[str, Any]:
    """Score a single lead using OpenAI and return structured scoring fields."""
    client = _get_openai_client()
    user_prompt = _build_user_prompt(lead)

    response = client.chat.completions.create(
        model=model,
        response_format={"type": "json_object"},
        temperature=0.2,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt},
        ],
    )

    content = (response.choices[0].message.content or "").strip()
    parsed = _parse_score_json(content)
    return _normalize_score(parsed)


def score_leads(
    leads: list[dict[str, Any]],
    limit: int | None = None,
    sleep_seconds: float = 0.5,
) -> list[dict[str, Any]]:
    """Score leads in batch, appending score fields into each lead dict."""
    max_items = len(leads) if limit is None else max(0, min(limit, len(leads)))
    scored: list[dict[str, Any]] = []

    for index, lead in enumerate(leads[:max_items]):
        result = dict(lead)
        try:
            scoring = score_lead(lead)
            result.update(scoring)
            result["score_error"] = None
        except Exception as exc:  # Defensive: keep processing batch on failures.
            result.update(
                {
                    "score": None,
                    "reasoning": "",
                    "opportunity": "",
                    "icebreaker": "",
                    "offer": "",
                    "confidence": "low",
                    "score_error": str(exc),
                }
            )
        scored.append(result)

        if sleep_seconds > 0 and index < max_items - 1:
            time.sleep(sleep_seconds)

    return scored


def _get_openai_client() -> OpenAI:
    load_dotenv()
    if not os.getenv("OPENAI_API_KEY"):
        raise RuntimeError("OPENAI_API_KEY is not set")
    return OpenAI()

def _build_user_prompt(lead: dict[str, Any]) -> str:
    homepage_text = str(lead.get("homepage_text") or "")[:1500]
    fetch_status = lead.get("fetch_status")
    fetch_error = lead.get("fetch_error")

    payload = {
        "name": lead.get("name"),
        "website": lead.get("website"),
        "rating": lead.get("rating"),
        "reviews": lead.get("reviews"),
        "homepage_text": homepage_text,
        "fetch_status": fetch_status,
        "fetch_error": fetch_error,
        "has_booking": lead.get("has_booking"),
        "has_contact_form": lead.get("has_contact_form"),
        "has_chat_widget": lead.get("has_chat_widget"),
        "tech_hints": lead.get("tech_hints"),
    }

    return (
        "You are evaluating a potential client for digital marketing and website optimization services.\n\n"
        "Your task is to determine how strong of a sales opportunity this business represents.\n\n"
        "Important scoring rule:\n"
        "- If the website could not be properly inspected (for example fetch_status is 403, 404, 500, or homepage_text is missing/blocked), "
        "do NOT assume the business has poor website conversion systems.\n"
        "- In those cases, lower your confidence and score conservatively unless other evidence strongly suggests opportunity.\n"
        "- Missing website evidence due to blocked access should be treated as unknown, not as proof of a problem.\n\n"
        "Scoring guidelines:\n"
        "1-3 = Poor lead (modern site, strong conversion systems already)\n"
        "4-6 = Moderate lead (some opportunities or limited evidence)\n"
        "7-8 = Strong lead (clear improvement opportunities visible)\n"
        "9-10 = Excellent lead (major website or conversion problems clearly visible)\n\n"
        "Focus on signals like:\n"
        "- Missing booking systems\n"
        "- Weak website conversion design\n"
        "- Outdated technology\n"
        "- Lack of chat/contact capture\n"
        "- Poor messaging or UX\n"
        "- Only count these as strong signals if they are actually visible from the available evidence\n\n"
        "Return ONLY a JSON object with these fields:\n"
        "score, reasoning, opportunity, icebreaker, offer, confidence\n\n"
        "Lead data:\n"
        f"{json.dumps(payload, ensure_ascii=False)}"
    )


def _parse_score_json(content: str) -> dict[str, Any]:
    if not content:
        raise RuntimeError("OpenAI response content is empty")
    try:
        parsed = json.loads(content)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Invalid JSON from model: {exc}") from exc
    if not isinstance(parsed, dict):
        raise RuntimeError("Model response JSON is not an object")
    return parsed


def _normalize_score(raw: dict[str, Any]) -> dict[str, Any]:
    score_value = raw.get("score")
    try:
        score = int(score_value)
    except (TypeError, ValueError):
        score = 1
    score = max(1, min(10, score))

    confidence_raw = str(raw.get("confidence", "low")).strip().lower()
    confidence = confidence_raw if confidence_raw in {"low", "medium", "high"} else "low"

    return {
        "score": score,
        "reasoning": str(raw.get("reasoning", "")).strip(),
        "opportunity": str(raw.get("opportunity", "")).strip(),
        "icebreaker": str(raw.get("icebreaker", "")).strip(),
        "offer": str(raw.get("offer", "")).strip(),
        "confidence": confidence,
    }
