"""Contactability checks for scored leads."""

from __future__ import annotations

import ast
import math
from typing import Any


def evaluate_contactability(lead: dict[str, Any]) -> dict[str, Any]:
    """Evaluate whether a lead is ready for manual outreach review."""
    evaluated = dict(lead)

    website = evaluated.get("website")
    homepage_text = evaluated.get("homepage_text")
    fetch_status = evaluated.get("fetch_status")
    score = evaluated.get("score")

    if _is_missing(website):
        evaluated.update(
            {
                "contactability_status": "review",
                "do_not_contact": True,
                "contactability_reason": "Website is missing.",
            }
        )
        evaluated.update(compute_lead_priority(evaluated))
        return evaluated

    status_code = _to_int(fetch_status)
    if status_code in {403, 404, 500, 502, 503} or _is_missing(homepage_text):
        evaluated.update(
            {
                "contactability_status": "review",
                "do_not_contact": True,
                "contactability_reason": "Website fetch/content is not usable.",
            }
        )
        evaluated.update(compute_lead_priority(evaluated))
        return evaluated

    if _is_missing(score):
        evaluated.update(
            {
                "contactability_status": "review",
                "do_not_contact": True,
                "contactability_reason": "Lead score is missing.",
            }
        )
        evaluated.update(compute_lead_priority(evaluated))
        return evaluated

    evaluated.update(
        {
            "contactability_status": "ready",
            "do_not_contact": False,
            "contactability_reason": "Lead has website data and a valid score.",
        }
    )
    evaluated.update(compute_lead_priority(evaluated))
    return evaluated


def evaluate_contactability_batch(leads: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Evaluate contactability for each lead and return updated rows."""
    return [evaluate_contactability(lead) for lead in leads]


def compute_lead_priority(lead: dict[str, Any]) -> dict[str, Any]:
    """Compute deterministic outreach priority score and label for a finalized lead."""
    ai_score = _clamp(_to_float(lead.get("score")) or 0.0, 0.0, 10.0)
    ai_points = (ai_score / 10.0) * 50.0

    contact_email_score = _clamp(_to_float(lead.get("contact_email_score")) or 0.0, 0.0, 10.0)
    contact_email_points = (contact_email_score / 10.0) * 20.0

    status = str(lead.get("contactability_status") or "").strip().lower()
    if status == "ready":
        contactability_points = 15
    elif status == "review":
        contactability_points = 5
    else:
        contactability_points = 0

    opportunity_points = _compute_opportunity_points(lead)

    total = int(round(ai_points + contact_email_points + contactability_points + opportunity_points))
    lead_priority_score = int(_clamp(float(total), 0.0, 100.0))

    if lead_priority_score >= 80:
        lead_priority_label = "high"
    elif lead_priority_score >= 50:
        lead_priority_label = "medium"
    elif lead_priority_score >= 1:
        lead_priority_label = "low"
    else:
        lead_priority_label = "none"

    return {
        "lead_priority_score": lead_priority_score,
        "lead_priority_label": lead_priority_label,
    }


def _compute_opportunity_points(lead: dict[str, Any]) -> int:
    missing_features = _parse_missing_features(lead.get("missing_features"))
    if missing_features:
        points = 0
        if "booking" in missing_features:
            points += 5
        if "contact_form" in missing_features:
            points += 5
        if "live_chat" in missing_features:
            points += 5
        return points

    points = 0
    if lead.get("has_booking") is False:
        points += 5
    if lead.get("has_contact_form") is False:
        points += 5
    if lead.get("has_chat_widget") is False:
        points += 5
    return points


def _parse_missing_features(value: Any) -> set[str]:
    if isinstance(value, (list, tuple, set)):
        candidates = value
    elif isinstance(value, str):
        raw = value.strip()
        if not raw:
            return set()
        parsed: Any = None
        try:
            parsed = ast.literal_eval(raw)
        except (ValueError, SyntaxError):
            parsed = None
        if isinstance(parsed, (list, tuple, set)):
            candidates = parsed
        else:
            candidates = [part.strip() for part in raw.split(",") if part.strip()]
    else:
        return set()

    normalized: set[str] = set()
    for item in candidates:
        token = str(item).strip().strip("\"'").strip("[]").lower()
        token = token.replace(" ", "_").replace("-", "_")
        if token:
            normalized.add(token)
    return normalized


def _is_missing(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, float) and math.isnan(value):
        return True
    if isinstance(value, str) and not value.strip():
        return True
    return False


def _to_int(value: Any) -> int | None:
    if _is_missing(value):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _to_float(value: Any) -> float | None:
    if _is_missing(value):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(value, upper))
