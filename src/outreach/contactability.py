"""Contactability checks for scored leads."""

from __future__ import annotations

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
        return evaluated

    if _is_missing(score):
        evaluated.update(
            {
                "contactability_status": "review",
                "do_not_contact": True,
                "contactability_reason": "Lead score is missing.",
            }
        )
        return evaluated

    evaluated.update(
        {
            "contactability_status": "ready",
            "do_not_contact": False,
            "contactability_reason": "Lead has website data and a valid score.",
        }
    )
    return evaluated


def evaluate_contactability_batch(leads: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Evaluate contactability for each lead and return updated rows."""
    return [evaluate_contactability(lead) for lead in leads]


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
