"""Run lead scoring on enriched leads and save scored output."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import pandas as pd
from dotenv import load_dotenv

# Ensure `src` imports work when running `python scripts/run_scoring.py`.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.scoring.lead_scorer import score_leads  # noqa: E402


def _to_numeric_score(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def main() -> None:
    load_dotenv()

    input_path = PROJECT_ROOT / "data" / "leads_enriched.csv"
    output_path = PROJECT_ROOT / "data" / "leads_scored.csv"

    df = pd.read_csv(input_path)
    leads: list[dict[str, Any]] = df.to_dict(orient="records")

    scored = score_leads(leads, limit=10, sleep_seconds=0.5)
    pd.DataFrame(scored).to_csv(output_path, index=False)

    numeric_scores = [
        score for score in (_to_numeric_score(lead.get("score")) for lead in scored) if score is not None
    ]
    average_score = (sum(numeric_scores) / len(numeric_scores)) if numeric_scores else 0.0

    top_leads = sorted(
        scored,
        key=lambda lead: _to_numeric_score(lead.get("score")) or float("-inf"),
        reverse=True,
    )[:3]

    print(f"Leads scored: {len(scored)}")
    print(f"Average score: {average_score:.2f}")
    print("Top 3 leads by score:")
    for lead in top_leads:
        print(
            {
                "name": lead.get("name"),
                "website": lead.get("website"),
                "score": lead.get("score"),
            }
        )


if __name__ == "__main__":
    main()
