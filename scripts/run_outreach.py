"""Run outreach generation for top scored leads and save results."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import pandas as pd
from dotenv import load_dotenv

# Ensure `src` imports work when running `python scripts/run_outreach.py`.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.outreach.email_generator import generate_outreach_batch  # noqa: E402


def _to_numeric_score(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return float("-inf")


def _first_two_lines(text: Any) -> str:
    content = str(text or "").strip()
    if not content:
        return ""
    lines = [line.strip() for line in content.splitlines() if line.strip()]
    if lines:
        return "\n".join(lines[:2])
    sentences = [part.strip() for part in content.split(". ") if part.strip()]
    return ". ".join(sentences[:2])


def main() -> None:
    load_dotenv()

    input_path = PROJECT_ROOT / "data" / "leads_scored.csv"
    output_path = PROJECT_ROOT / "data" / "leads_outreach.csv"

    df = pd.read_csv(input_path)
    leads: list[dict[str, Any]] = df.to_dict(orient="records")

    sorted_leads = sorted(leads, key=lambda lead: _to_numeric_score(lead.get("score")), reverse=True)
    top_leads = sorted_leads[:10]

    outreach = generate_outreach_batch(top_leads, limit=10, sleep_seconds=0.5)
    pd.DataFrame(outreach).to_csv(output_path, index=False)

    print(f"Outreach generated: {len(outreach)}")
    print("Sample outreach:")
    for row in outreach[:2]:
        print(
            {
                "name": row.get("name"),
                "subject": row.get("subject"),
                "email_first_2_lines": _first_two_lines(row.get("email")),
            }
        )


if __name__ == "__main__":
    main()
