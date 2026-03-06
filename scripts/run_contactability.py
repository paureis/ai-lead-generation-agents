"""Run contactability evaluation for outreach leads and save output."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import pandas as pd
from dotenv import load_dotenv

# Ensure `src` imports work when running `python scripts/run_contactability.py`.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.outreach.contactability import evaluate_contactability_batch  # noqa: E402


def _sample_row(lead: dict[str, Any]) -> dict[str, Any]:
    return {
        "name": lead.get("name"),
        "score": lead.get("score"),
        "contactability_status": lead.get("contactability_status"),
        "do_not_contact": lead.get("do_not_contact"),
        "contactability_reason": lead.get("contactability_reason"),
    }


def main() -> None:
    load_dotenv()

    input_path = PROJECT_ROOT / "data" / "leads_outreach.csv"
    output_path = PROJECT_ROOT / "data" / "leads_ready.csv"

    df = pd.read_csv(input_path)
    leads: list[dict[str, Any]] = df.to_dict(orient="records")

    evaluated = evaluate_contactability_batch(leads)
    pd.DataFrame(evaluated).to_csv(output_path, index=False)

    ready_count = sum(1 for lead in evaluated if lead.get("contactability_status") == "ready")
    review_count = sum(1 for lead in evaluated if lead.get("contactability_status") == "review")

    print(f"Total leads processed: {len(evaluated)}")
    print(f"Ready: {ready_count}")
    print(f"Review: {review_count}")
    print("Sample rows:")
    for lead in evaluated[:3]:
        print(_sample_row(lead))


if __name__ == "__main__":
    main()
