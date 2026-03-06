"""Run website enrichment on raw leads and save enriched output."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import pandas as pd
from dotenv import load_dotenv

# Ensure `src` imports work when running `python scripts/run_enrichment.py`.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.enrichment.website_enricher import enrich_leads  # noqa: E402


def _row_preview(lead: dict[str, Any]) -> dict[str, Any]:
    return {
        "name": lead.get("name"),
        "website": lead.get("website"),
        "has_booking": lead.get("has_booking"),
        "has_contact_form": lead.get("has_contact_form"),
        "has_chat_widget": lead.get("has_chat_widget"),
    }


def main() -> None:
    load_dotenv()

    input_path = PROJECT_ROOT / "data" / "leads_raw.csv"
    output_path = PROJECT_ROOT / "data" / "leads_enriched.csv"

    df = pd.read_csv(input_path)
    leads: list[dict[str, Any]] = df.to_dict(orient="records")

    enriched = enrich_leads(leads, limit=20, sleep_seconds=1.0)
    enriched_df = pd.DataFrame(enriched)
    enriched_df.to_csv(output_path, index=False)

    success_count = sum(1 for lead in enriched if lead.get("fetch_status") == 200)

    print(f"Enriched leads: {len(enriched)}")
    print(f"Website fetch success (status 200): {success_count}")
    print("Sample enriched rows:")
    for row in enriched[:3]:
        print(_row_preview(row))


if __name__ == "__main__":
    main()
