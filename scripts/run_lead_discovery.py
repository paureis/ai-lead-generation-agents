"""Run Google Maps lead discovery and save raw leads to CSV."""

from __future__ import annotations

import sys
from pathlib import Path

from dotenv import load_dotenv

# Ensure `src` imports work when running `python scripts/run_lead_discovery.py`.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.lead_sources.serpapi_maps import (  # noqa: E402
    save_leads_to_csv,
    search_google_maps,
)


def main() -> None:
    load_dotenv()

    query = "dentists in miami"
    max_results = 50
    output_path = PROJECT_ROOT / "data" / "leads_raw.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    leads = search_google_maps(query=query, max_results=max_results)
    save_leads_to_csv(leads, str(output_path))

    print(f"Leads found: {len(leads)}")
    print("First 5 leads:")
    for lead in leads[:5]:
        print(lead)


if __name__ == "__main__":
    main()
