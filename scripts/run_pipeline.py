"""Run the full AI lead generation pipeline end-to-end."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import pandas as pd
from dotenv import load_dotenv

# Ensure `src` imports work when running `python scripts/run_pipeline.py`.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.enrichment.website_enricher import enrich_leads  # noqa: E402
from src.lead_sources.serpapi_maps import save_leads_to_csv, search_google_maps  # noqa: E402
from src.outreach.contactability import evaluate_contactability_batch  # noqa: E402
from src.outreach.email_generator import generate_outreach_batch  # noqa: E402
from src.scoring.lead_scorer import score_leads  # noqa: E402


def _to_numeric_score(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return float("-inf")


def _read_csv_records(path: Path) -> list[dict[str, Any]]:
    df = pd.read_csv(path)
    return df.to_dict(orient="records")


def _write_csv_records(records: list[dict[str, Any]], path: Path) -> None:
    pd.DataFrame(records).to_csv(path, index=False)


def main() -> None:
    load_dotenv()
    data_dir = PROJECT_ROOT / "data"
    data_dir.mkdir(parents=True, exist_ok=True)

    raw_path = data_dir / "leads_raw.csv"
    enriched_path = data_dir / "leads_enriched.csv"
    scored_path = data_dir / "leads_scored.csv"
    outreach_path = data_dir / "leads_outreach.csv"
    ready_path = data_dir / "leads_ready.csv"

    print("\nStep 1/5: Lead discovery...")
    raw_leads = search_google_maps(query="dentists in miami", max_results=20)
    save_leads_to_csv(raw_leads, str(raw_path))
    print(f"Processed: {len(raw_leads)} | Output: {str(raw_path)}")

    print("\nStep 2/5: Enrichment...")
    raw_records = _read_csv_records(raw_path)
    enriched_leads = enrich_leads(raw_records, limit=20, sleep_seconds=1.0)
    _write_csv_records(enriched_leads, enriched_path)
    print(f"Processed: {len(enriched_leads)} | Output: {str(enriched_path)}")

    print("\nStep 3/5: Scoring...")
    enriched_records = _read_csv_records(enriched_path)
    scored_leads = score_leads(enriched_records, limit=10, sleep_seconds=0.5)
    _write_csv_records(scored_leads, scored_path)
    print(f"Processed: {len(scored_leads)} | Output: {str(scored_path)}")

    print("\nStep 4/5: Outreach generation...")
    scored_records = _read_csv_records(scored_path)
    top_scored = sorted(scored_records, key=lambda row: _to_numeric_score(row.get("score")), reverse=True)[:10]
    outreach_leads = generate_outreach_batch(top_scored, limit=10, sleep_seconds=0.5)
    _write_csv_records(outreach_leads, outreach_path)
    print(f"Processed: {len(outreach_leads)} | Output: {str(outreach_path)}")

    print("\nStep 5/5: Contactability review...")
    outreach_records = _read_csv_records(outreach_path)
    ready_leads = evaluate_contactability_batch(outreach_records)
    _write_csv_records(ready_leads, ready_path)
    print(f"Processed: {len(ready_leads)} | Output: {str(ready_path)}")

    ready_count = sum(1 for row in ready_leads if row.get("contactability_status") == "ready")
    review_count = sum(1 for row in ready_leads if row.get("contactability_status") == "review")

    print("Pipeline complete.")
    print(f"raw leads count: {len(raw_leads)}")
    print(f"enriched leads count: {len(enriched_leads)}")
    print(f"scored leads count: {len(scored_leads)}")
    print(f"outreach generated count: {len(outreach_leads)}")
    print(f"ready count: {ready_count}")
    print(f"review count: {review_count}")


if __name__ == "__main__":
    main()
