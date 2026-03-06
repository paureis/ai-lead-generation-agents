from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from src.enrichment.website_enricher import enrich_leads
from src.lead_sources.serpapi_maps import save_leads_to_csv, search_google_maps
from src.outreach.contactability import evaluate_contactability_batch
from src.outreach.email_generator import generate_outreach_batch
from src.scoring.lead_scorer import score_leads


def _to_numeric_score(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return float("-inf")


def run_lead_pipeline(
    niche: str,
    city: str,
    max_results: int = 20,
    data_dir: str | Path = "data",
) -> dict[str, Any]:
    data_path = Path(data_dir)
    data_path.mkdir(parents=True, exist_ok=True)

    raw_path = data_path / "leads_raw.csv"
    enriched_path = data_path / "leads_enriched.csv"
    scored_path = data_path / "leads_scored.csv"
    outreach_path = data_path / "leads_outreach.csv"
    ready_path = data_path / "leads_ready.csv"

    query = f"{niche} in {city}"

    # Step 1: Lead discovery
    raw_leads = search_google_maps(query=query, max_results=int(max_results))
    save_leads_to_csv(raw_leads, str(raw_path))

    # Step 2: Enrichment
    enriched_leads = enrich_leads(raw_leads, limit=len(raw_leads), sleep_seconds=1.0)
    pd.DataFrame(enriched_leads).to_csv(enriched_path, index=False)

    # Step 3: Scoring
    scored_leads = score_leads(enriched_leads, limit=len(enriched_leads), sleep_seconds=0.5)
    pd.DataFrame(scored_leads).to_csv(scored_path, index=False)

    # Step 4: Outreach
    top_scored = sorted(
        scored_leads,
        key=lambda row: _to_numeric_score(row.get("score")),
        reverse=True,
    )
    outreach_leads = generate_outreach_batch(top_scored, limit=len(top_scored), sleep_seconds=0.5)
    pd.DataFrame(outreach_leads).to_csv(outreach_path, index=False)

    # Step 5: Contactability
    ready_leads = evaluate_contactability_batch(outreach_leads)
    pd.DataFrame(ready_leads).to_csv(ready_path, index=False)

    ready_count = sum(1 for row in ready_leads if row.get("contactability_status") == "ready")
    review_count = sum(1 for row in ready_leads if row.get("contactability_status") == "review")

    return {
        "query": query,
        "raw_leads": raw_leads,
        "enriched_leads": enriched_leads,
        "scored_leads": scored_leads,
        "outreach_leads": outreach_leads,
        "ready_leads": ready_leads,
        "raw_path": raw_path,
        "enriched_path": enriched_path,
        "scored_path": scored_path,
        "outreach_path": outreach_path,
        "ready_path": ready_path,
        "summary": {
            "raw_count": len(raw_leads),
            "enriched_count": len(enriched_leads),
            "scored_count": len(scored_leads),
            "outreach_count": len(outreach_leads),
            "ready_count": ready_count,
            "review_count": review_count,
        },
    }