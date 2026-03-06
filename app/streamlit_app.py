import sys
from pathlib import Path

from geopy.geocoders import Nominatim

import pydeck as pdk
import pandas as pd
import streamlit as st
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.enrichment.website_enricher import enrich_leads
from src.lead_sources.serpapi_maps import save_leads_to_csv, search_google_maps
from src.outreach.contactability import evaluate_contactability_batch
from src.outreach.email_generator import generate_outreach_batch
from src.scoring.lead_scorer import score_leads

load_dotenv()

geolocator = Nominatim(user_agent="ai_lead_generation_agents")


@st.cache_data(show_spinner=False)
def geocode_address(address: str):
    if not address or not str(address).strip():
        return None, None

    try:
        location = geolocator.geocode(address, timeout=10)
        if location:
            return location.latitude, location.longitude
    except Exception:
        pass

    return None, None

st.set_page_config(page_title="AI Lead Generation Agents", layout="wide")

st.markdown("""
<style>

.opportunity-card {
    border: 1px solid rgba(255,255,255,0.12);
    border-radius: 16px;
    padding: 18px;
    margin-bottom: 16px;
    background: rgba(255,255,255,0.02);
    transition: transform 0.18s ease, box-shadow 0.18s ease;
}

.opportunity-card:hover {
    transform: translateY(-3px);
    box-shadow: 0 12px 28px rgba(0,0,0,0.35);
}

</style>
""", unsafe_allow_html=True)

st.title("AI Lead Generation Agents")
st.caption(
    "Discover local businesses, analyze websites, score opportunities, and generate outreach."
)

with st.sidebar:
    st.header("Search Settings")
    niche = st.text_input("Niche", value="dentists")
    city = st.text_input("City", value="miami")
    max_results = st.number_input("Max Leads", min_value=1, max_value=100, value=5)
    min_score = st.slider("Minimum Score", min_value=1, max_value=10, value=5)
    export_mode = st.selectbox("Export Mode",["Outreach Ready", "Lead List Only", "CRM Upload"],)
    run_clicked = st.button("Run Full Pipeline", use_container_width=True)

if run_clicked:
    try:
        data_dir = PROJECT_ROOT / "data"
        data_dir.mkdir(exist_ok=True)

        raw_path = data_dir / "leads_raw.csv"
        enriched_path = data_dir / "leads_enriched.csv"
        scored_path = data_dir / "leads_scored.csv"
        outreach_path = data_dir / "leads_outreach.csv"
        ready_path = data_dir / "leads_ready.csv"

        progress_bar = st.progress(0)
        status_text = st.empty()

        query = f"{niche} in {city}"

        status_text.info("Step 1/5: Discovering leads...")
        raw_leads = search_google_maps(query=query, max_results=int(max_results))
        save_leads_to_csv(raw_leads, str(raw_path))
        progress_bar.progress(20)

        status_text.info("Step 2/5: Enriching websites...")
        enriched_leads = enrich_leads(raw_leads, limit=len(raw_leads), sleep_seconds=1.0)
        pd.DataFrame(enriched_leads).to_csv(enriched_path, index=False)
        progress_bar.progress(40)

        status_text.info("Step 3/5: Scoring leads with AI...")
        scored_leads = score_leads(enriched_leads, limit=len(enriched_leads), sleep_seconds=0.5)
        pd.DataFrame(scored_leads).to_csv(scored_path, index=False)
        progress_bar.progress(60)

        status_text.info("Step 4/5: Generating outreach...")
        outreach_leads = generate_outreach_batch(
            scored_leads, limit=len(scored_leads), sleep_seconds=0.5
        )
        pd.DataFrame(outreach_leads).to_csv(outreach_path, index=False)
        progress_bar.progress(80)

        status_text.info("Step 5/5: Running contactability review...")
        ready_leads = evaluate_contactability_batch(outreach_leads)
        pd.DataFrame(ready_leads).to_csv(ready_path, index=False)
        progress_bar.progress(100)

        status_text.success(f"Pipeline complete for: {query}")

        ready_count = sum(
            1 for row in ready_leads if row.get("contactability_status") == "ready"
        )
        review_count = sum(
            1 for row in ready_leads if row.get("contactability_status") == "review"
        )

        df = pd.DataFrame(ready_leads)

        if "score" in df.columns:
            df["score_numeric"] = pd.to_numeric(df["score"], errors="coerce")
            filtered_df = df[df["score_numeric"] >= min_score].copy()
            filtered_df = filtered_df.sort_values(by="score_numeric", ascending=False)
        else:
            filtered_df = df.copy()

        st.subheader("Pipeline Summary")
        m1, m2, m3, m4, m5, m6 = st.columns(6)
        m1.metric("Raw", len(raw_leads))
        m2.metric("Enriched", len(enriched_leads))
        m3.metric("Scored", len(scored_leads))
        m4.metric("Outreach", len(outreach_leads))
        m5.metric("Ready", ready_count)
        m6.metric("Review", review_count)

        st.markdown("---")
        st.subheader("Lead Map")

        map_df = filtered_df.copy()

        if "latitude" not in map_df.columns:
            map_df["latitude"] = None
        if "longitude" not in map_df.columns:
            map_df["longitude"] = None

        for idx, row in map_df.iterrows():
            if pd.isna(row.get("latitude")) or pd.isna(row.get("longitude")):
                lat, lon = geocode_address(row.get("address", ""))
                map_df.at[idx, "latitude"] = lat
                map_df.at[idx, "longitude"] = lon

        plotted_df = map_df.dropna(subset=["latitude", "longitude"]).copy()

        if not plotted_df.empty:
            plotted_df["tooltip_name"] = plotted_df["name"].fillna("Unknown Lead")
            plotted_df["tooltip_score"] = plotted_df["score"].fillna("N/A")
            plotted_df["tooltip_status"] = plotted_df["contactability_status"].fillna("unknown")
            plotted_df["tooltip_address"] = plotted_df["address"].fillna("")

            # Color markers by lead quality / status
            def marker_color(row):
                score = pd.to_numeric(row.get("score"), errors="coerce")
                status = str(row.get("contactability_status", "")).lower()

                if status == "review":
                    return [245, 158, 11, 220]  # amber
                if pd.notna(score) and score >= 7:
                    return [34, 197, 94, 220]   # green
                if pd.notna(score) and score >= 5:
                    return [59, 130, 246, 220]  # blue
                return [156, 163, 175, 220]     # gray

            plotted_df["marker_color"] = plotted_df.apply(marker_color, axis=1)

            # Offset overlapping coordinates slightly so stacked leads are visible
            coord_counts = {}
            adjusted_lats = []
            adjusted_lons = []

            for _, row in plotted_df.iterrows():
                key = (round(float(row["latitude"]), 5), round(float(row["longitude"]), 5))
                count = coord_counts.get(key, 0)

                lat_offset = 0.00025 * count
                lon_offset = 0.00025 * count

                adjusted_lats.append(float(row["latitude"]) + lat_offset)
                adjusted_lons.append(float(row["longitude"]) + lon_offset)

                coord_counts[key] = count + 1

            plotted_df["plot_latitude"] = adjusted_lats
            plotted_df["plot_longitude"] = adjusted_lons

            center_lat = plotted_df["plot_latitude"].mean()
            center_lon = plotted_df["plot_longitude"].mean()

            layer = pdk.Layer(
                "ScatterplotLayer",
                data=plotted_df,
                get_position="[plot_longitude, plot_latitude]",
                get_radius=220,
                get_fill_color="marker_color",
                pickable=True,
            )

            view_state = pdk.ViewState(
                latitude=center_lat,
                longitude=center_lon,
                zoom=12,
                pitch=0,
            )

            tooltip = {
                "html": """
                    <b>{tooltip_name}</b><br/>
                    Score: {tooltip_score}<br/>
                    Status: {tooltip_status}<br/>
                    {tooltip_address}
                """,
                "style": {
                    "backgroundColor": "rgba(15, 23, 42, 0.95)",
                    "color": "white",
                    "fontSize": "13px",
                    "padding": "10px",
                    "borderRadius": "8px",
                },
            }

            deck = pdk.Deck(
                layers=[layer],
                initial_view_state=view_state,
                tooltip=tooltip,
                map_style="dark",
            )

            st.pydeck_chart(deck, use_container_width=True)

            missing_count = len(map_df) - len(plotted_df)
            if missing_count > 0:
                st.caption(f"{missing_count} lead(s) could not be mapped because coordinates were not found.")

        else:
            st.info("Map coordinates could not be generated for the current filtered leads.")

        st.markdown("---")
        st.subheader("Top Opportunities")

        top_leads = filtered_df.head(5).to_dict(orient="records")

        for lead in top_leads:
            name = lead.get("name", "Unknown Lead")
            score = lead.get("score", "N/A")
            score_value = int(score) if str(score).isdigit() else 0
            if score_value >= 7:
                score_color = "#22c55e"
            elif score_value >= 5:
                score_color = "#3b82f6"
            else:
                score_color = "#f59e0b"
            status = lead.get("contactability_status", "unknown")
            opportunity = lead.get("opportunity", "No opportunity available.")
            website = lead.get("website", "")
            subject = lead.get("subject", "")

            status_color = "#16a34a" if status == "ready" else "#f59e0b"

            st.markdown(
                f"""
                <div class="opportunity-card"
                    border: 1px solid rgba(255,255,255,0.12);
                    border-radius: 16px;
                    padding: 18px;
                    margin-bottom: 16px;
                    background: rgba(255,255,255,0.02);
                ">
                    <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:10px;">
                        <div style="font-size:1.1rem; font-weight:700;">{name}</div>
                        <div style="display:flex; gap:8px;">
                            <span style="
                            background:{score_color};
                            padding:6px 10px;
                            border-radius:999px;
                            font-size:0.85rem;
                            font-weight:600;
                            color:white;
                            ">Score: {score}</span>
                            <span style="
                                background:{status_color};
                                color:white;
                                padding:6px 10px;
                                border-radius:999px;
                                font-size:0.85rem;
                                font-weight:600;
                            ">{status}</span>
                        </div>
                    </div>
                    <div style="margin-bottom:10px;">
                        <strong>Opportunity:</strong> {opportunity}
                    </div>
                    <div style="margin-bottom:10px;">
                        <strong>Website:</strong> <a href="{website}" target="_blank">{website}</a>
                    </div>
                    <div>
                        <strong>Suggested Subject:</strong> {subject}
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )

        st.markdown("---")
        st.subheader("Lead Details")

        for _, row in filtered_df.iterrows():
            company_name = row.get("name", "Unknown Lead")
            score = row.get("score", "N/A")
            status = row.get("contactability_status", "unknown")

            with st.expander(f"{company_name} | Score: {score} | Status: {status}"):
                left, right = st.columns([1, 1])

                with left:
                    st.write("**Website**")
                    st.write(row.get("website", ""))
                    st.write("**Opportunity**")
                    st.write(row.get("opportunity", ""))
                    st.write("**Subject**")
                    st.write(row.get("subject", ""))

                with right:
                    st.write("**Generated Email**")
                    st.text_area(
                        label=f"email_{company_name}",
                        value=row.get("email", ""),
                        height=180,
                        key=f"email_{company_name}",
                        label_visibility="collapsed",
                    )

            st.markdown("---")

        if export_mode == "Outreach Ready":
            export_columns = [
                "name",
                "website",
                "score",
                "opportunity",
                "subject",
                "email",
                "contactability_status",
            ]
            export_df = filtered_df[
                [c for c in export_columns if c in filtered_df.columns]
            ].copy()
            export_filename = "outreach_ready.csv"

        elif export_mode == "Lead List Only":
            export_columns = [
                "name",
                "address",
                "phone",
                "website",
                "rating",
                "reviews",
                "score",
                "contactability_status",
            ]
            export_df = filtered_df[
                [c for c in export_columns if c in filtered_df.columns]
            ].copy()
            export_filename = "lead_list_only.csv"

        else:  # CRM Upload
            export_df = filtered_df.copy()
            export_df = export_df.rename(
                columns={
                    "name": "company_name",
                    "opportunity": "notes",
                }
            )
            crm_columns = [
                "company_name",
                "website",
                "phone",
                "address",
                "score",
                "notes",
            ]
            export_df = export_df[
                [c for c in crm_columns if c in export_df.columns]
            ].copy()
            export_filename = "crm_upload.csv"

        csv_data = export_df.to_csv(index=False).encode("utf-8")

        st.download_button(
            label=f"Download {export_mode} CSV",
            data=csv_data,
            file_name=export_filename,
            mime="text/csv",
            use_container_width=True,
        )
    except Exception as e: 
        st.error(f"Pipeline failed: {e}")
    