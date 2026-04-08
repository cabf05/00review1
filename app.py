"""
Google Maps Review Scraper – Streamlit App

Paste a Google Maps URL, set the date range, and download all reviews
as CSV or JSON.
"""

import streamlit as st
import pandas as pd
import json

from scraper import ReviewScraper, install_browser

# ── Page config ──
st.set_page_config(
    page_title="Google Maps Review Scraper",
    page_icon="🗺️",
    layout="wide",
)


# ── One-time browser setup (cached across reruns) ──
@st.cache_resource(show_spinner="Installing browser engine (first run only)...")
def _setup():
    install_browser()
    return True


_setup()


# ── UI ──
st.title("Google Maps Review Scraper")
st.markdown(
    "Paste any Google Maps link below, choose how far back to look, "
    "and get all the reviews in a table you can download."
)

col_url, col_days = st.columns([3, 1])
with col_url:
    url = st.text_input(
        "Google Maps URL",
        placeholder="https://maps.app.goo.gl/... or full Google Maps URL",
    )
with col_days:
    max_days = st.number_input(
        "Days to look back",
        min_value=1,
        max_value=10_000,
        value=2_000,
        help="Only reviews from the last N days will be collected.",
    )

run = st.button("Scrape Reviews", type="primary", use_container_width=True)

# ── Scraping logic ──
if run:
    # Basic input validation
    if not url:
        st.error("Please enter a Google Maps URL.")
        st.stop()
    if not any(k in url for k in ("google", "goo.gl", "maps")):
        st.error("The URL doesn't look like a Google Maps link. Please check it.")
        st.stop()

    status = st.empty()
    progress = st.progress(0, text="Starting...")

    step = {"n": 0}

    def _on_progress(msg):
        step["n"] = min(step["n"] + 8, 95)
        progress.progress(step["n"], text=msg)

    try:
        scraper = ReviewScraper()
        reviews = scraper.scrape(
            url, max_days=int(max_days), on_progress=_on_progress
        )

        progress.progress(100, text="Complete!")
        status.empty()

    except Exception as exc:
        progress.empty()
        st.error(f"Scraping failed: {exc}")
        st.stop()

    # ── Results ──
    if not reviews:
        st.warning("No reviews found. Please check the URL and try again.")
        st.stop()

    place_name = reviews[0].get("title", "Unknown Place")
    st.success(f"Collected **{len(reviews)}** reviews for **{place_name}**")

    # Summary table (user-friendly columns)
    df = pd.DataFrame(reviews)
    friendly = ["name", "stars", "text", "publishAt", "isLocalGuide", "likesCount"]
    show_cols = [c for c in friendly if c in df.columns]
    st.dataframe(df[show_cols] if show_cols else df, use_container_width=True)

    # Download buttons
    col_csv, col_json = st.columns(2)
    with col_csv:
        st.download_button(
            label="Download CSV",
            data=df.to_csv(index=False),
            file_name=f"reviews_{place_name[:30].replace(' ', '_')}.csv",
            mime="text/csv",
            use_container_width=True,
        )
    with col_json:
        st.download_button(
            label="Download JSON",
            data=json.dumps(reviews, indent=2, ensure_ascii=False),
            file_name=f"reviews_{place_name[:30].replace(' ', '_')}.json",
            mime="application/json",
            use_container_width=True,
        )

    # Full data (expandable)
    with st.expander("Show all columns"):
        st.dataframe(df, use_container_width=True)
