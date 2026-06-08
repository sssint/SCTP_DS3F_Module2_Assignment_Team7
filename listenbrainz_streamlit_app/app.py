"""
ListenBrainz Analytics Dashboard
Streamlit + BigQuery + dbt Star Schema + Cloud Run

Main features:
- Executive summary dashboard
- Top artists, songs, users, and releases
- Listening pattern by time of day
- Monthly trend for latest 24 months
- Clean UI with sidebar navigation
- BigQuery error handling
"""

import pandas as pd
import streamlit as st
from google.cloud import bigquery


# =========================================================
# Page configuration
# =========================================================

st.set_page_config(
    page_title="ListenBrainz Analytics Dashboard",
    page_icon="🎧",
    layout="wide",
    initial_sidebar_state="expanded",
)


# =========================================================
# Custom styling
# =========================================================

st.markdown(
    """
    <style>
    .main {
        background-color: #F8FAFC;
    }

    .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
    }

    .hero-card {
        background: linear-gradient(135deg, #111827 0%, #312E81 45%, #1D4ED8 100%);
        padding: 2rem;
        border-radius: 18px;
        color: white;
        margin-bottom: 1.5rem;
    }

    .hero-title {
        font-size: 2.2rem;
        font-weight: 800;
        margin-bottom: 0.3rem;
    }

    .hero-subtitle {
        font-size: 1rem;
        color: #DBEAFE;
    }

    .metric-card {
        background-color: white;
        padding: 1.2rem;
        border-radius: 16px;
        box-shadow: 0 4px 14px rgba(15, 23, 42, 0.08);
        border: 1px solid #E5E7EB;
    }

    .section-card {
        background-color: white;
        padding: 1.2rem;
        border-radius: 16px;
        box-shadow: 0 4px 14px rgba(15, 23, 42, 0.06);
        border: 1px solid #E5E7EB;
        margin-bottom: 1rem;
    }

    .small-muted {
        color: #64748B;
        font-size: 0.9rem;
    }

    div[data-testid="stMetricValue"] {
        font-size: 1.8rem;
        font-weight: 800;
    }

    div[data-testid="stMetricLabel"] {
        font-size: 0.9rem;
        color: #475569;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


# =========================================================
# Configuration
# =========================================================

PROJECT_ID = "my-project-sssint1"
DATASET = "listenbrainz_gcp"


# =========================================================
# BigQuery helpers
# =========================================================

@st.cache_resource
def get_bigquery_client():
    """Create a BigQuery client.

    On Cloud Run, authentication uses the Cloud Run service account.
    Do not set GOOGLE_APPLICATION_CREDENTIALS in app.py for Cloud Run.
    """
    return bigquery.Client(project=PROJECT_ID)


@st.cache_data(ttl=3600, show_spinner=False)
def run_query(sql: str) -> pd.DataFrame:
    """Run BigQuery SQL and return a pandas DataFrame."""
    client = get_bigquery_client()
    return client.query(sql).to_dataframe()


def safe_run_query(name: str, sql: str) -> pd.DataFrame:
    """Run query with Streamlit-friendly error handling."""
    try:
        return run_query(sql)
    except Exception as exc:
        st.error(f"Failed to load: {name}")
        st.exception(exc)
        return pd.DataFrame()


# =========================================================
# SQL queries
# =========================================================

query_summary = f"""
SELECT
    COUNT(*) AS total_listens,
    COUNT(DISTINCT user_id) AS total_users,
    COUNT(DISTINCT track_id) AS total_tracks,
    COUNT(DISTINCT artist_id) AS total_artists,
    COUNT(DISTINCT album_id) AS total_releases
FROM `{PROJECT_ID}.{DATASET}.fact_listening_events`
"""

query_top_artists = f"""
SELECT
    ar.artist_name,
    COUNT(*) AS total_listens
FROM `{PROJECT_ID}.{DATASET}.fact_listening_events` f
LEFT JOIN `{PROJECT_ID}.{DATASET}.dim_artist` ar
    ON f.artist_id = ar.artist_id
WHERE ar.artist_name IS NOT NULL
GROUP BY ar.artist_name
ORDER BY total_listens DESC
LIMIT 10
"""

query_top_songs = f"""
SELECT
    t.track_name,
    t.artist_name,
    COUNT(*) AS total_listens
FROM `{PROJECT_ID}.{DATASET}.fact_listening_events` f
LEFT JOIN `{PROJECT_ID}.{DATASET}.dim_track` t
    ON f.track_id = t.track_id
WHERE t.track_name IS NOT NULL
GROUP BY
    t.track_name,
    t.artist_name
ORDER BY total_listens DESC
LIMIT 10
"""

query_time_of_day = f"""
SELECT
    tm.time_period,
    COUNT(*) AS total_listens
FROM `{PROJECT_ID}.{DATASET}.fact_listening_events` f
LEFT JOIN `{PROJECT_ID}.{DATASET}.dim_time` tm
    ON f.time_id = tm.time_id
WHERE tm.time_period IS NOT NULL
GROUP BY tm.time_period
ORDER BY total_listens DESC
"""

query_listens_by_hour = f"""
SELECT
    tm.hour,
    COUNT(*) AS total_listens
FROM `{PROJECT_ID}.{DATASET}.fact_listening_events` f
LEFT JOIN `{PROJECT_ID}.{DATASET}.dim_time` tm
    ON f.time_id = tm.time_id
WHERE tm.hour IS NOT NULL
GROUP BY tm.hour
ORDER BY tm.hour
"""

query_monthly_trend = f"""
SELECT
    d.year,
    d.month,
    COUNT(*) AS total_listens
FROM `{PROJECT_ID}.{DATASET}.fact_listening_events` f
LEFT JOIN `{PROJECT_ID}.{DATASET}.dim_date` d
    ON f.date_id = d.date_id
WHERE d.year IS NOT NULL
  AND d.month IS NOT NULL
GROUP BY
    d.year,
    d.month
ORDER BY
    d.year,
    d.month
"""

query_top_users = f"""
SELECT
    u.user_name,
    COUNT(*) AS total_listens
FROM `{PROJECT_ID}.{DATASET}.fact_listening_events` f
LEFT JOIN `{PROJECT_ID}.{DATASET}.dim_user` u
    ON f.user_id = u.user_id
WHERE u.user_name IS NOT NULL
GROUP BY u.user_name
ORDER BY total_listens DESC
LIMIT 10
"""

# IMPORTANT:
# dim_album has columns: album_id, release_name, artist_name
# Use release_name, NOT album_name.
query_top_releases = f"""
SELECT
    al.release_name,
    al.artist_name,
    COUNT(*) AS total_listens
FROM `{PROJECT_ID}.{DATASET}.fact_listening_events` f
LEFT JOIN `{PROJECT_ID}.{DATASET}.dim_album` al
    ON f.album_id = al.album_id
WHERE al.release_name IS NOT NULL
GROUP BY
    al.release_name,
    al.artist_name
ORDER BY total_listens DESC
LIMIT 10
"""


# =========================================================
# Sidebar
# =========================================================

with st.sidebar:
    st.title("🎧 ListenBrainz")
    st.caption("Analytics Dashboard")

    page = st.radio(
        "Navigate",
        [
            "Executive Overview",
            "Artists & Songs",
            "Listening Patterns",
            "Users & Releases",
            "Data Pipeline",
        ],
    )

    st.divider()

    st.subheader("Project Info")
    st.write(f"**Project:** `{PROJECT_ID}`")
    st.write(f"**Dataset:** `{DATASET}`")
    st.write("**Warehouse:** BigQuery")
    st.write("**Transform:** dbt")
    st.write("**Deploy:** Cloud Run")

    st.divider()
    st.caption("Data is cached for 1 hour.")


# =========================================================
# Header
# =========================================================

st.markdown(
    """
    <div class="hero-card">
        <div class="hero-title">ListenBrainz Music Analytics Dashboard</div>
        <div class="hero-subtitle">
            BigQuery star schema dashboard for music listening trends, popular artists,
            top songs, active users, and trending releases.
        </div>
    </div>
    """,
    unsafe_allow_html=True,
)


# =========================================================
# Load data
# =========================================================

with st.spinner("Loading dashboard data from BigQuery..."):
    summary = safe_run_query("summary metrics", query_summary)
    top_artists = safe_run_query("top artists", query_top_artists)
    top_songs = safe_run_query("top songs", query_top_songs)
    time_of_day = safe_run_query("time of day", query_time_of_day)
    listens_by_hour = safe_run_query("listens by hour", query_listens_by_hour)
    monthly_trend = safe_run_query("monthly trend", query_monthly_trend)
    top_users = safe_run_query("top users", query_top_users)
    top_releases = safe_run_query("top releases", query_top_releases)


# =========================================================
# Common prepared data
# =========================================================

if not monthly_trend.empty:
    monthly_trend_plot = monthly_trend.copy()
    monthly_trend_plot["year_month_date"] = pd.to_datetime(
        monthly_trend_plot["year"].astype(str)
        + "-"
        + monthly_trend_plot["month"].astype(str).str.zfill(2)
        + "-01"
    )
    monthly_trend_plot = monthly_trend_plot.sort_values("year_month_date").tail(24)
    monthly_trend_plot["year_month"] = monthly_trend_plot["year_month_date"].dt.strftime("%Y-%m")
else:
    monthly_trend_plot = pd.DataFrame()

if not top_songs.empty:
    top_songs_plot = top_songs.copy()
    top_songs_plot["song_label"] = (
        top_songs_plot["track_name"].astype(str)
        + " - "
        + top_songs_plot["artist_name"].fillna("Unknown").astype(str)
    )
else:
    top_songs_plot = pd.DataFrame()

if not top_releases.empty:
    top_releases_plot = top_releases.copy()
    top_releases_plot["release_label"] = (
        top_releases_plot["release_name"].astype(str)
        + " - "
        + top_releases_plot["artist_name"].fillna("Unknown").astype(str)
    )
else:
    top_releases_plot = pd.DataFrame()


# =========================================================
# Page: Executive Overview
# =========================================================

if page == "Executive Overview":
    st.subheader("Executive Overview")

    if not summary.empty:
        row = summary.iloc[0]

        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Total listens", f"{int(row['total_listens']):,}")
        c2.metric("Users", f"{int(row['total_users']):,}")
        c3.metric("Tracks", f"{int(row['total_tracks']):,}")
        c4.metric("Artists", f"{int(row['total_artists']):,}")
        c5.metric("Releases", f"{int(row['total_releases']):,}")

    st.markdown("### Key Insights")

    left, right = st.columns(2)

    with left:
        st.markdown("#### Top Artists")
        if not top_artists.empty:
            st.bar_chart(top_artists.set_index("artist_name")["total_listens"])
            st.dataframe(top_artists, use_container_width=True, hide_index=True)
        else:
            st.info("No artist data available.")

    with right:
        st.markdown("#### Latest 24 Months Trend")
        if not monthly_trend_plot.empty:
            st.line_chart(monthly_trend_plot.set_index("year_month")["total_listens"])
            st.dataframe(monthly_trend_plot[["year_month", "total_listens"]], use_container_width=True, hide_index=True)
        else:
            st.info("No monthly trend data available.")

    st.markdown("### Business Value")
    st.info(
        "This dashboard helps stakeholders identify popular artists, songs, releases, "
        "active listeners, and listening patterns over time. It demonstrates how the "
        "BigQuery star schema supports fast analytical reporting."
    )


# =========================================================
# Page: Artists & Songs
# =========================================================

elif page == "Artists & Songs":
    st.subheader("Artists & Songs")

    tab1, tab2 = st.tabs(["Top Artists", "Top Songs"])

    with tab1:
        st.markdown("### Which artists are most listened to?")
        if not top_artists.empty:
            st.bar_chart(top_artists.set_index("artist_name")["total_listens"])
            st.dataframe(top_artists, use_container_width=True, hide_index=True)
        else:
            st.info("No top artists found.")

    with tab2:
        st.markdown("### Which songs are most popular?")
        if not top_songs_plot.empty:
            st.bar_chart(top_songs_plot.set_index("song_label")["total_listens"])
            st.dataframe(top_songs, use_container_width=True, hide_index=True)
        else:
            st.info("No top songs found.")


# =========================================================
# Page: Listening Patterns
# =========================================================

elif page == "Listening Patterns":
    st.subheader("Listening Patterns")

    tab1, tab2, tab3 = st.tabs(["Time of Day", "Hourly Pattern", "Monthly Trend"])

    with tab1:
        st.markdown("### What time of day do users listen most?")
        if not time_of_day.empty:
            st.bar_chart(time_of_day.set_index("time_period")["total_listens"])
            st.dataframe(time_of_day, use_container_width=True, hide_index=True)
        else:
            st.info("No time period data available.")

    with tab2:
        st.markdown("### Listening by hour")
        if not listens_by_hour.empty:
            st.line_chart(listens_by_hour.set_index("hour")["total_listens"])
            st.dataframe(listens_by_hour, use_container_width=True, hide_index=True)
        else:
            st.info("No hourly data available.")

    with tab3:
        st.markdown("### Monthly listening trend - latest 24 months")
        if not monthly_trend_plot.empty:
            st.line_chart(monthly_trend_plot.set_index("year_month")["total_listens"])
            st.dataframe(monthly_trend_plot[["year_month", "total_listens"]], use_container_width=True, hide_index=True)
        else:
            st.info("No monthly trend data available.")


# =========================================================
# Page: Users & Releases
# =========================================================

elif page == "Users & Releases":
    st.subheader("Users & Releases")

    tab1, tab2 = st.tabs(["Top Users", "Trending Releases"])

    with tab1:
        st.markdown("### Which users listen most often?")
        if not top_users.empty:
            st.bar_chart(top_users.set_index("user_name")["total_listens"])
            st.dataframe(top_users, use_container_width=True, hide_index=True)
        else:
            st.info("No top users found.")

    with tab2:
        st.markdown("### Which releases are trending?")
        if not top_releases_plot.empty:
            st.bar_chart(top_releases_plot.set_index("release_label")["total_listens"])
            st.dataframe(top_releases, use_container_width=True, hide_index=True)
        else:
            st.info("No top releases found.")


# =========================================================
# Page: Data Pipeline
# =========================================================

elif page == "Data Pipeline":
    st.subheader("Data Pipeline Architecture")

    st.markdown(
        """
        The ListenBrainz analytics system follows a modern data engineering pipeline.

        ```text
        ListenBrainz Raw Data
                ↓
        BigQuery Raw Table
                ↓
        dbt Staging Model
                ↓
        dbt Star Schema
                ↓
        Data Quality Checks
                ↓
        Streamlit Dashboard
                ↓
        Cloud Run Deployment
        ```
        """
    )

    st.markdown("### Star Schema")

    st.code(
        """
                    dim_date
                       |
        dim_user -- fact_listening_events -- dim_track
                       |
                   dim_artist
                       |
                   dim_album
                       |
                   dim_time
        """,
        language="text",
    )

    st.markdown("### Why star schema?")

    st.write(
        """
        A star schema was chosen because it supports efficient analytical queries.
        The fact table stores listening events, while dimensions store descriptive
        data such as user, track, artist, release, date, and time. This makes it
        easier to answer business questions using simple joins and aggregations.
        """
    )

    st.markdown("### Optional Streaming Enhancement")

    st.write(
        """
        Kafka and Spark Structured Streaming can be added as a future enhancement
        to process high-volume music listening events in near real time. This would
        support real-time monitoring of top songs, top artists, and event volume.
        """
    )


# =========================================================
# Footer
# =========================================================

st.divider()
st.caption(
    "ListenBrainz Analytics Dashboard | BigQuery + dbt + Streamlit + Cloud Run"
)
