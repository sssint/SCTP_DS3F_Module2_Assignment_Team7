import os

import pandas as pd
import streamlit as st
from google.cloud import bigquery


PROJECT_ID = "my-project-sssint1"
DATASET = "listenbrainz_gcp"

st.set_page_config(
    page_title="ListenBrainz Analytics Dashboard",
    page_icon="🎧",
    layout="wide"
)

st.title("🎧 ListenBrainz Analytics Dashboard")
st.write("BigQuery + dbt Star Schema + Streamlit + Cloud Run")


@st.cache_resource
def get_bigquery_client():
    return bigquery.Client(project=PROJECT_ID)


@st.cache_data(ttl=3600)
def run_query(sql: str) -> pd.DataFrame:
    client = get_bigquery_client()
    return client.query(sql).to_dataframe()


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

query_top_releases = f"""
SELECT
    al.album_name AS release_name,
    al.artist_name,
    COUNT(*) AS total_listens
FROM `{PROJECT_ID}.{DATASET}.fact_listening_events` f
LEFT JOIN `{PROJECT_ID}.{DATASET}.dim_album` al
    ON f.album_id = al.album_id
WHERE al.album_name IS NOT NULL
GROUP BY
    release_name,
    al.artist_name
ORDER BY total_listens DESC
LIMIT 10
"""


with st.spinner("Loading data from BigQuery..."):
    top_artists = run_query(query_top_artists)
    top_songs = run_query(query_top_songs)
    time_of_day = run_query(query_time_of_day)
    monthly_trend = run_query(query_monthly_trend)
    top_users = run_query(query_top_users)
    top_releases = run_query(query_top_releases)


col1, col2, col3 = st.columns(3)

with col1:
    st.metric("Top artists", len(top_artists))

with col2:
    st.metric("Top songs", len(top_songs))

with col3:
    st.metric("Top users", len(top_users))


st.divider()

tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "Top Artists",
    "Top Songs",
    "Time of Day",
    "Monthly Trend",
    "Top Users",
    "Top Releases"
])

with tab1:
    st.subheader("Which artists are most listened to?")
    st.dataframe(top_artists, use_container_width=True)
    st.bar_chart(top_artists.set_index("artist_name")["total_listens"])

with tab2:
    st.subheader("Which songs are most popular?")
    top_songs["song_label"] = top_songs["track_name"] + " - " + top_songs["artist_name"].fillna("Unknown")
    st.dataframe(top_songs, use_container_width=True)
    st.bar_chart(top_songs.set_index("song_label")["total_listens"])

with tab3:
    st.subheader("What time of day do users listen most?")
    st.dataframe(time_of_day, use_container_width=True)
    st.bar_chart(time_of_day.set_index("time_period")["total_listens"])

with tab4:
    st.subheader("How does listening change monthly?")

    monthly_trend_plot = monthly_trend.copy()
    monthly_trend_plot["year_month_date"] = pd.to_datetime(
        monthly_trend_plot["year"].astype(str) + "-"
        + monthly_trend_plot["month"].astype(str).str.zfill(2) + "-01"
    )
    monthly_trend_plot = monthly_trend_plot.sort_values("year_month_date").tail(24)
    monthly_trend_plot["year_month"] = monthly_trend_plot["year_month_date"].dt.strftime("%Y-%m")

    st.dataframe(monthly_trend_plot, use_container_width=True)
    st.line_chart(monthly_trend_plot.set_index("year_month")["total_listens"])

with tab5:
    st.subheader("Which users listen most often?")
    st.dataframe(top_users, use_container_width=True)
    st.bar_chart(top_users.set_index("user_name")["total_listens"])

with tab6:
    st.subheader("Which releases are trending?")
    top_releases["release_label"] = top_releases["release_name"] + " - " + top_releases["artist_name"].fillna("Unknown")
    st.dataframe(top_releases, use_container_width=True)
    st.bar_chart(top_releases.set_index("release_label")["total_listens"])