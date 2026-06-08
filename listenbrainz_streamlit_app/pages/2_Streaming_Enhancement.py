"""
Streamlit Page: Real-Time Streaming Enhancement

Place this file inside your Streamlit app folder:

listenbrainz_streamlit_app/
└── pages/
    └── 2_Streaming_Enhancement.py

This page explains the optional Kafka + Spark Streaming enhancement for the
ListenBrainz music listening analytics project.
"""

import streamlit as st


st.set_page_config(
    page_title="Streaming Enhancement",
    page_icon="⚡",
    layout="wide"
)

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
        box-shadow: 0 10px 28px rgba(30, 64, 175, 0.22);
    }

    .hero-title {
        font-size: 2.1rem;
        font-weight: 800;
        margin-bottom: 0.3rem;
    }

    .hero-subtitle {
        font-size: 1rem;
        color: #DBEAFE;
        line-height: 1.55;
    }

    .note-card {
        background-color: #EFF6FF;
        border: 1px solid #BFDBFE;
        padding: 1rem;
        border-radius: 14px;
        color: #1E3A8A;
        margin-top: 0.8rem;
        margin-bottom: 0.8rem;
    }

    .success-card {
        background-color: #ECFDF5;
        border: 1px solid #A7F3D0;
        padding: 1rem;
        border-radius: 14px;
        color: #065F46;
        margin-top: 0.8rem;
        margin-bottom: 0.8rem;
    }

    .warning-card {
        background-color: #FFFBEB;
        border: 1px solid #FDE68A;
        padding: 1rem;
        border-radius: 14px;
        color: #92400E;
        margin-top: 0.8rem;
        margin-bottom: 0.8rem;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

st.markdown(
    """
    <div class="hero-card">
        <div class="hero-title">⚡ Optional Enhancement: Kafka + Spark Streaming</div>
        <div class="hero-subtitle">
            Extend the ListenBrainz batch analytics pipeline into a near real-time streaming architecture
            for high-volume music listening events.
        </div>
    </div>
    """,
    unsafe_allow_html=True,
)

st.header("1. Why streaming enhancement is useful")

st.write(
    """
Music listening events can happen at any time of the day and may arrive in very high volume.
Popular songs, viral artists, and new release campaigns can generate many listening events within
a short period. A batch-only pipeline is useful for daily reporting, while a streaming pipeline
allows the business to monitor trends almost immediately.
"""
)

col1, col2, col3 = st.columns(3)

with col1:
    st.metric("Use Case", "Real-time trends")

with col2:
    st.metric("Data Type", "Listening events")

with col3:
    st.metric("Processing Style", "Streaming + Batch")

st.divider()

st.header("2. Proposed Streaming Architecture")

st.code(
    """
ListenBrainz Listening Events
        ↓
Kafka Topic: listenbrainz-listens
        ↓
Spark Structured Streaming
        ↓
Real-time Cleaning and Aggregation
        ↓
BigQuery Streaming Tables
        ↓
Streamlit Real-time Dashboard
        ↓
Alerts / Business Monitoring
    """,
    language="text"
)

st.write(
    """
Kafka acts as the event broker and receives listening events as messages. Spark Structured
Streaming consumes the Kafka messages, cleans the data, aggregates listening activity, and writes
the results into BigQuery. Streamlit can then read from BigQuery to show near real-time dashboards.
"""
)

st.divider()

st.header("3. Component Responsibilities")

components = [
    {
        "Component": "Kafka",
        "Responsibility": "Receives and buffers high-volume listening events",
        "Reason": "Suitable for real-time event streaming and decoupling producers from consumers"
    },
    {
        "Component": "Spark Structured Streaming",
        "Responsibility": "Processes events continuously",
        "Reason": "Can handle large-scale streaming transformations and aggregations"
    },
    {
        "Component": "BigQuery",
        "Responsibility": "Stores raw, cleaned, and aggregated streaming outputs",
        "Reason": "Supports scalable analytics and dashboard queries"
    },
    {
        "Component": "dbt",
        "Responsibility": "Maintains the batch star schema models",
        "Reason": "Provides clean dimensional models for historical reporting"
    },
    {
        "Component": "Streamlit",
        "Responsibility": "Displays real-time and historical insights",
        "Reason": "Easy dashboard layer for business users"
    }
]

st.dataframe(components, use_container_width=True, hide_index=True)

st.divider()

st.header("4. Real-Time Metrics to Monitor")

metrics = [
    {
        "Metric": "Top songs in the last 5 minutes",
        "Purpose": "Detect viral or trending songs quickly"
    },
    {
        "Metric": "Top artists in the last hour",
        "Purpose": "Identify rapidly growing artist activity"
    },
    {
        "Metric": "Listening events per minute",
        "Purpose": "Monitor platform activity and event volume"
    },
    {
        "Metric": "Peak listening time",
        "Purpose": "Understand when users are most active"
    },
    {
        "Metric": "Failed or invalid events",
        "Purpose": "Detect data quality or ingestion issues"
    },
    {
        "Metric": "Known releases trending",
        "Purpose": "Monitor release_name only when release metadata is available"
    }
]

st.dataframe(metrics, use_container_width=True, hide_index=True)

st.markdown(
    """
    <div class="note-card">
        <b>Metadata note:</b> release_name is optional in ListenBrainz. In a streaming design,
        missing release_name should be monitored as metadata completeness, while valid events
        should still be used for song, artist, user, and time analysis.
    </div>
    """,
    unsafe_allow_html=True,
)

st.divider()

st.header("5. Example Kafka Message Format")

st.code(
    """
{
  "user_name": "listener_123",
  "track_name": "Example Song",
  "artist_name": "Example Artist",
  "release_name": "Example Album",
  "recording_msid": "abc123",
  "listened_at": "2026-06-08T10:15:30Z"
}
    """,
    language="json"
)

st.divider()

st.header("6. Example Spark Structured Streaming Logic")

st.code(
    """
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count, trim
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

spark = SparkSession.builder.appName("listenbrainz-streaming").getOrCreate()

schema = StructType([
    StructField("user_name", StringType()),
    StructField("track_name", StringType()),
    StructField("artist_name", StringType()),
    StructField("release_name", StringType()),
    StructField("recording_msid", StringType()),
    StructField("listened_at", TimestampType())
])

raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "KAFKA_BOOTSTRAP_SERVER")
    .option("subscribe", "listenbrainz-listens")
    .load()
)

parsed_stream = (
    raw_stream
    .selectExpr("CAST(value AS STRING) AS json_value")
    .select(from_json(col("json_value"), schema).alias("data"))
    .select("data.*")
    .filter(col("user_name").isNotNull())
    .filter(col("track_name").isNotNull() & (trim(col("track_name")) != ""))
    .filter(col("artist_name").isNotNull() & (trim(col("artist_name")) != ""))
    .filter(col("recording_msid").isNotNull())
)

top_songs_stream = (
    parsed_stream
    .withWatermark("listened_at", "10 minutes")
    .groupBy(
        window(col("listened_at"), "5 minutes"),
        col("track_name"),
        col("artist_name")
    )
    .agg(count("*").alias("total_listens"))
)

query = (
    top_songs_stream.writeStream
    .format("bigquery")
    .option("table", "my-project-sssint1.listenbrainz_gcp.realtime_top_songs")
    .option("checkpointLocation", "gs://your-bucket/checkpoints/listenbrainz")
    .outputMode("append")
    .start()
)

query.awaitTermination()
    """,
    language="python"
)

st.divider()

st.header("7. Batch and Streaming Hybrid Design")

hybrid = [
    {
        "Layer": "Batch pipeline",
        "Technology": "BigQuery + dbt + Dagster",
        "Purpose": "Historical reporting, star schema, data quality, dashboard analytics"
    },
    {
        "Layer": "Streaming pipeline",
        "Technology": "Kafka + Spark Structured Streaming",
        "Purpose": "Near real-time monitoring of top songs, artists, and event volume"
    },
    {
        "Layer": "Dashboard",
        "Technology": "Streamlit + Cloud Run",
        "Purpose": "Display both historical and real-time insights"
    }
]

st.dataframe(hybrid, use_container_width=True, hide_index=True)

st.divider()

st.header("8. Benefits")

st.markdown(
    """
    <div class="success-card">
        Adding Kafka and Spark Streaming improves the system by allowing the business to monitor
        music listening behaviour as it happens. This supports faster detection of trending songs,
        higher responsiveness to traffic spikes, and better operational visibility for a music
        streaming-style platform.
    </div>
    """,
    unsafe_allow_html=True,
)

st.header("9. Limitations")

st.markdown(
    """
    <div class="warning-card">
        This enhancement increases system complexity. Kafka and Spark require additional infrastructure,
        monitoring, checkpointing, and error handling. For the current assignment, this is recommended
        as an optional future enhancement rather than the main implementation.
    </div>
    """,
    unsafe_allow_html=True,
)

st.divider()

st.header("10. Report Wording")

st.markdown(
    """
**Optional Enhancement:**  
Kafka and Spark Streaming can be incorporated to support near real-time processing of music
listening events. Since music listening can occur at any time and event volume can become very
large when popular songs are played repeatedly, a streaming architecture would allow the system
to process and monitor events continuously. Kafka can be used as the message broker to ingest
listening events, while Spark Structured Streaming can clean, aggregate, and write real-time
metrics into BigQuery. This enhancement would support real-time dashboards showing top songs,
top artists, listening events per minute, and trending known releases.
"""
)
