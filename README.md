# ListenBrainz Analytics Engineering Project

## 1. Project Overview

This repository contains an end-to-end analytics engineering project using the ListenBrainz music listening dataset.

The project demonstrates how raw listening event data can be transformed into a clean star schema, validated through data quality checks, orchestrated with Dagster, and visualised using a Streamlit dashboard deployed on Google Cloud Run.

The three main project components are:

1. **dbt Project** — transforms raw BigQuery data into a star schema.
2. **Dagster Project** — orchestrates the data pipeline and downstream assets.
3. **Streamlit Project** — provides an interactive dashboard for business users.

---

## 2. Repository Structure

```text
SCTP_DS3F_Module2_Assignment_Team7/
│
├── listenbrainz_gcp/
│   └── README.md
│
├── listenbrainz_dagster/
│   └── README.md
│
├── listenbrainz_streamlit_app/
│   └── README.md
│
└── README.md
```

---

## 3. Project Architecture

```text
ListenBrainz Raw Data
        ↓
Google BigQuery Raw Table
        ↓
dbt Staging Model
        ↓
dbt Star Schema
        ↓
dbt Tests / Data Quality Checks
        ↓
Dagster Orchestration
        ↓
Streamlit Dashboard
        ↓
Google Cloud Run Deployment
```

---

## 4. Component Summary

| Component | Folder | Main Purpose |
|---|---|---|
| dbt | `listenbrainz_gcp/` | Cleans and transforms raw ListenBrainz data into a star schema |
| Dagster | `listenbrainz_dagster/` | Orchestrates dbt models, tests, data quality checks, and analysis outputs |
| Streamlit | `listenbrainz_streamlit_app/` | Provides a dashboard for exploring listening trends and business insights |

---

## 5. dbt Project Summary

The dbt project is located in:

```text
listenbrainz_gcp/
```

It transforms the raw ListenBrainz table in BigQuery into a dimensional star schema.

### Main dbt Models

| Model | Description |
|---|---|
| `stg_listenbrainz_listen` | Cleans raw listening records |
| `dim_user` | Stores unique users |
| `dim_track` | Stores unique tracks |
| `dim_artist` | Stores unique artists |
| `dim_album` | Stores known releases |
| `dim_date` | Stores date attributes |
| `dim_time` | Stores time attributes |
| `fact_listening_events` | Stores one row per listening event |

### Star Schema

```text
                    dim_date
                       |
dim_user ---- fact_listening_events ---- dim_track
                       |
                   dim_artist
                       |
                   dim_album
                       |
                   dim_time
```

### Common dbt Commands

```bash
cd listenbrainz_gcp

dbt deps
dbt parse
dbt run
dbt test
```

To rebuild the full project:

```bash
dbt clean
dbt deps
dbt run --full-refresh
dbt test
```

---

## 6. Dagster Project Summary

The Dagster project is located in:

```text
listenbrainz_dagster/
```

Dagster is used to orchestrate the analytics pipeline and provide visibility into data assets.

### Dagster Responsibilities

- Run dbt models
- Run dbt tests
- Trigger data quality checks
- Generate business analysis outputs
- Prepare optional machine learning datasets
- Provide pipeline lineage and monitoring

### Run Dagster Locally

Before running Dagster, generate the dbt manifest:

```bash
cd listenbrainz_gcp
dbt deps
dbt parse
```

Then run Dagster:

```bash
cd ../listenbrainz_dagster
dagster dev
```

---

## 7. Streamlit Project Summary

The Streamlit app is located in:

```text
listenbrainz_streamlit_app/
```

The dashboard reads from the BigQuery star schema and presents interactive music listening insights.

### Dashboard Pages

| Page | Description |
|---|---|
| Executive Overview | Summary KPIs and key insights |
| Artists & Songs | Top artists and most popular songs |
| Listening Patterns | Time-of-day, hourly, and monthly trends |
| Users & Releases | Active users, known releases, and release metadata completeness |
| Data Pipeline | Architecture and star schema explanation |
| Streaming Enhancement | Optional Kafka and Spark Streaming design |

### Run Locally

```bash
cd listenbrainz_streamlit_app
streamlit run app.py
```

### Deploy to Cloud Run

```bash
gcloud run deploy listenbrainz-streamlit-app \
  --source . \
  --region us-central1 \
  --allow-unauthenticated
```

---

## 8. Data Quality Approach

The project includes data quality checks at both dbt and notebook levels.

### Main Checks

| Check | Expected Result | Purpose |
|---|---|---|
| `listen_id` not null | 0 invalid rows | Ensures every event has a generated ID |
| `listen_id` unique | 0 duplicates | Prevents double-counting listening events |
| Dimension keys unique | 0 duplicates | Prevents many-to-many joins |
| Fact foreign keys valid | All fact rows map to dimensions | Ensures star schema integrity |
| `artist_name` and `track_name` not blank | 0 invalid rows | Prevents poor-quality dashboard labels |
| `listened_at` not in future | 0 invalid rows | Prevents invalid future listening events |
| `listen_count = 1` | 0 invalid rows | Ensures each fact row represents one listening event |

### Release Name Metadata

`release_name` is treated as optional metadata because not all ListenBrainz records contain album or release information.

Missing `release_name` values are monitored as metadata completeness, but they do not fail the pipeline because the listening event can still support user, artist, track, date, and time analysis.

---

## 9. Optional Streaming Enhancement

The project includes an optional future enhancement using Kafka and Spark Structured Streaming.

```text
ListenBrainz Listening Events
        ↓
Kafka Topic
        ↓
Spark Structured Streaming
        ↓
BigQuery Streaming Tables
        ↓
Streamlit Real-Time Dashboard
```

This enhancement would support near real-time monitoring of:

- top songs
- top artists
- listening events per minute
- peak listening periods
- trending known releases
- invalid or failed events

---

## 10. Key Business Questions Answered

| Business Question | Supported By |
|---|---|
| Which artists are most listened to? | `fact_listening_events`, `dim_artist` |
| Which songs are most popular? | `fact_listening_events`, `dim_track` |
| What time of day do users listen most? | `fact_listening_events`, `dim_time` |
| How does listening activity change monthly? | `fact_listening_events`, `dim_date` |
| Which users listen most often? | `fact_listening_events`, `dim_user` |
| Which known releases are trending? | `fact_listening_events`, `dim_album` |

---

## 11. Important Notes

- The raw source table is named `listen`.
- The staging model is `stg_listenbrainz_listen`.
- `dim_album` uses `release_name`, not `album_name`.
- `release_name` is optional metadata.
- Cloud Run should use a service account, not a JSON key file inside the app.
- Service account JSON files should not be committed to GitHub.

---

## 12. GitHub Workflow

After making changes:

```bash
git status
git add .
git commit -m "Update ListenBrainz project files"
git push origin main
```

Then redeploy Streamlit if dashboard code changed:

```bash
cd listenbrainz_streamlit_app

gcloud run deploy listenbrainz-streamlit-app \
  --source . \
  --region us-central1 \
  --allow-unauthenticated
```

---

## 13. Final Outcome

This project demonstrates a complete analytics engineering workflow:

1. Ingest raw ListenBrainz data into BigQuery.
2. Transform the data using dbt.
3. Model the data as a star schema.
4. Validate the data using tests and quality checks.
5. Orchestrate the pipeline with Dagster.
6. Visualise insights with Streamlit.
7. Deploy the dashboard using Google Cloud Run.
8. Propose Kafka and Spark Streaming as a future real-time enhancement.
