# ListenBrainz Streamlit App

## Overview

This Streamlit app provides an interactive dashboard for the ListenBrainz analytics project. It reads from the BigQuery star schema created by dbt and displays executive metrics, listening trends, top artists, top songs, top users, and known releases.

The app is designed for deployment to Google Cloud Run.

## Technology Stack

| Tool | Purpose |
|---|---|
| Streamlit | Interactive web dashboard |
| BigQuery | Data source |
| pandas | Data processing |
| Google Cloud Run | Serverless deployment |
| Docker | Containerized deployment |
| GitHub | Version control |

## Project Structure

```text
listenbrainz_streamlit_app/
├── app.py
├── requirements.txt
├── Dockerfile
├── .dockerignore
└── pages/
    └── 2_Streaming_Enhancement.py
```

## Main Dashboard Pages

| Page | Description |
|---|---|
| Executive Overview | KPI cards and key insights |
| Artists & Songs | Top artists and top songs |
| Listening Patterns | Time-of-day, hourly, and monthly trends |
| Users & Releases | Top users, known releases, and release metadata completeness |
| Data Pipeline | Architecture and schema explanation |

## Streaming Enhancement Page

```text
pages/2_Streaming_Enhancement.py
```

This page explains the optional Kafka and Spark Streaming enhancement.

It covers:

- why streaming is useful for music listening events
- proposed Kafka + Spark architecture
- component responsibilities
- real-time metrics
- sample Kafka message
- sample Spark Structured Streaming code
- batch and streaming hybrid design
- benefits and limitations

## BigQuery Tables Used

```text
my-project-sssint1.listenbrainz_gcp.fact_listening_events
my-project-sssint1.listenbrainz_gcp.dim_user
my-project-sssint1.listenbrainz_gcp.dim_track
my-project-sssint1.listenbrainz_gcp.dim_artist
my-project-sssint1.listenbrainz_gcp.dim_album
my-project-sssint1.listenbrainz_gcp.dim_date
my-project-sssint1.listenbrainz_gcp.dim_time
```

## Important Schema Notes

`dim_album` contains:

```text
album_id
release_name
artist_name
```

Use:

```sql
al.release_name
```

Do not use:

```sql
al.album_name
```

`release_name` is optional metadata. The dashboard excludes blank `release_name` values from the Top Releases chart but keeps the original listening records for artist, track, user, and time analysis.

## Local Setup

Install dependencies:

```bash
pip install -r requirements.txt
```

Run the app locally:

```bash
streamlit run app.py
```

For local BigQuery access using a service account:

```bash
export GOOGLE_APPLICATION_CREDENTIALS="/home/sansa/my-project-sssint1-7e02e9078e06.json"
streamlit run app.py
```

On Windows PowerShell:

```powershell
$env:GOOGLE_APPLICATION_CREDENTIALS="C:\Users\YourName\.dbt\my-project-sssint1-key.json"
streamlit run app.py
```

Do not set `GOOGLE_APPLICATION_CREDENTIALS` inside `app.py` for Cloud Run.

## requirements.txt

```text
streamlit
google-cloud-bigquery
pandas
db-dtypes
pyarrow
```

## Dockerfile

```dockerfile
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PORT=8080

CMD streamlit run app.py \
    --server.port=$PORT \
    --server.address=0.0.0.0 \
    --server.headless=true \
    --server.enableCORS=false \
    --server.enableXsrfProtection=false \
    --browser.gatherUsageStats=false
```

## Deploy to Google Cloud Run

Set project:

```bash
gcloud config set project my-project-sssint1
```

Deploy from the folder containing `app.py`:

```bash
gcloud run deploy listenbrainz-streamlit-app \
  --source . \
  --region us-central1 \
  --allow-unauthenticated
```

Force a new revision:

```bash
gcloud run deploy listenbrainz-streamlit-app \
  --source . \
  --region us-central1 \
  --allow-unauthenticated \
  --set-env-vars APP_VERSION=streamlit-update-1
```

## Cloud Run Service Account

Cloud Run should use a service account with BigQuery permissions:

```text
roles/bigquery.jobUser
roles/bigquery.dataViewer
```

Do not upload service account JSON files to GitHub.

## Common Errors

### `Name album_name not found inside al`

Cause:

```sql
al.album_name
```

Fix:

```sql
al.release_name
```

### Blank dashboard screen

Check logs:

```bash
gcloud run services logs read listenbrainz-streamlit-app \
  --region us-central1 \
  --limit 50
```

Common causes:

- missing package in `requirements.txt`
- BigQuery permission issue
- wrong table or column name
- Cloud Run running old GitHub code
- Dockerfile not listening on `$PORT`

### Cloud Shell has old code

Update from GitHub:

```bash
git pull origin main
```

Then redeploy.

## GitHub Sync

After editing files:

```bash
git add .
git commit -m "Update Streamlit dashboard"
git push origin main
```

Then redeploy from Cloud Shell:

```bash
gcloud run deploy listenbrainz-streamlit-app \
  --source . \
  --region us-central1 \
  --allow-unauthenticated
```

## Dashboard Value

The Streamlit dashboard turns the BigQuery star schema into interactive insights, making it easier to understand popular artists, songs, active users, listening trends, and known releases.
