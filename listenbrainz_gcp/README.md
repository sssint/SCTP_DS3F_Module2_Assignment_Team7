# ListenBrainz dbt Project

## Overview

This dbt project transforms raw ListenBrainz listening data in BigQuery into a clean analytical star schema. It prepares data for business analysis, Streamlit dashboard reporting, data quality checks, and optional machine learning enhancement.

## Technology Stack

| Tool | Purpose |
|---|---|
| Google BigQuery | Cloud data warehouse |
| dbt | SQL transformation, testing, and documentation |
| GitHub | Version control |
| Google Cloud service account | BigQuery authentication |

## Project Structure

```text
listenbrainz_gcp/
├── dbt_project.yml
├── packages.yml
├── profiles.yml
├── models/
│   ├── staging/
│   │   ├── sources.yml
│   │   ├── schema.yml
│   │   └── stg_listenbrainz_listen.sql
│   └── marts/
│       ├── dim_user.sql
│       ├── dim_track.sql
│       ├── dim_artist.sql
│       ├── dim_album.sql
│       ├── dim_date.sql
│       ├── dim_time.sql
│       ├── fact_listening_events.sql
│       └── schema.yml
└── target/
```

## Source Data

The raw source table is stored in BigQuery:

```text
my-project-sssint1.listenbrainz_gcp.listen
```

The source is defined in:

```text
models/staging/sources.yml
```

Example:

```yaml
version: 2

sources:
  - name: listenbrainz_raw
    database: my-project-sssint1
    schema: listenbrainz_gcp

    tables:
      - name: listen
        description: Raw ListenBrainz listening events
```

The staging model references the source using:

```sql
FROM {{ source('listenbrainz_raw', 'listen') }}
```

## Data Models

### Staging

| Model | Description |
|---|---|
| `stg_listenbrainz_listen` | Cleans raw ListenBrainz records, trims text fields, removes invalid blanks, and generates `listen_id` |

### Marts

| Model | Description |
|---|---|
| `dim_user` | Unique users |
| `dim_track` | Unique tracks |
| `dim_artist` | Unique artists |
| `dim_album` | Known releases or albums |
| `dim_date` | Calendar attributes |
| `dim_time` | Time and time-period attributes |
| `fact_listening_events` | Central fact table with one row per listening event |

## Star Schema

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

The star schema was chosen because it supports efficient analytical queries. The fact table stores measurable listening events, while dimension tables store descriptive attributes for users, tracks, artists, releases, dates, and times.

## Data Quality Tests

Key dbt tests include:

| Check | Purpose |
|---|---|
| `listen_id` not null | Ensures every event has a generated ID |
| `listen_id` unique | Prevents duplicate listening events |
| Dimension primary keys unique | Prevents many-to-many joins |
| Foreign key relationships | Ensures fact rows map to valid dimensions |
| `track_name` and `artist_name` not null | Required for reporting |
| `listen_count = 1` | Ensures one fact row represents one event |

`release_name` is optional metadata because some ListenBrainz records do not contain album or release information.

## Running dbt

Install dependencies:

```bash
dbt deps
```

Parse project:

```bash
dbt parse
```

Run models:

```bash
dbt run
```

Run tests:

```bash
dbt test
```

Rebuild staging and downstream star schema:

```bash
dbt run --select stg_listenbrainz_listen+
dbt test --select stg_listenbrainz_listen+
```

Full rebuild:

```bash
dbt clean
dbt deps
dbt run --full-refresh
dbt test
```

## Important Notes

- The source table name is `listen`, not `listens`.
- `dim_album` uses `release_name`, not `album_name`.
- Missing `release_name` values are monitored as metadata completeness, not treated as a pipeline failure.
- The fact table should preserve one row per listening event.

## Downstream Usage

This dbt star schema supports:

- Streamlit dashboard
- Python business analysis notebook
- data quality notebook
- optional machine learning prediction dataset
- optional Dagster orchestration
