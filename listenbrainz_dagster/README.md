# ListenBrainz Dagster Project

## Overview

This Dagster project orchestrates the ListenBrainz data pipeline. It coordinates dbt model execution, dbt tests, star schema validation, business analysis outputs, and optional machine learning dataset preparation.

Dagster provides an asset graph, run history, scheduling, monitoring, and a clear view of pipeline dependencies.

## Technology Stack

| Tool | Purpose |
|---|---|
| Dagster | Pipeline orchestration |
| dagster-dbt | Loads dbt models as Dagster assets |
| dbt | Data transformation and tests |
| BigQuery | Data warehouse |
| Python | Data quality and analysis tasks |

## Project Structure

```text
listenbrainz_dagster/
├── pyproject.toml
├── README.md
└── listenbrainz_dagster/
    ├── __init__.py
    ├── definitions.py
    └── assets.py
```

## Scaffold Command

The Dagster project can be scaffolded around the existing dbt project:

```bash
dagster-dbt project scaffold --project-name listenbrainz_dagster --dbt-project-dir listenbrainz_gcp
```

## Pipeline Architecture

```text
BigQuery Raw Table
        ↓
dbt Staging Model
        ↓
dbt Star Schema Models
        ↓
dbt Tests
        ↓
Dagster Data Quality Asset
        ↓
Business Analysis Asset
        ↓
Prediction Dataset Asset
        ↓
CSV / Dashboard / ML Outputs
```

## Dagster Assets

| Asset | Description |
|---|---|
| `listenbrainz_dbt_assets` | Runs dbt models and tests |
| `listenbrainz_data_quality_results` | Runs SQL-based data quality checks |
| `listenbrainz_business_analysis` | Produces business-analysis CSV outputs |
| `listenbrainz_prediction_dataset` | Creates a machine learning dataset from the star schema |

## Installation

```bash
pip install dagster dagster-webserver dagster-dbt dagster-gcp google-cloud-bigquery pandas db-dtypes scikit-learn
```

## dbt Manifest Requirement

Before running Dagster, generate the dbt manifest:

```bash
cd listenbrainz_gcp
dbt deps
dbt parse
```

This creates:

```text
target/manifest.json
```

Dagster uses this file to load dbt models as assets.

## Running Dagster Locally

From the Dagster project folder:

```bash
cd listenbrainz_dagster
dagster dev
```

Then open the local Dagster UI URL shown in the terminal.

## Example Dagster Flow

```text
dbt build
   ↓
dbt test
   ↓
data quality checks
   ↓
business analysis outputs
   ↓
prediction dataset creation
```

## Scheduling

A daily schedule can be defined in `definitions.py`:

```python
ScheduleDefinition(
    job=listenbrainz_job,
    cron_schedule="0 8 * * *"
)
```

## Data Quality Checks

Dagster can run SQL checks against BigQuery, including:

- primary key uniqueness
- fact table `listen_id` uniqueness
- foreign key relationship checks
- blank track and artist names
- future timestamp validation
- release metadata completeness monitoring

`release_name` is optional metadata and should be reported as an informational completeness issue, not a hard failure.

## Optional Streaming Enhancement

Kafka and Spark Structured Streaming can be added as a future enhancement.

```text
ListenBrainz Events
        ↓
Kafka Topic
        ↓
Spark Structured Streaming
        ↓
BigQuery Streaming Tables
        ↓
Streamlit Dashboard
```

This supports near real-time monitoring of top songs, top artists, listening volume, and anomaly detection.

## Notes

- Ensure the BigQuery service account has correct permissions.
- Ensure dbt models pass tests before running downstream Dagster assets.
- Use `dagster dev` for local development.
- Use schedules only after the pipeline runs successfully manually.
