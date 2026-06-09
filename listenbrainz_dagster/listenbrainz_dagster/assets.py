"""
Dagster assets for the ListenBrainz analytics pipeline.

Assets included:
1. dbt assets loaded from dbt manifest
2. Star schema data quality checks
3. Business analysis CSV outputs
4. Prediction dataset CSV output

Before running Dagster:
    cd listenbrainz_gcp
    dbt deps
    dbt parse

Then:
    cd ../listenbrainz_dagster
    dagster dev
"""

from pathlib import Path

import pandas as pd
from dagster import AssetExecutionContext, MaterializeResult, MetadataValue, asset
from dagster_dbt import DbtCliResource, dbt_assets
from google.cloud import bigquery

from .constants import (
    BIGQUERY_DATASET,
    DBT_MANIFEST_PATH,
    DBT_PROJECT_DIR,
    GCP_PROJECT_ID,
    OUTPUT_DIR,
)


# =========================================================
# dbt assets
# =========================================================

@dbt_assets(manifest=DBT_MANIFEST_PATH)
def listenbrainz_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    """Run dbt models as Dagster assets."""
    yield from dbt.cli(["build"], context=context).stream()


# =========================================================
# Helper functions
# =========================================================

def get_bigquery_client() -> bigquery.Client:
    """Create BigQuery client.

    Authentication:
    - Local: use GOOGLE_APPLICATION_CREDENTIALS environment variable.
    - Cloud: use attached service account.
    """
    return bigquery.Client(project=GCP_PROJECT_ID)


def run_query(sql: str) -> pd.DataFrame:
    client = get_bigquery_client()
    return client.query(sql).to_dataframe()


# =========================================================
# Data quality asset
# =========================================================

@asset(
    name="listenbrainz_star_schema_data_quality",
    deps=[listenbrainz_dbt_assets],
    description="Runs SQL-based data quality checks against the ListenBrainz BigQuery star schema.",
)
def listenbrainz_star_schema_data_quality(context: AssetExecutionContext) -> MaterializeResult:
    """Run star schema data quality checks.

    release_name is optional metadata, so missing release_name is reported as INFO,
    not as a hard failure.
    """

    dq_checks = {
        "dim_user.user_id not null": f"""
            SELECT COUNT(*) AS invalid_rows
            FROM `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.dim_user`
            WHERE user_id IS NULL
        """,
        "dim_user.user_id unique": f"""
            SELECT COUNT(*) AS invalid_rows
            FROM (
                SELECT user_id
                FROM `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.dim_user`
                GROUP BY user_id
                HAVING COUNT(*) > 1
            )
        """,
        "dim_track.track_id not null": f"""
            SELECT COUNT(*) AS invalid_rows
            FROM `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.dim_track`
            WHERE track_id IS NULL
        """,
        "dim_track.track_id unique": f"""
            SELECT COUNT(*) AS invalid_rows
            FROM (
                SELECT track_id
                FROM `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.dim_track`
                GROUP BY track_id
                HAVING COUNT(*) > 1
            )
        """,
        "dim_artist.artist_id not null": f"""
            SELECT COUNT(*) AS invalid_rows
            FROM `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.dim_artist`
            WHERE artist_id IS NULL
        """,
        "dim_artist.artist_id unique": f"""
            SELECT COUNT(*) AS invalid_rows
            FROM (
                SELECT artist_id
                FROM `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.dim_artist`
                GROUP BY artist_id
                HAVING COUNT(*) > 1
            )
        """,
        "dim_album.album_id not null": f"""
            SELECT COUNT(*) AS invalid_rows
            FROM `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.dim_album`
            WHERE album_id IS NULL
        """,
        "dim_album.album_id unique": f"""
            SELECT COUNT(*) AS invalid_rows
            FROM (
                SELECT album_id
                FROM `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.dim_album`
                GROUP BY album_id
                HAVING COUNT(*) > 1
            )
        """,
        "dim_date.date_id unique": f"""
            SELECT COUNT(*) AS invalid_rows
            FROM (
                SELECT date_id
                FROM `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.dim_date`
                GROUP BY date_id
                HAVING COUNT(*) > 1
            )
        """,
        "dim_time.time_id unique": f"""
            SELECT COUNT(*) AS invalid_rows
            FROM (
                SELECT time_id
                FROM `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.dim_time`
                GROUP BY time_id
                HAVING COUNT(*) > 1
            )
        """,
        "fact.listen_id not null": f"""
            SELECT COUNT(*) AS invalid_rows
            FROM `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.fact_listening_events`
            WHERE listen_id IS NULL
        """,
        "fact.listen_id unique": f"""
            SELECT COUNT(*) AS invalid_rows
            FROM (
                SELECT listen_id
                FROM `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.fact_listening_events`
                GROUP BY listen_id
                HAVING COUNT(*) > 1
            )
        """,
        "fact foreign keys not null": f"""
            SELECT COUNT(*) AS invalid_rows
            FROM `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.fact_listening_events`
            WHERE user_id IS NULL
               OR track_id IS NULL
               OR artist_id IS NULL
               OR date_id IS NULL
               OR time_id IS NULL
        """,
        "fact.user_id maps to dim_user": f"""
            SELECT COUNT(*) AS invalid_rows
            FROM `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.fact_listening_events` f
            LEFT JOIN `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.dim_user` d
                ON f.user_id = d.user_id
            WHERE f.user_id IS NOT NULL
              AND d.user_id IS NULL
        """,
        "fact.track_id maps to dim_track": f"""
            SELECT COUNT(*) AS invalid_rows
            FROM `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.fact_listening_events` f
            LEFT JOIN `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.dim_track` d
                ON f.track_id = d.track_id
            WHERE f.track_id IS NOT NULL
              AND d.track_id IS NULL
        """,
        "fact.artist_id maps to dim_artist": f"""
            SELECT COUNT(*) AS invalid_rows
            FROM `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.fact_listening_events` f
            LEFT JOIN `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.dim_artist` d
                ON f.artist_id = d.artist_id
            WHERE f.artist_id IS NOT NULL
              AND d.artist_id IS NULL
        """,
        "fact.album_id maps to dim_album when populated": f"""
            SELECT COUNT(*) AS invalid_rows
            FROM `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.fact_listening_events` f
            LEFT JOIN `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.dim_album` d
                ON f.album_id = d.album_id
            WHERE f.album_id IS NOT NULL
              AND d.album_id IS NULL
        """,
        "fact.date_id maps to dim_date": f"""
            SELECT COUNT(*) AS invalid_rows
            FROM `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.fact_listening_events` f
            LEFT JOIN `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.dim_date` d
                ON f.date_id = d.date_id
            WHERE f.date_id IS NOT NULL
              AND d.date_id IS NULL
        """,
        "fact.time_id maps to dim_time": f"""
            SELECT COUNT(*) AS invalid_rows
            FROM `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.fact_listening_events` f
            LEFT JOIN `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.dim_time` d
                ON f.time_id = d.time_id
            WHERE f.time_id IS NOT NULL
              AND d.time_id IS NULL
        """,
        "listen_count equals 1": f"""
            SELECT COUNT(*) AS invalid_rows
            FROM `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.fact_listening_events`
            WHERE listen_count != 1
               OR listen_count IS NULL
        """,
        "listened_at not in future": f"""
            SELECT COUNT(*) AS invalid_rows
            FROM `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.fact_listening_events`
            WHERE listened_at > CURRENT_TIMESTAMP()
        """,
        "dim_track.track_name not blank": f"""
            SELECT COUNT(*) AS invalid_rows
            FROM `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.dim_track`
            WHERE track_name IS NULL
               OR TRIM(track_name) = ''
        """,
        "dim_artist.artist_name not blank": f"""
            SELECT COUNT(*) AS invalid_rows
            FROM `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.dim_artist`
            WHERE artist_name IS NULL
               OR TRIM(artist_name) = ''
        """,
    }

    results = []

    for check_name, sql in dq_checks.items():
        df = run_query(sql)
        invalid_rows = int(df.loc[0, "invalid_rows"])
        status = "PASS" if invalid_rows == 0 else "FAIL"

        results.append({
            "check_name": check_name,
            "invalid_rows": invalid_rows,
            "status": status,
            "severity": "ERROR",
        })

        if status == "FAIL":
            context.log.warning(f"{check_name} failed with {invalid_rows} invalid rows")
        else:
            context.log.info(f"{check_name} passed")

    # Informational metadata completeness check
    metadata_sql = f"""
        SELECT
            COUNT(*) AS total_album_rows,
            COUNTIF(release_name IS NULL OR TRIM(release_name) = '') AS missing_release_rows,
            COUNTIF(release_name IS NOT NULL AND TRIM(release_name) != '') AS known_release_rows
        FROM `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.dim_album`
    """

    metadata_df = run_query(metadata_sql)
    metadata_row = metadata_df.iloc[0]

    total_album_rows = int(metadata_row["total_album_rows"])
    missing_release_rows = int(metadata_row["missing_release_rows"])
    known_release_rows = int(metadata_row["known_release_rows"])
    missing_pct = (missing_release_rows / total_album_rows * 100) if total_album_rows else 0

    results.append({
        "check_name": "dim_album.release_name metadata completeness",
        "invalid_rows": missing_release_rows,
        "status": "INFO",
        "severity": "INFO",
    })

    results_df = pd.DataFrame(results)
    output_path = OUTPUT_DIR / "dagster_star_schema_data_quality_results.csv"
    results_df.to_csv(output_path, index=False)

    failed_count = int((results_df["status"] == "FAIL").sum())
    passed_count = int((results_df["status"] == "PASS").sum())

    if failed_count > 0:
        context.log.warning(f"Data quality completed with {failed_count} failed checks")
    else:
        context.log.info("All hard data quality checks passed")

    return MaterializeResult(
        metadata={
            "passed_checks": passed_count,
            "failed_checks": failed_count,
            "missing_release_rows_info": missing_release_rows,
            "known_release_rows": known_release_rows,
            "missing_release_percentage": round(missing_pct, 2),
            "output_file": MetadataValue.path(str(output_path)),
            "preview": MetadataValue.md(results_df.head(20).to_markdown(index=False)),
        }
    )


# =========================================================
# Business analysis asset
# =========================================================

@asset(
    name="listenbrainz_business_analysis_outputs",
    deps=[listenbrainz_star_schema_data_quality],
    description="Creates business analysis CSV outputs from the ListenBrainz star schema.",
)
def listenbrainz_business_analysis_outputs(context: AssetExecutionContext) -> MaterializeResult:
    """Generate business analysis CSV outputs for dashboard/reporting."""

    analysis_queries = {
        "top_artists": f"""
            SELECT
                ar.artist_name,
                COUNT(*) AS total_listens
            FROM `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.fact_listening_events` f
            LEFT JOIN `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.dim_artist` ar
                ON f.artist_id = ar.artist_id
            WHERE ar.artist_name IS NOT NULL
            GROUP BY ar.artist_name
            ORDER BY total_listens DESC
            LIMIT 20
        """,
        "top_songs": f"""
            SELECT
                t.track_name,
                t.artist_name,
                COUNT(*) AS total_listens
            FROM `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.fact_listening_events` f
            LEFT JOIN `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.dim_track` t
                ON f.track_id = t.track_id
            WHERE t.track_name IS NOT NULL
            GROUP BY
                t.track_name,
                t.artist_name
            ORDER BY total_listens DESC
            LIMIT 20
        """,
        "listens_by_hour": f"""
            SELECT
                tm.hour,
                COUNT(*) AS total_listens
            FROM `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.fact_listening_events` f
            LEFT JOIN `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.dim_time` tm
                ON f.time_id = tm.time_id
            WHERE tm.hour IS NOT NULL
            GROUP BY tm.hour
            ORDER BY tm.hour
        """,
        "monthly_trend": f"""
            SELECT
                d.year,
                d.month,
                COUNT(*) AS total_listens
            FROM `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.fact_listening_events` f
            LEFT JOIN `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.dim_date` d
                ON f.date_id = d.date_id
            WHERE d.year IS NOT NULL
              AND d.month IS NOT NULL
            GROUP BY d.year, d.month
            ORDER BY d.year, d.month
        """,
        "top_users": f"""
            SELECT
                u.user_name,
                COUNT(*) AS total_listens
            FROM `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.fact_listening_events` f
            LEFT JOIN `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.dim_user` u
                ON f.user_id = u.user_id
            WHERE u.user_name IS NOT NULL
            GROUP BY u.user_name
            ORDER BY total_listens DESC
            LIMIT 20
        """,
        "top_known_releases": f"""
            SELECT
                al.release_name,
                al.artist_name,
                COUNT(*) AS total_listens
            FROM `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.fact_listening_events` f
            LEFT JOIN `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.dim_album` al
                ON f.album_id = al.album_id
            WHERE al.release_name IS NOT NULL
              AND TRIM(al.release_name) != ''
            GROUP BY
                al.release_name,
                al.artist_name
            ORDER BY total_listens DESC
            LIMIT 20
        """,
    }

    output_files = []

    for name, sql in analysis_queries.items():
        df = run_query(sql)
        output_path = OUTPUT_DIR / f"{name}.csv"
        df.to_csv(output_path, index=False)
        output_files.append(str(output_path))
        context.log.info(f"Saved {name}: {output_path}")

    return MaterializeResult(
        metadata={
            "number_of_outputs": len(output_files),
            "output_folder": MetadataValue.path(str(OUTPUT_DIR)),
            "files": MetadataValue.md("\n".join([f"- `{file}`" for file in output_files])),
        }
    )


# =========================================================
# Prediction dataset asset
# =========================================================

@asset(
    name="listenbrainz_prediction_dataset",
    deps=[listenbrainz_star_schema_data_quality],
    description="Creates a user-track prediction dataset from the star schema.",
)
def listenbrainz_prediction_dataset(context: AssetExecutionContext) -> MaterializeResult:
    """Prepare a prediction dataset for optional ML enhancement."""

    prediction_sql = f"""
        SELECT
            u.user_name,
            t.track_id,
            t.track_name,
            t.artist_name AS track_artist_name,
            ar.artist_id,
            ar.artist_name,
            al.album_id,
            al.release_name,
            d.day_of_week,
            d.month,
            d.year,
            tm.hour,
            tm.time_period,
            COUNT(*) AS listen_count
        FROM `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.fact_listening_events` f
        LEFT JOIN `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.dim_user` u
            ON f.user_id = u.user_id
        LEFT JOIN `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.dim_track` t
            ON f.track_id = t.track_id
        LEFT JOIN `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.dim_artist` ar
            ON f.artist_id = ar.artist_id
        LEFT JOIN `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.dim_album` al
            ON f.album_id = al.album_id
        LEFT JOIN `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.dim_date` d
            ON f.date_id = d.date_id
        LEFT JOIN `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.dim_time` tm
            ON f.time_id = tm.time_id
        WHERE u.user_name IS NOT NULL
          AND t.track_id IS NOT NULL
        GROUP BY
            u.user_name,
            t.track_id,
            t.track_name,
            track_artist_name,
            ar.artist_id,
            ar.artist_name,
            al.album_id,
            al.release_name,
            d.day_of_week,
            d.month,
            d.year,
            tm.hour,
            tm.time_period
        ORDER BY listen_count DESC
        LIMIT 100000
    """

    df = run_query(prediction_sql)
    output_path = OUTPUT_DIR / "listenbrainz_prediction_dataset.csv"
    df.to_csv(output_path, index=False)

    return MaterializeResult(
        metadata={
            "rows": len(df),
            "columns": len(df.columns),
            "output_file": MetadataValue.path(str(output_path)),
            "preview": MetadataValue.md(df.head(10).to_markdown(index=False)),
        }
    )
