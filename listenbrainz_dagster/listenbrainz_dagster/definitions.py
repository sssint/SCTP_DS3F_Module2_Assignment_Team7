"""
Dagster definitions for ListenBrainz analytics pipeline.
"""

from dagster import (
    AssetSelection,
    Definitions,
    ScheduleDefinition,
    define_asset_job,
)
from dagster_dbt import DbtCliResource

from .assets import (
    listenbrainz_business_analysis_outputs,
    listenbrainz_dbt_assets,
    listenbrainz_prediction_dataset,
    listenbrainz_star_schema_data_quality,
)
from .constants import DBT_PROJECT_DIR


# Job that materializes all assets
listenbrainz_all_assets_job = define_asset_job(
    name="listenbrainz_all_assets_job",
    selection=AssetSelection.all(),
)

# Job for dbt + data quality only
listenbrainz_dbt_quality_job = define_asset_job(
    name="listenbrainz_dbt_quality_job",
    selection=AssetSelection.assets(
        listenbrainz_dbt_assets,
        listenbrainz_star_schema_data_quality,
    ),
)

# Daily schedule
daily_listenbrainz_schedule = ScheduleDefinition(
    name="daily_listenbrainz_pipeline_schedule",
    job=listenbrainz_all_assets_job,
    cron_schedule="0 8 * * *",
)

defs = Definitions(
    assets=[
        listenbrainz_dbt_assets,
        listenbrainz_star_schema_data_quality,
        listenbrainz_business_analysis_outputs,
        listenbrainz_prediction_dataset,
    ],
    jobs=[
        listenbrainz_all_assets_job,
        listenbrainz_dbt_quality_job,
    ],
    schedules=[
        daily_listenbrainz_schedule,
    ],
    resources={
        "dbt": DbtCliResource(project_dir=str(DBT_PROJECT_DIR)),
    },
)
