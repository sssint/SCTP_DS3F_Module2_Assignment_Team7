"""
To add a daily schedule that materializes your dbt assets, uncomment the following lines.
"""
from dagster_dbt import build_schedule_from_dbt_selection

from .assets import listenbrainz_gcp_dbt_assets

schedules = [
     build_schedule_from_dbt_selection(
         [listenbrainz_gcp_dbt_assets],
         job_name="materialize_dbt_models",
         cron_schedule="0 0 * * *",
         dbt_select="fqn:*",
     ),
]

# Access the job object created by the schedule
materialize_dbt_models_job = schedules[0].job
schedules = [schedules[0]]
jobs = [materialize_dbt_models_job]