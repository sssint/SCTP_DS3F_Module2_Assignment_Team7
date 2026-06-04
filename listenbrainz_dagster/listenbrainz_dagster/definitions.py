from dagster import Definitions
from dagster_dbt import DbtCliResource
from .assets import listenbrainz_gcp_dbt_assets
from .project import listenbrainz_gcp_project
from .schedules import schedules

defs = Definitions(
    assets=[listenbrainz_gcp_dbt_assets],
    schedules=schedules,
    resources={
        "dbt": DbtCliResource(project_dir=listenbrainz_gcp_project),
    },
)