from pathlib import Path

from dagster_dbt import DbtProject

listenbrainz_gcp_project = DbtProject(
    project_dir=Path(__file__).joinpath("..", "..", "..", "listenbrainz_gcp").resolve(),
    packaged_project_dir=Path(__file__).joinpath("..", "..", "dbt-project").resolve(),
)
listenbrainz_gcp_project.prepare_if_dev()