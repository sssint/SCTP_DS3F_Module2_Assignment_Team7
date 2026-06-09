"""
Project constants for the ListenBrainz Dagster pipeline.
Update paths if your folder structure is different.
"""

from pathlib import Path

# Repo structure expected:
# SCTP_DS3F_Module2_Assignment_Team7/
# ├── listenbrainz_gcp/
# └── listenbrainz_dagster/

DAGSTER_PROJECT_DIR = Path(__file__).resolve().parent.parent
REPO_ROOT = DAGSTER_PROJECT_DIR.parent

DBT_PROJECT_DIR = REPO_ROOT / "listenbrainz_gcp"
DBT_MANIFEST_PATH = DBT_PROJECT_DIR / "target" / "manifest.json"

GCP_PROJECT_ID = "my-project-sssint1"
BIGQUERY_DATASET = "listenbrainz_gcp"

OUTPUT_DIR = REPO_ROOT / "dagster_outputs"
OUTPUT_DIR.mkdir(exist_ok=True)
