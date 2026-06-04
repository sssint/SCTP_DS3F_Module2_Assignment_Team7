In Google Cloud Console:

gcloud config set project my-project-sssint1

gcloud services enable run.googleapis.com
gcloud services enable cloudbuild.googleapis.com
gcloud services enable artifactregistry.googleapis.com
gcloud services enable bigquery.googleapis.com

git clone https://github.com/sssint/SCTP_DS3F_Module2_Assignment_Team7.git
export GOOGLE_CLOUD_PROJECT='my-project-sssint1'  # Change this
export GOOGLE_CLOUD_REGION='us-central1'
export SERVICE_NAME='listenbrainz-streamlit-app'

cd SCTP_DS3F_Module2_Assignment_Team7/
cd listenbrainz_streamlit_app/

gcloud run deploy "$SERVICE_NAME" \
  --port=8080 \
  --source=. \
  --allow-unauthenticated \
  --region=$GOOGLE_CLOUD_REGION \
  --project=$GOOGLE_CLOUD_PROJECT \
  --set-env-vars=GOOGLE_CLOUD_PROJECT=$GOOGLE_CLOUD_PROJECT,GOOGLE_CLOUD_REGION=$GOOGLE_CLOUD_REGION