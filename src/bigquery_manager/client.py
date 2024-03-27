import google.auth
from google.cloud.bigquery import Client

def set_bigquery_client(scopes: list[str]=None):
    if scopes is None:
        scopes = [
            "https://www.googleapis.com/auth/drive",
            "https://www.googleapis.com/auth/bigquery",
            "https://www.googleapis.com/auth/cloud-platform"
        ]
    credentials, project = google.auth.default(scopes=scopes)
    return Client(credentials=credentials, project=project)