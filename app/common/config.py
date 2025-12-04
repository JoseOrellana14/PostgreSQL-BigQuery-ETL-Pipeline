import os
import json
from google.cloud import secretmanager
from dotenv import load_dotenv
from google.oauth2 import service_account
import json

# Load variables from .env file
load_dotenv()

def access_secret_version(secret_name):
    """Access the secret from Google Secret Manager."""
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{os.getenv('GCP_PROJECT')}/secrets/{secret_name}/versions/latest"
    response = client.access_secret_version(name=name)
    return response.payload.data.decode('UTF-8')

def validate_env_variables(*args):
    """Validate that the required environment variables are set."""
    for var in args:
        if not os.getenv(var):
            raise Exception(f"Missing environment variable: {var}")
        
def get_bq_credentials():
    """Return BigQuery credentials from Secret Manager or local key file."""
    if os.getenv("USE_SECRET_MANAGER", "false").lower() == "true":
        secret_name = os.getenv("BQ_CREDENTIALS_SECRET")
        if not secret_name:
            raise Exception("Error: BQ_CREDENTIALS_SECRET is not set.")
        credentials_json = access_secret_version(secret_name)
        return service_account.Credentials.from_service_account_info(json.loads(credentials_json))
    else:
        return service_account.Credentials.from_service_account_file("service_account_key.json")

def get_connection_string():
    """Return PostgreSQL connection string from Secret Manager or local .env"""
    if os.getenv("USE_SECRET_MANAGER", "false").lower() == "true":
        try:
            return access_secret_version("POSTGRES_CONNECTION_STRING")
        except Exception:
            raise Exception("Error: POSTGRES_CONNECTION_STRING is not properly configured")
    else:
        conn_str = os.getenv("POSTGRES_CONNECTION_STRING")
        if not conn_str:
            raise Exception("Error: POSTGRES_CONNECTION_STRING is not set in .env")
        return conn_str