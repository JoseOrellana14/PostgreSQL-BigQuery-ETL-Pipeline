import os
import logging
from google.cloud import bigquery
from app.common.config import get_bq_credentials, validate_env_variables
from app.organizations.models import ORGANIZATION_TABLE_NAME, ORGANIZATION_SCHEMA_PATH
from logging_config import setup_logging
from app.common.utils import merge_dataframe_to_bq, convert_json_schema_to_bq_schema, load_schema

setup_logging()
logger = logging.getLogger(__name__)


"""DATA SOURCE DETECTION incremental load logic."""
def load_organizations(organizations_df):
    """Load organizations data into BigQuery."""
    try:
        # Validate environment variables
        validate_env_variables('BQ_PROJECT', 'BQ_DATASET', 'BQ_ORGANIZATIONS_TABLE')

        # Define the target table in BigQuery
        table_id = f"{os.getenv('BQ_PROJECT')}.{os.getenv('BQ_DATASET')}.{os.getenv('BQ_CUSTOMERS_TABLE')}"

        schema = load_schema(ORGANIZATION_SCHEMA_PATH)
        bq_schema = convert_json_schema_to_bq_schema(schema)

        # Call merge upsert function
        merge_dataframe_to_bq(
            df=organizations_df,
            table_id=table_id,
            key_column="organization_id",
            updated_at_col="updated_at",
            bq_schema=bq_schema,
            credentials=get_bq_credentials()
        )
    except Exception:
        logger.exception("Error loading organizations data into BigQuery.")
        raise