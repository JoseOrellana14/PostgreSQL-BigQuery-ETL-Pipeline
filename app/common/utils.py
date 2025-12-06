import petl as etl
import json
import pandas as pd
import numpy as np
import os
import logging
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from google.cloud import bigquery
from app.common.config import get_bq_credentials, validate_env_variables
from logging_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

def filter_columns(df, schema_columns):
    """Keep only the columns specified in the schema."""
    return df[schema_columns]

def load_schema(schema_path):
    """Load a BigQuery schema from a JSON file."""
    with open(schema_path, 'r') as f:
        return json.load(f)
    
def preview_table(table, limit=5, message="Table Preview"):
    """Print a preview of a PETL table."""
    print(message)
    print(etl.look(table, limit=limit))

def convert_column_types(table, schema):
    """Convert column types based on the schema."""
    for field in schema:
        column_name = field['name']
        column_type = field['type']

        if column_name not in table.fieldnames():
            print(f"Warning: Column {column_name} not found in table.")
            continue
        if column_type == 'INTEGER':
            def to_int(v):
                if pd.isna(v):
                    return pd.NA  # Not None: we use pd.NA for pandas compatibility
                try:
                    f = float(v)
                    if f.is_integer():
                        return int(f)
                except (ValueError, TypeError):
                    pass
                return pd.NA
            table = etl.convert(table, column_name, to_int)
            
        elif column_type == 'FLOAT':
            table = etl.convert(table, column_name, lambda v: float(v) if v not in (None, '', 'NaN') else None)
        elif column_type == 'TIMESTAMP':
            # Handle different date formats and datetime objects
            def convert_timestamp(value):
                if value is None or value == '' or value == 'NaT':
                    return None
                
                # If it's already a datetime object, return it
                if isinstance(value, datetime):
                    return value
                
                # Try different date formats
                date_formats = ['%Y-%m-%d', '%Y-%m-%d %H:%M:%S', '%Y-%m-%dT%H:%M:%S', '%d/%m/%Y']
                for date_format in date_formats:
                    try:
                        return datetime.strptime(str(value), date_format)
                    except ValueError:
                        continue
                
                # If all formats fail, return None
                return None
            
            table = etl.convert(table, column_name, convert_timestamp)
        elif column_type == 'STRING':
            table = etl.convert(table, column_name, lambda v: str(v) if v is not None else None)
        elif column_type == 'JSON':
            table = etl.convert(table, column_name, lambda v: json(v) if v is not None else None)
        elif column_type == 'BOOLEAN':
            table = etl.convert(table, column_name, lambda v: bool(v) if isinstance(v, bool) else (
                str(v).strip().lower() == 'true' if v not in (None, '', 'nan', 'none') else None
            ))

    
    return table

# SOURCE CAHNGE DETECTION for incremental load logic

def get_load_date():
    """Return current Bolivian UTC timestamp as string."""
    bolivia_now = datetime.now(ZoneInfo("America/La_Paz"))
    naive_bolivia_time = bolivia_now.replace(tzinfo=None)
    return naive_bolivia_time

def get_last_loaded_timestamp(table_name, dataset):
    """Gets the latest updated_at loaded into BigQuery."""
    client = bigquery.Client(credentials=get_bq_credentials())
    query = f"""
        SELECT MAX(updated_at) AS last_update
        FROM `{client.project}.{dataset}.{table_name}`
    """
    result = client.query(query).result()
    for row in result:
        last_update = row.last_update.strftime('%Y-%m-%d %H:%M:%S') if row.last_update else None
        print(f"[DEBUG] Last updated_at loaded on {dataset}.{table_name}: {last_update}")
        return last_update
    
def merge_dataframe_to_bq(df, table_id, key_column, updated_at_col, bq_schema=None, credentials=None):
    """Performs a dynamic MERGE (upsert) of a DataFrame into a BigQuery table."""
    client = bigquery.Client(credentials=credentials or get_bq_credentials())

    # Convert NaT in datetime columns to None (important for BigQuery)
    # Enforce datetime conversion just for TIMESTAMP column type according to schema
    timestamp_fields = [field.name for field in bq_schema if field.field_type == "TIMESTAMP"]
    for col in timestamp_fields:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
            df[col] = df[col].where(df[col].notnull(), None)

    temp_table_id = f"{table_id}_temp"
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE", schema=bq_schema) #Enforce real type not inferred
    client.load_table_from_dataframe(df, temp_table_id, job_config=job_config).result()

    all_columns = df.columns.tolist()
    update_columns = [col for col in all_columns if col != key_column]
    update_set = ", ".join([f"T.{col} = S.{col}" for col in update_columns])

    merge_query = f"""
    MERGE `{table_id}` T
    USING `{temp_table_id}` S
    ON T.{key_column} = S.{key_column}
    WHEN MATCHED AND (T.{updated_at_col} IS NULL OR S.{updated_at_col} > T.{updated_at_col}) THEN
      UPDATE SET {update_set}
    WHEN NOT MATCHED THEN
      INSERT ({', '.join(all_columns)})
      VALUES ({', '.join([f"S.{col}" for col in all_columns])})
    """

    client.query(merge_query).result()
    client.delete_table(temp_table_id, not_found_ok=True)

# Enfore schema temporary table
def convert_json_schema_to_bq_schema(schema_json):
    """Converts a JSON schema as sales.json to a SchemaField[] for BigQuery."""
    type_mapping = {
        'STRING': 'STRING',
        'INTEGER': 'INT64',
        'FLOAT': 'FLOAT64',
        'TIMESTAMP': 'TIMESTAMP',
        'BOOLEAN': 'BOOL',
        'JSON': 'JSON'
    }

    return [
        bigquery.SchemaField(field['name'], type_mapping.get(field['type'], 'STRING'), mode=field.get('mode', 'NULLABLE'))
        for field in schema_json
    ]

def upload_dataframe_to_bq(df, table_id, bq_schema=None, credentials=None, write_disposition="WRITE_APPEND"):
    """
    Upload a DataFrame directly to BigQuery. Used in manychat_funnel
    This function assumes no deduplication logic is handled here.
    """
    client = bigquery.Client(credentials=credentials or get_bq_credentials())

    job_config = bigquery.LoadJobConfig(
        write_disposition=write_disposition,
        schema=bq_schema
    )

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()  # Wait for the job to complete

def load_dataframe_with_merge(
    df,
    bq_table_env_var: str,
    schema_path: str,
    key_column: str,
    updated_at_col: str,
):
    """
    Load a DataFrame into BigQuery using MERGE (upsert), parametrized by table.

    Params:
        df               : pandas.DataFrame to load
        bq_table_env_var : name of the env var that contains the name of the BQ table
                           (eg: 'BQ_ORGANIZATIONS_TABLE', 'BQ_BUYER_LEADS_TABLE', ...)
        schema_path      : path to the JSON schema of BigQuery for the table
        key_column       : name of the PK in the BQ table (eg: 'organization_id')
        updated_at_col   : timestamp column used for incremental laod (eg: 'updated_at')
    """

    try:
        # Validar env vars
        validate_env_variables("BQ_PROJECT", "BQ_DATASET", bq_table_env_var)

        project = os.getenv("BQ_PROJECT")
        dataset = os.getenv("BQ_DATASET")
        table_name = os.getenv(bq_table_env_var)

        if not table_name:
            raise Exception(f"Environment variable {bq_table_env_var} is not set.")

        table_id = f"{project}.{dataset}.{table_name}"

        # Leer schema JSON y convertirlo a SchemaField[]
        schema = load_schema(schema_path)
        bq_schema = convert_json_schema_to_bq_schema(schema)

        # Usar tu función genérica de MERGE
        merge_dataframe_to_bq(
            df=df,
            table_id=table_id,
            key_column=key_column,
            updated_at_col=updated_at_col,
            bq_schema=bq_schema,
            credentials=get_bq_credentials(),
        )

        logger.info(f"Loaded {len(df)} rows into {table_id}")

    except Exception:
        logger.exception(f"Error loading data into BigQuery for table env {bq_table_env_var}")
        raise