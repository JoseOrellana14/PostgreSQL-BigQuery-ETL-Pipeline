import petl as etl
import pandas as pd
import json
import logging
from app.common.utils import preview_table, convert_column_types, load_schema
from app.listing_projects.models import LISTING_PROJECT_SCHEMA_PATH
from logging_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)


def transform_listing_projects(listing_projects_table, load_date):
    """Transform listing projects data and convert to a clean DataFrame"""
    try:
        # Load the schema
        schema = load_schema(LISTING_PROJECT_SCHEMA_PATH)
        # Preview the input table
        preview_table(listing_projects_table, message="Input listing projects table:")

        # Conver column types based on the schema
        listing_projects_table = convert_column_types(listing_projects_table, schema)

        # Convert PETL table to pandas DataFrame
        listing_projects_df = etl.todataframe(listing_projects_table)

        # Mandatory Keys
        mandatory_keys = ['listing_project_id', 'user_id']
        for key in mandatory_keys:
            if key in listing_projects_df.columns:
                listing_projects_df[key] = pd.to_numeric(listing_projects_df[key], errors='coerce')
        listing_projects_df = listing_projects_df.dropna(subset=mandatory_keys)
        for key in mandatory_keys:
            if key in listing_projects_df.columns:
                listing_projects_df[key] = listing_projects_df[key].astype('int64')

        # Getting the table's load date
        listing_projects_df['load_date'] = load_date

        # Explicityly convert date fields to pandas datetime
        date_fields = ['delivery_date', 'created_at', 'updated_at', 'record_deleted_at', 'load_date']
        for field in date_fields:
            if field in listing_projects_df.columns:
                listing_projects_df[field] = pd.to_datetime(listing_projects_df[field], errors='coerce')

        # Final column filtering and type enforcement according to the schema
        final_petl_table = etl.fromdataframe(listing_projects_df)
        schema_columns = [field['name'] for field in schema]
        final_petl_table = etl.cut(final_petl_table, *schema_columns)
        final_petl_table = convert_column_types(final_petl_table, schema)

        # Preview final result as PETL table
        preview_table(final_petl_table, message="Final transformed table:")

        result = etl.todataframe(final_petl_table)

        return result
    
    except Exception:
        logger.exception("Error transforming listing projects data")
        raise
