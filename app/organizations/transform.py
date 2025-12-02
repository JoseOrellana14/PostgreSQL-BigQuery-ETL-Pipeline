import petl as etl
import pandas as pd
import json
import logging
from app.common.utils import preview_table, convert_column_types, load_schema
from app.organizations.models import ORGANIZATION_SCHEMA_PATH
from logging_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

def transfrom_organizations(organizations_table, load_date):
    """Transform organizations data and convert to a clean DataFrame"""
    try:
        # Load the schema
        schema = load_schema(ORGANIZATION_SCHEMA_PATH)

        # Preview the input table
        preview_table(organizations_table, message="Input organizations table:")

        # Conver column types based on the schema
        organizations_table = convert_column_types(organizations_table, schema)

        # Convert PETL table to pandas DataFrame
        organizations_df = etl.todataframe(organizations_table)

        # Convert organization_id to numeric (INTEGER)
        organizations_df['organization_id'] = pd.to_numeric(organizations_df['organization_id'].astype(str), errors='coerce')

        # Remove rows where organization_id is NaN
        organizations_df = organizations_df[organizations_df['organization_id'].notna()]

        # Fill NaN and convert to INTEGER
        organizations_df['organization_id'] = organizations_df['organization_id'].astype('int64')

        # Getting the table's load date
        organizations_df['load_date'] = load_date

        # Explicityly convert date fields to pandas datetime
        date_fields = ['created_at', 'updated_at']
        for field in date_fields:
            if field in organizations_df.columns:
                organizations_df[field] = pd.to_datetime(organizations_df[field], errors='coerce')

        # Final column filtering and type enforcement according to the schema
        final_petl_table = etl.fromdataframe(organizations_df)
        schema_columns = [field['name'] for field in schema]
        final_petl_table = etl.cut(final_petl_table, *schema_columns)
        final_petl_table = convert_column_types(final_petl_table, schema)

        # Preview final result as PETL table
        preview_table(final_petl_table, message="Final transformed table:")

        result = etl.todataframe(final_petl_table)

        return result
    
    except Exception:
        logger.exception("Error transforming organizations data")
        raise
