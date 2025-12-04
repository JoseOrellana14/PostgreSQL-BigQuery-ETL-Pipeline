import petl as etl
import pandas as pd
import json
import logging
from app.common.utils import preview_table, convert_column_types, load_schema
from app.users.models import USER_SCHEMA_PATH
from logging_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

def transfrom_users(users_table, load_date):
    """Transform users data and convert to a clean DataFrame"""
    try:
        # Load the schema
        schema = load_schema(USER_SCHEMA_PATH)

        # Preview the input table
        preview_table(users_table, message="Input users table:")

        # Conver column types based on the schema
        users_table = convert_column_types(users_table, schema)

        # Convert PETL table to pandas DataFrame
        users_df = etl.todataframe(users_table)

        # Mandatory Keys
        mandatory_keys = ['user_id', 'organization_id']
        for key in mandatory_keys:
            if key in users_df.columns:
                users_df[key] = pd.to_numeric(users_df[key], errors='coerce')
        users_df = users_df.dropna(subset=mandatory_keys)
        for key in mandatory_keys:
            if key in users_df.columns:
                users_df[key] = users_df[key].astype('int64')

        # Getting the table's load date
        users_df['load_date'] = load_date

        # Additional transformations
        # Force last_name data type to STRING and capitalize
        users_df['last_name'] = users_df['last_name'].astype(str).str.title()
        
        # Fill NaN values in name with empty strings and capitalize
        users_df['name'] = users_df['name'].fillna('').str.title()

        # Create full_name column by concatenating the name components
        # Only include non-empty and non-space-only values
        name_components = []
        
        # Helper function to check if a value is meaningful
        def is_meaningful(value):
            return pd.notna(value) and str(value).strip() != ''
        
        # Add each name component if it's meaningful
        if 'last_name' in users_df.columns:
            name_components.append(users_df['last_name'])
        if 'name' in users_df.columns:
            name_components.append(users_df['name'])
        
        # Join only the meaningful components with spaces
        users_df['full_name'] = name_components[0].apply(lambda x: x if is_meaningful(x) else '')
        for component in name_components[1:]:
            users_df['full_name'] = users_df['full_name'] + ' ' + \
                                      component.apply(lambda x: x if is_meaningful(x) else '')
        
        # Clean up the full_name column (remove extra spaces)
        users_df['full_name'] = users_df['full_name'].str.strip().replace(r'\s+', ' ', regex=True)


        # Explicityly convert date fields to pandas datetime
        date_fields = ['created_at', 'updated_at']
        for field in date_fields:
            if field in users_df.columns:
                users_df[field] = pd.to_datetime(users_df[field], errors='coerce')

        # Final column filtering and type enforcement according to the schema
        final_petl_table = etl.fromdataframe(users_df)
        schema_columns = [field['name'] for field in schema]
        final_petl_table = etl.cut(final_petl_table, *schema_columns)
        final_petl_table = convert_column_types(final_petl_table, schema)

        # Preview final result as PETL table
        preview_table(final_petl_table, message="Final transformed table:")

        result = etl.todataframe(final_petl_table)

        return result
    
    except Exception:
        logger.exception("Error transforming users data")
        raise
