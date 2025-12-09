import petl as etl
import pandas as pd
import json
import logging
from app.common.utils import preview_table, convert_column_types, load_schema
from app.seller_leads.models import SELLER_LEAD_SCHEMA_PATH
from logging_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

def transform_seller_leads(seller_leads_table, load_date):
    """Transform seller leads data and convert to a clean DataFrame"""
    try:
        # Load the schema
        schema = load_schema(SELLER_LEAD_SCHEMA_PATH)

        # Preview the input table
        preview_table(seller_leads_table, message="Input seller leads table:")

        # Conver column types based on the schema
        seller_leads_table = convert_column_types(seller_leads_table, schema)

        # Convert PETL table to pandas DataFrame
        seller_leads_df = etl.todataframe(seller_leads_table)

        # Mandatory Keys
        mandatory_keys = ['seller_lead_id', 'user_id', 'organization_id']
        for key in mandatory_keys:
            if key in seller_leads_df.columns:
                seller_leads_df[key] = pd.to_numeric(seller_leads_df[key], errors='coerce')
        seller_leads_df = seller_leads_df.dropna(subset=mandatory_keys)
        for key in mandatory_keys:
            if key in seller_leads_df.columns:
                seller_leads_df[key] = seller_leads_df[key].astype('int64')

        # Getting the table's load date
        seller_leads_df['load_date'] = load_date

        # Additional transformations
        # Force last_name data type to STRING and capitalize
        seller_leads_df['last_name'] = seller_leads_df['last_name'].astype(str).str.title()
        
        # Fill NaN values in name with empty strings and capitalize
        seller_leads_df['name'] = seller_leads_df['name'].fillna('').str.title()

        # Create full_name column by concatenating the name components
        # Only include non-empty and non-space-only values
        name_components = []
        
        # Helper function to check if a value is meaningful
        def is_meaningful(value):
            return pd.notna(value) and str(value).strip() != ''
        
        # Add each name component if it's meaningful
        if 'last_name' in seller_leads_df.columns:
            name_components.append(seller_leads_df['last_name'])
        if 'name' in seller_leads_df.columns:
            name_components.append(seller_leads_df['name'])
        
        # Join only the meaningful components with spaces
        seller_leads_df['full_name'] = name_components[0].apply(lambda x: x if is_meaningful(x) else '')
        for component in name_components[1:]:
            seller_leads_df['full_name'] = seller_leads_df['full_name'] + ' ' + \
                                      component.apply(lambda x: x if is_meaningful(x) else '')
        
        # Clean up the full_name column (remove extra spaces)
        seller_leads_df['full_name'] = seller_leads_df['full_name'].str.strip().replace(r'\s+', ' ', regex=True)


        # Explicityly convert date fields to pandas datetime
        date_fields = ['created_at', 'updated_at', 'load_date']
        for field in date_fields:
            if field in seller_leads_df.columns:
                seller_leads_df[field] = pd.to_datetime(seller_leads_df[field], errors='coerce')

        # Final column filtering and type enforcement according to the schema
        final_petl_table = etl.fromdataframe(seller_leads_df)
        schema_columns = [field['name'] for field in schema]
        final_petl_table = etl.cut(final_petl_table, *schema_columns)
        final_petl_table = convert_column_types(final_petl_table, schema)

        # Preview final result as PETL table
        preview_table(final_petl_table, message="Final transformed table:")

        result = etl.todataframe(final_petl_table)

        return result
    
    except Exception:
        logger.exception("Error transforming seller leads data")
        raise
