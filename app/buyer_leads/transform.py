import petl as etl
import pandas as pd
import json
import logging
from app.common.utils import preview_table, convert_column_types, load_schema
from app.buyer_leads.models import BUYER_LEAD_SCHEMA_PATH
from logging_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

def transfrom_buyer_leads(buyer_leads_table, load_date):
    """Transform buyer leads data and convert to a clean DataFrame"""
    try:
        # Load the schema
        schema = load_schema(BUYER_LEAD_SCHEMA_PATH)

        # Preview the input table
        preview_table(buyer_leads_table, message="Input buyer leads table:")

        # Conver column types based on the schema
        buyer_leads_table = convert_column_types(buyer_leads_table, schema)

        # Convert PETL table to pandas DataFrame
        buyer_leads_df = etl.todataframe(buyer_leads_table)

        # Mandatory Keys
        mandatory_keys = ['buer_lead_id', 'user_id', 'organization_id']
        for key in mandatory_keys:
            if key in buyer_leads_df.columns:
                buyer_leads_df[key] = pd.to_numeric(buyer_leads_df[key], errors='coerce')
        buyer_leads_df = buyer_leads_df.dropna(subset=mandatory_keys)
        for key in mandatory_keys:
            if key in buyer_leads_df.columns:
                buyer_leads_df[key] = buyer_leads_df[key].astype('int64')

        # Getting the table's load date
        buyer_leads_df['load_date'] = load_date

        # Explicityly convert date fields to pandas datetime
        date_fields = ['created_at', 'updated_at']
        for field in date_fields:
            if field in buyer_leads_df.columns:
                buyer_leads_df[field] = pd.to_datetime(buyer_leads_df[field], errors='coerce')

        # Final column filtering and type enforcement according to the schema
        final_petl_table = etl.fromdataframe(buyer_leads_df)
        schema_columns = [field['name'] for field in schema]
        final_petl_table = etl.cut(final_petl_table, *schema_columns)
        final_petl_table = convert_column_types(final_petl_table, schema)

        # Preview final result as PETL table
        preview_table(final_petl_table, message="Final transformed table:")

        result = etl.todataframe(final_petl_table)

        return result
    
    except Exception:
        logger.exception("Error transforming buyer leads data")
        raise
