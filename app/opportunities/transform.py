import petl as etl
import pandas as pd
import json
import logging
from app.common.utils import preview_table, convert_column_types, load_schema
from app.opportunities.models import OPPORTUNITY_SCHEMA_PATH
from logging_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)


def validate_money_field(value, field_name):
    """Validate money fields to ensure they have reasonable values for Bolivia."""
    if pd.isna(value):
        return None
        
    try:
        # Convert to float
        value = float(value)
        
        # Define reasonable limits (in USD)
        limits = {
            'budget': 100000000,  # 100 million for property price
            'counter_price': 100000000,  # 100 million for property price
            'final_price': 100000000  # 100 million for property price
        }
        
        # Get the limit for this field
        limit = limits.get(field_name, 1000000)  # Default to 1M if field not in limits
        
        # If value is negative or exceeds limit, return None
        if value < 0 or value > limit:
            return None
            
        return value
        
    except (ValueError, TypeError):
        return None
    

def transform_opportunities(opportunities_table, load_date):
    """Transform opportunities data and convert to a clean DataFrame"""
    try:
        # Load the schema
        schema = load_schema(OPPORTUNITY_SCHEMA_PATH)
        # Preview the input table
        preview_table(opportunities_table, message="Input opportunities table:")

        # Conver column types based on the schema
        opportunities_table = convert_column_types(opportunities_table, schema)

        # Convert PETL table to pandas DataFrame
        opportunities_df = etl.todataframe(opportunities_table)

        # Mandatory Keys
        mandatory_keys = ['opportunity_id', 'user_id', 'lead_id']
        for key in mandatory_keys:
            if key in opportunities_df.columns:
                opportunities_df[key] = pd.to_numeric(opportunities_df[key], errors='coerce')
        opportunities_df = opportunities_df.dropna(subset=mandatory_keys)
        for key in mandatory_keys:
            if key in opportunities_df.columns:
                opportunities_df[key] = opportunities_df[key].astype('int64')

        # Getting the table's load date
        opportunities_df['load_date'] = load_date

        # Explicityly convert date fields to pandas datetime
        date_fields = ['created_at', 'updated_at', 'load_date']
        for field in date_fields:
            if field in opportunities_df.columns:
                opportunities_df[field] = pd.to_datetime(opportunities_df[field], errors='coerce')

        # Validate money-related fields
        money_fields = [
            'budget',
            'counter_price',
            'final_price'
        ]
        
        for field in money_fields:
            if field in opportunities_df.columns:
                opportunities_df[field] = opportunities_df[field].apply(lambda x: validate_money_field(x, field))


        # Final column filtering and type enforcement according to the schema
        final_petl_table = etl.fromdataframe(opportunities_df)
        schema_columns = [field['name'] for field in schema]
        final_petl_table = etl.cut(final_petl_table, *schema_columns)
        final_petl_table = convert_column_types(final_petl_table, schema)

        # Preview final result as PETL table
        preview_table(final_petl_table, message="Final transformed table:")

        result = etl.todataframe(final_petl_table)

        return result
    
    except Exception:
        logger.exception("Error transforming property opportunities data")
        raise
