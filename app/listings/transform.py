import petl as etl
import pandas as pd
import json
import logging
from app.common.utils import preview_table, convert_column_types, load_schema
from app.listings.models import LISTING_SCHEMA_PATH, LISTING_TABLE_NAME
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
            'area_sqm': 10000000,  # 10 million sqm for area (acres)
            'price': 100000000,  # 100 million for property price
            'price_sqm': 50000  # 50,000 per sqm
        }
        
        # Get the limit for this field
        limit = limits.get(field_name, 1000000)  # Default to 1M if field not in limits
        
        # If value is negative or exceeds limit, return None
        if value < 0 or value > limit:
            return None
            
        return value
        
    except (ValueError, TypeError):
        return None

def transform_listings(listings_table, load_date):
    """Transform listings data and convert to a clean DataFrame"""
    try:
        # Load the schema
        schema = load_schema(LISTING_SCHEMA_PATH)
        # Preview the input table
        preview_table(listings_table, message="Input listings table:")

        # Conver column types based on the schema
        listings_table = convert_column_types(listings_table, schema)

        # Convert PETL table to pandas DataFrame
        listings_df = etl.todataframe(listings_table)

        # Mandatory Keys
        mandatory_keys = ['listing_id', 'user_id']
        for key in mandatory_keys:
            if key in listings_df.columns:
                listings_df[key] = pd.to_numeric(listings_df[key], errors='coerce')
        listings_df = listings_df.dropna(subset=mandatory_keys)
        for key in mandatory_keys:
            if key in listings_df.columns:
                listings_df[key] = listings_df[key].astype('int64')

        # Getting the table's load date
        listings_df['load_date'] = load_date

        # Explicityly convert date fields to pandas datetime
        date_fields = ['created_at', 'updated_at', 'record_deleted_at', 'load_date']
        for field in date_fields:
            if field in listings_df.columns:
                listings_df[field] = pd.to_datetime(listings_df[field], errors='coerce')

        # Validate money-related fields
        money_fields = [
            'area_sqm',
            'price',
            'price_sqm'
        ]
        
        for field in money_fields:
            if field in listings_df.columns:
                listings_df[field] = listings_df[field].apply(lambda x: validate_money_field(x, field))

        # Final column filtering and type enforcement according to the schema
        final_petl_table = etl.fromdataframe(listings_df)
        schema_columns = [field['name'] for field in schema]
        final_petl_table = etl.cut(final_petl_table, *schema_columns)
        final_petl_table = convert_column_types(final_petl_table, schema)

        # Preview final result as PETL table
        preview_table(final_petl_table, message="Final transformed table:")

        result = etl.todataframe(final_petl_table)

        return result
    
    except Exception:
        logger.exception("Error transforming listings data")
        raise
