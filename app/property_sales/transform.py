import petl as etl
import pandas as pd
import json
import logging
from app.common.utils import preview_table, convert_column_types, load_schema
from app.property_sales.models import PROPERTY_SALE_SCHEMA_PATH
from logging_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

def transform_property_sales(property_sales_table, load_date):
    """Transform property sales data and convert to a clean DataFrame"""
    try:
        # Load the schema
        schema = load_schema(PROPERTY_SALE_SCHEMA_PATH)

        # Preview the input table
        preview_table(property_sales_table, message="Input property sales table:")

        # Conver column types based on the schema
        property_sales_table = convert_column_types(property_sales_table, schema)

        # Convert PETL table to pandas DataFrame
        property_sales_df = etl.todataframe(property_sales_table)

        # Mandatory Keys
        mandatory_keys = ['property_sale_id', 'organization_id', 'user_id', 'buyer_lead_id', 'seller_lead_id', 'property_opportunity_id', 'property_unit_id']
        for key in mandatory_keys:
            if key in property_sales_df.columns:
                property_sales_df[key] = pd.to_numeric(property_sales_df[key], errors='coerce')
        property_sales_df = property_sales_df.dropna(subset=mandatory_keys)
        for key in mandatory_keys:
            if key in property_sales_df.columns:
                property_sales_df[key] = property_sales_df[key].astype('int64')

        # Getting the table's load date
        property_sales_df['load_date'] = load_date

        # Explicityly convert date fields to pandas datetime
        date_fields = ['created_at', 'updated_at']
        for field in date_fields:
            if field in property_sales_df.columns:
                property_sales_df[field] = pd.to_datetime(property_sales_df[field], errors='coerce')

        # Final column filtering and type enforcement according to the schema
        final_petl_table = etl.fromdataframe(property_sales_df)
        schema_columns = [field['name'] for field in schema]
        final_petl_table = etl.cut(final_petl_table, *schema_columns)
        final_petl_table = convert_column_types(final_petl_table, schema)

        # Preview final result as PETL table
        preview_table(final_petl_table, message="Final transformed table:")

        result = etl.todataframe(final_petl_table)

        return result
    
    except Exception:
        logger.exception("Error transforming property sales data")
        raise
