import petl as etl
import pandas as pd
import json
import logging
from app.common.utils import preview_table, convert_column_types, load_schema
from app.appointments.models import APPOINTMENT_SCHEMA_PATH
from logging_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

def transform_appointments(appointments_table, load_date):
    """Transform appointments data and convert to a clean DataFrame"""
    try:
        # Load the schema
        schema = load_schema(APPOINTMENT_SCHEMA_PATH)

        # Preview the input table
        preview_table(appointments_table, message="Input appointments table:")

        # Conver column types based on the schema
        appointments_table = convert_column_types(appointments_table, schema)

        # Convert PETL table to pandas DataFrame
        appointments_df = etl.todataframe(appointments_table)

        # Mandatory Keys
        mandatory_keys = ['appointment_id', 'organization_id', 'user_id', 'buyer_lead_id', 'seller_lead_id', 'property_opportunity_id', 'property_unit_id']
        for key in mandatory_keys:
            if key in appointments_df.columns:
                appointments_df[key] = pd.to_numeric(appointments_df[key], errors='coerce')
        appointments_df = appointments_df.dropna(subset=mandatory_keys)
        for key in mandatory_keys:
            if key in appointments_df.columns:
                appointments_df[key] = appointments_df[key].astype('int64')

        # Getting the table's load date
        appointments_df['load_date'] = load_date

        # Explicityly convert date fields to pandas datetime
        date_fields = ['created_at', 'updated_at', 'load_date']
        for field in date_fields:
            if field in appointments_df.columns:
                appointments_df[field] = pd.to_datetime(appointments_df[field], errors='coerce')

        # Final column filtering and type enforcement according to the schema
        final_petl_table = etl.fromdataframe(appointments_df)
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
