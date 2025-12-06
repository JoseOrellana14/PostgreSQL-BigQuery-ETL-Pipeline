import petl as etl
import pandas as pd
import json
import logging
from app.common.utils import preview_table, convert_column_types, load_schema
from app.chat_messages.models import CHAT_MESSAGE_SCHEMA_PATH
from logging_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

def transform_chat_messages(chat_messages_table, load_date):
    """Transform chat messages data and convert to a clean DataFrame"""
    try:
        # Load the schema
        schema = load_schema(CHAT_MESSAGE_SCHEMA_PATH)

        # Preview the input table
        preview_table(chat_messages_table, message="Input chat messages table:")

        # Conver column types based on the schema
        chat_messages_table = convert_column_types(chat_messages_table, schema)

        # Convert PETL table to pandas DataFrame
        chat_messages_df = etl.todataframe(chat_messages_table)

        # Mandatory Keys
        mandatory_keys = ['chat_message_id', 'organization_id', 'user_id', 'buyer_lead_id', 'seller_lead_id', 'property_opportunity_id']
        for key in mandatory_keys:
            if key in chat_messages_df.columns:
                chat_messages_df[key] = pd.to_numeric(chat_messages_df[key], errors='coerce')
        chat_messages_df = chat_messages_df.dropna(subset=mandatory_keys)
        for key in mandatory_keys:
            if key in chat_messages_df.columns:
                chat_messages_df[key] = chat_messages_df[key].astype('int64')

        # Getting the table's load date
        chat_messages_df['load_date'] = load_date

        # Explicityly convert date fields to pandas datetime
        date_fields = ['created_at', 'updated_at']
        for field in date_fields:
            if field in chat_messages_df.columns:
                chat_messages_df[field] = pd.to_datetime(chat_messages_df[field], errors='coerce')

        # Final column filtering and type enforcement according to the schema
        final_petl_table = etl.fromdataframe(chat_messages_df)
        schema_columns = [field['name'] for field in schema]
        final_petl_table = etl.cut(final_petl_table, *schema_columns)
        final_petl_table = convert_column_types(final_petl_table, schema)

        # Preview final result as PETL table
        preview_table(final_petl_table, message="Final transformed table:")

        result = etl.todataframe(final_petl_table)

        return result
    
    except Exception:
        logger.exception("Error transforming chat messages data")
        raise
