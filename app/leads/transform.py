import petl as etl
import pandas as pd
import json
import logging
from app.common.utils import preview_table, convert_column_types, load_schema
from app.leads.models import LEAD_SCHEMA_PATH, LEAD_TABLE_NAME
from logging_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)


def transform_leads(leads_table, load_date):
    """Transform leads data and convert to a clean DataFrame"""
    try:
        # Load the schema
        schema = load_schema(LEAD_SCHEMA_PATH)
        # Preview the input table
        preview_table(leads_table, message="Input leads table:")

        # Conver column types based on the schema
        leads_table = convert_column_types(leads_table, schema)

        # Convert PETL table to pandas DataFrame
        leads_df = etl.todataframe(leads_table)

        # Mandatory Keys
        mandatory_keys = ['lead_id', 'user_id']
        for key in mandatory_keys:
            if key in leads_df.columns:
                leads_df[key] = pd.to_numeric(leads_df[key], errors='coerce')
        leads_df = leads_df.dropna(subset=mandatory_keys)
        for key in mandatory_keys:
            if key in leads_df.columns:
                leads_df[key] = leads_df[key].astype('int64')

        # Getting the table's load date
        leads_df['load_date'] = load_date

        # Explicityly convert date fields to pandas datetime
        date_fields = ['proxima_cita_inicio', 'primer_mensaje_recibido', 'ultimo_mensaje_recibido', 'proxima_cita_fin', 'registro_creado_el', 'registro_ultima_actualizacion', 'registro_eliminado_el', 'ultimo_analisis_ia_at', 'last_agent_message_at', 'load_date']
        for field in date_fields:
            if field in leads_df.columns:
                leads_df[field] = pd.to_datetime(leads_df[field], errors='coerce')

        # Final column filtering and type enforcement according to the schema
        final_petl_table = etl.fromdataframe(leads_df)
        schema_columns = [field['name'] for field in schema]
        final_petl_table = etl.cut(final_petl_table, *schema_columns)
        final_petl_table = convert_column_types(final_petl_table, schema)

        # Preview final result as PETL table
        preview_table(final_petl_table, message="Final transformed table:")

        result = etl.todataframe(final_petl_table)

        return result
    
    except Exception:
        logger.exception("Error transforming leads data")
        raise
