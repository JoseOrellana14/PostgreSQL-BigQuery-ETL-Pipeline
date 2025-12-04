import petl as etl
import psycopg2
import logging
from app.common.config import get_connection_string
from app.common.utils import preview_table
from logging_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

def extract_buyer_leads(last_loaded=None):
    """Extract buyer_leads data from PostgreSQL database using PETL"""

    #Define the SQL query to extract buyer_leads
    query = f"""
    SELECT
        id as buyer_lead_id,
        organization_id,
        user_id,
        name,
        last_name,
        email,
        phone,
        origin_channel,
        origin_detail,
        status,
        chatbot_enabled,
        next_appointment_at,
        lead_intent,
        property_type_pref,
        state,
        created_at,
        updated_at,
        created_by,
        updated_by
    FROM public.buyer_leads
    """

    # Compare with last_loaded date for incremental load
    if last_loaded:
        query += f"""
        WHERE (
            created_at > '{last_loaded}'
            OR updated_at > '{last_loaded}'
        )
        """
    else:
        query += " WHERE created_at IS NOT NULL"

    try:
        # Use a context manager to handle the database connection
        with psycopg2.connect(get_connection_string()) as conn:
            # Use petl to extract data from the database
            buyer_leads_table = etl.fromdb(conn, query)

            # Materialize the data before exiting the context manager to avoid lazy evaluation issues
            buyer_leads_table = buyer_leads_table.cache()

        # Return the PETL table
        return buyer_leads_table

    except Exception:
        logger.exception("Error extracting buyer leads data")
        raise