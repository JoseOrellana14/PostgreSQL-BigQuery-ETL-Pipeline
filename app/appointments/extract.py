import petl as etl
import psycopg2
import logging
from app.common.config import get_connection_string
from app.common.utils import preview_table
from logging_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

def extract_appointments(last_loaded=None):
    """Extract appointments data from PostgreSQL database using PETL"""

    #Define the SQL query to extract appointments
    query = f"""
    SELECT
        id as appointment_id,
        user_id,
        listing_id,
        lead_id,
        opportunity_id,
        gcal_event_id,
        start_at,
        end_at,
        modality,
        showup,
        record_status,
        created_at,
        updated_at,
        created_by,
        updated_by,
        status,
        remind_at,
        reminder_sent_at,
        summary,
        post_followup_at,
        post_followup_sent_at
    FROM public.appointments
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
            appointments_table = etl.fromdb(conn, query)

            # Materialize the data before exiting the context manager to avoid lazy evaluation issues
            appointments_table = appointments_table.cache()

        # Return the PETL table
        return appointments_table

    except Exception:
        logger.exception("Error extracting appointments data")
        raise