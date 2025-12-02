import petl as etl
import psycopg2
import logging
from app.common.config import get_connection_string
from app.common.utils import preview_table
from logging_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

def extract_organizations(last_loaded=None):
    """Extract organizations data from PostgreSQL database using PETL"""

    #Define the SQL query to extract organizations
    query = f"""
    SELECT
        id as organization_id,
        legal_name,
        commercial_name,
        chatbot_enabled_default,
        is_active,
        state,
        created_at,
        updated_at,
        created_by,
        updated_by
    FROM public.organizations
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
            organizations_table = etl.fromdb(conn, query)

            # Materialize the data before exiting the context manager to avoid lazy evaluation issues
            organizations_table = organizations_table.cache()

        # Return the PETL table
        return organizations_table

    except Exception:
        logger.exception("Error extracting organizations data")
        raise