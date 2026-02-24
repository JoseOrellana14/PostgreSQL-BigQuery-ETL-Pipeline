import petl as etl
import psycopg2
import logging
from app.common.config import get_connection_string
from app.common.utils import preview_table
from logging_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

def extract_users(last_loaded=None):
    """Extract users data from PostgreSQL database using PETL"""

    #Define the SQL query to extract users
    query = f"""
    SELECT
        user_id,
        organization_id,
        name,
        last_name,
        email,
        phone
        role,
        status,
        chatbot_enabled_user,
        state,
        created_at,
        updated_at,
        created_by,
        updated_by
    FROM public.users
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
            users_table = etl.fromdb(conn, query)

            # Materialize the data before exiting the context manager to avoid lazy evaluation issues
            users_table = users_table.cache()

        # Return the PETL table
        return users_table

    except Exception:
        logger.exception("Error extracting users data")
        raise