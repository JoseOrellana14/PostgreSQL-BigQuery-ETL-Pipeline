import petl as etl
import psycopg2
import logging
from app.common.config import get_connection_string
from app.common.utils import preview_table
from logging_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

def extract_listing_sales(last_loaded=None):
    """Extract property units data from PostgreSQL database using PETL"""

    #Define the SQL query to extract listings
    query = f"""
    SELECT
        id as listing_sale_id,
        organization_id,
        user_id,
        lead_id,
        opportunity_id,
        listing_id,
        listing_project_id,
        sale_price,
        currency,
        closed_at,
        sale_operation,
        state,
        created_at,
        updated_at,
        created_by,
        updated_by,
        deleted_at
    FROM public.listing_sales
    """

    # Compare with last_loaded date for incremental load
    if last_loaded:
        query += f"""
        WHERE (
            record_created_at > '{last_loaded}'
            OR record_updated_at > '{last_loaded}'
        )
        """
    else:
        query += " WHERE record_created_at IS NOT NULL"

    try:
        # Use a context manager to handle the database connection
        with psycopg2.connect(get_connection_string()) as conn:
            # Use petl to extract data from the database
            listings_table = etl.fromdb(conn, query)

            # Materialize the data before exiting the context manager to avoid lazy evaluation issues
            listings_table = listings_table.cache()

        # Return the PETL table
        return listings_table

    except Exception:
        logger.exception("Error extracting listing sales data")
        raise