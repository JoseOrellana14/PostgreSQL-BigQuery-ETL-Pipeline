import petl as etl
import psycopg2
import logging
from app.common.config import get_connection_string
from app.common.utils import preview_table
from logging_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

def extract_property_units(last_loaded=None):
    """Extract property units data from PostgreSQL database using PETL"""

    #Define the SQL query to extract property_units
    query = f"""
    SELECT
        id as property_unit_id,
        organization_id,
        user_id,
        seller_lead_id,
        property_type,
        neighborhood,
        bedrooms,
        area_sqm,
        price,
        currency,
        operation_type,
        listing_status,
        amenities,
        state,
        created_at,
        updated_at,
        created_by,
        updated_by
    FROM public.property_units
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
            property_units_table = etl.fromdb(conn, query)

            # Materialize the data before exiting the context manager to avoid lazy evaluation issues
            property_units_table = property_units_table.cache()

        # Return the PETL table
        return property_units_table

    except Exception:
        logger.exception("Error extracting property units data")
        raise