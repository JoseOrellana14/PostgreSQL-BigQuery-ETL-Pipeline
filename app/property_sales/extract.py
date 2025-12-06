import petl as etl
import psycopg2
import logging
from app.common.config import get_connection_string
from app.common.utils import preview_table
from logging_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

def extract_property_sales(last_loaded=None):
    """Extract property sales data from PostgreSQL database using PETL"""

    #Define the SQL query to extract property_sales
    query = f"""
    SELECT
        id as property_sale_id,
        organization_id,
        user_id,
        buyer_lead_id,
        seller_lead_id,
        property_opportunity_id,
        property_unit_id,
        sale_price,
        currency,
        closed_at,
        commission_percent,
        commission_amount,
        state,
        created_at,
        updated_at,
        created_by,
        updated_by
    FROM public.property_sales
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
            property_sales_table = etl.fromdb(conn, query)

            # Materialize the data before exiting the context manager to avoid lazy evaluation issues
            property_sales_table = property_sales_table.cache()

        # Return the PETL table
        return property_sales_table

    except Exception:
        logger.exception("Error extracting property sales data")
        raise