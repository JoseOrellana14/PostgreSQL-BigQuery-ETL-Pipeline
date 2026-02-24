import petl as etl
import psycopg2
import logging
from app.common.config import get_connection_string
from app.common.utils import preview_table
from logging_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

def extract_opportunities(last_loaded=None):
    """Extract opportunities data from PostgreSQL database using PETL"""

    #Define the SQL query to extract opportunities
    query = f"""
    SELECT
        id as opportunity_id,
        user_id,
        lead_id,
        listing_id,
        listing_project_id,
        lead_role,
        lead_name,
        property_name,
        operation_type,
        property_type,
        property_regime,
        neighborhood_macro,
        neighborhood,
        bedrooms,
        bathrooms,
        area_sqm,
        amenities,
        budget,
        target_currency,
        counter_price,
        counter_price_at,
        final_price,
        final_price_at,
        price_sqm,
        qualitative_description,
        additional_criteria,
        financing_available,
        counterparty_preference,
        stage,
        next_appointment,
        channel_appointment,
        source_appointment,
        showup_last_appointment,
        qualitative_interest,
        opportunity_scoring,
        primary_motive,
        timeframe,
        price_flexibility,
        personal_notes,
        created_at,
        updated_at,
        notion_record_created_by,
        internal_record_created_by,
        notion_record_deleted_at,
        notion_record_status,
        floor_level,
        last_opportunity
    FROM public.opportunities
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
            opportunities_table = etl.fromdb(conn, query)

            # Materialize the data before exiting the context manager to avoid lazy evaluation issues
            opportunities_table = opportunities_table.cache()

        # Return the PETL table
        return opportunities_table

    except Exception:
        logger.exception("Error extracting opportunities data")
        raise