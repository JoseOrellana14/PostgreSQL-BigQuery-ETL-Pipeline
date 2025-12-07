import petl as etl
import psycopg2
import logging
from app.common.config import get_connection_string
from app.common.utils import preview_table
from logging_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

def extract_seller_leads(last_loaded=None):
    """Extract seller leads data from PostgreSQL database using PETL"""

    #Define the SQL query to extract seller leads
    query = f"""
    SELECT
        id as seller_lead_id,
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
        seller_price,
        stage_seller,
        reason_to_sell,
        willingness_to_negotiate,
        accepted,
        accepted_at,
        rejected_reason,
        property_unit_id,
        intake_payload,
        intake_features,
        pre_capture_score,
        pre_score_version,
        pre_scored_at,
        state,
        created_at,
        updated_at,
        created_by,
        updated_by
    FROM public.seller_leads
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
            seller_leads_table = etl.fromdb(conn, query)

            # Materialize the data before exiting the context manager to avoid lazy evaluation issues
            seller_leads_table = seller_leads_table.cache()

        # Return the PETL table
        return seller_leads_table

    except Exception:
        logger.exception("Error extracting seller leads data")
        raise