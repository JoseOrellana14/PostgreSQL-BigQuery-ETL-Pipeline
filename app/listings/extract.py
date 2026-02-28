import petl as etl
import psycopg2
import logging
from app.common.config import get_connection_string
from app.common.utils import preview_table
from logging_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

def extract_listings(last_loaded=None):
    """Extract property units data from PostgreSQL database using PETL"""

    #Define the SQL query to extract listings
    query = f"""
    SELECT
        id as listing_id,
        user_id,
        listing_project_id,
        operation_type,
        property_type,
        property_regime,
        neighborhood_macro,
        neighborhood,
        bedrooms,
        bathrooms,
        area_sqm,
        amenities,
        price,
        target_currency,
        price_sqm,
        qualitative_description,
        record_status,
        record_created_at AS created_at,
        record_updated_at AS updated_at,
        record_created_by,
        record_updated_by,
        name,
        collection_method,
        promoted_opportunity_id,
        estado,
        floor_level,
        floor_level_tag,
        floors_count,
        address,
        visibility,
        location_reference_raw,
        main_road,
        intersecting_road,
        anillo_min,
        anillo_max,
        lat,
        lng,
        google_maps_link,
        owner_name,
        personal_notes,
        brochure_name,
        url_brochure,
        notion_brochure_key,
        photo_name,
        url_photo,
        notion_photo_key,
        record_deleted_at,
        down_payment,
        exchange_rate,
        price_adjusted
    FROM public.listings
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
        logger.exception("Error extracting listings data")
        raise