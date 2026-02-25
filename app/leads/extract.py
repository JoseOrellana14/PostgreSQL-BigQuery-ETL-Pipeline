import petl as etl
import psycopg2
import logging
from app.common.config import get_connection_string
from app.common.utils import preview_table
from logging_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

def extract_leads(last_loaded=None):
    """Extract property units data from PostgreSQL database using PETL"""

    #Define the SQL query to extract leads
    query = f"""
    SELECT
        lead_id,
        user_id,
        related_lead_id,
        prospecto_numero_whatsapp,
        prospecto_nombre_whatsapp,
        bot_estado,
        plataforma_origen,
        prospecto_etapa,
        prospecto_interes_cuantitativo,
        proxima_cita_inicio,
        agente_notas_personales,
        primer_mensaje_recibido,
        ultimo_mensaje_recibido,
        prospecto_email,
        asistente_resumen,
        id_registro_notion,
        proxima_cita_fin,
        registro_creado_el AS created_at,
        registro_ultima_actualizacion AS updated_at,
        prospecto_intencion_categoria,
        prospecto_urgencia,
        prospecto_perfil,
        prospecto_motivacion_categoria,
        registro_notion_estado,
        registro_notion_creado_por,
        registro_eliminado_el,
        prospecto_nombre_agendado,
        registro_interno_creado_por,
        ultimo_analisis_ia_at,
        last_agent_message_at
    FROM public.prospectos
    """

    # Compare with last_loaded date for incremental load
    if last_loaded:
        query += f"""
        WHERE (
            registro_creado_el > '{last_loaded}'
            OR registro_ultima_actualizacion > '{last_loaded}'
        )
        """
    else:
        query += " WHERE registro_creado_el IS NOT NULL"

    try:
        # Use a context manager to handle the database connection
        with psycopg2.connect(get_connection_string()) as conn:
            # Use petl to extract data from the database
            leads_table = etl.fromdb(conn, query)

            # Materialize the data before exiting the context manager to avoid lazy evaluation issues
            leads_table = leads_table.cache()

        # Return the PETL table
        return leads_table

    except Exception:
        logger.exception("Error extracting leads data")
        raise