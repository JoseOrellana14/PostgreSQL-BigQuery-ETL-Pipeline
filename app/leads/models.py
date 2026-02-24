from dataclasses import dataclass
from datetime import datetime
from typing import Optional, List, Dict, Any

@dataclass
class Lead:
    """Lead data model."""
    lead_id: int
    user_id: int
    related_lead_id: Optional[int] = None
    prospecto_numero_whatsapp: Optional[int] = None
    prospecto_nombre_whatsapp: Optional[str] = None
    bot_estado: Optional[str] = None
    plataforma_origen: Optional[str] = None
    prospecto_etapa: Optional[str] = None
    prospecto_interes_cuantitativo: Optional[str] = None
    proxima_cita_inicio: Optional[datetime] = None
    agente_notas_personales: Optional[str] = None
    primer_mensaje_recibido: Optional[datetime] = None
    ultimo_mensaje_recibido: Optional[datetime] = None
    prospecto_email: Optional[str] = None
    asistente_resumen: Optional[str] = None
    id_registro_notion: Optional[str] = None
    proxima_cita_fin: Optional[datetime] = None
    registro_creado_el: Optional[datetime] = None
    registro_ultima_actualizacion: Optional[datetime] = None
    prospecto_intencion_categoria: Optional[str] = None
    prospecto_urgencia: Optional[str] = None
    prospecto_perfil: Optional[str] = None
    prospecto_motivacion_categoria: Optional[str] = None
    registro_notion_estado: Optional[str] = None
    registro_notion_creado_por: Optional[str] = None
    registro_eliminado_el: Optional[datetime] = None
    prospecto_nombre_agendado: Optional[str] = None
    registro_interno_creado_por: Optional[str] = None
    ultimo_analisis_ia_at: Optional[datetime] = None
    last_agent_message_at: Optional[datetime] = None
    

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Lead':
        """Create a Lead instance from a dictionary."""
        return cls(**data)

    def to_dict(self) -> Dict[str, Any]:
        """Convert the Lead instance to a dictionary."""
        return {k: v for k, v in self.__dict__.items() if v is not None}
    
# Schema paths
LEAD_SCHEMA_PATH = 'schemas/leads.json'

# Table names
LEAD_TABLE_NAME = 'raw_leads'