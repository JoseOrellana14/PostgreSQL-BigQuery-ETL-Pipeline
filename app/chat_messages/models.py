from dataclasses import dataclass
from datetime import datetime
from typing import Optional, List, Dict, Any

@dataclass
class Chat_Message:
    """Chat Message data model."""
    chat_message_id: int
    lead_id: int
    user_id: int
    session_id: Optional[int] = None
    telegram_chat_id: Optional[int] = None
    content: Optional[str] = None
    emisor: Optional[str] = None
    receptor: Optional[str] = None
    registro_creado_el: Optional[datetime] = None
    mensaje_enviado_el: Optional[datetime] = None
    tipo_emisor: Optional[str] = None
    estatus: Optional[str] = None
    tipo_mensaje: Optional[str] = None
    api_url: Optional[str] = None
    chat_id: Optional[str] = None
    wamid: Optional[str] = None
    filename: Optional[str] = None
    file_mime_type: Optional[str] = None
    file_extension: Optional[str] = None
    file_summary: Optional[str] = None
    file_keywords: Optional[str] = None
    file_character_length: Optional[int] = None
    plataforma_origen: Optional[str] = None
    registro_actualizado_el: Optional[datetime] = None



    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Chat_Message':
        """Create a Chat Message instance from a dictionary."""
        return cls(**data)

    def to_dict(self) -> Dict[str, Any]:
        """Convert the Chat Message instance to a dictionary."""
        return {k: v for k, v in self.__dict__.items() if v is not None}
    
# Schema paths
CHAT_MESSAGE_SCHEMA_PATH = 'schemas/chat_messages.json'

# Table names
CHAT_MESSAGE_TABLE_NAME = 'raw_chat_messages'