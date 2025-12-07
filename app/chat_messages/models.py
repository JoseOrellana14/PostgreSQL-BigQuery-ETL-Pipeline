from dataclasses import dataclass
from datetime import datetime
from typing import Optional, List, Dict, Any

@dataclass
class Chat_Message:
    """Chat Message data model."""
    chat_message_id: int
    organization_id: int
    sender_phone: Optional[str] = None
    reciever_phone: Optional[str] = None
    user_id: Optional[int] = None
    buyer_lead_id: Optional[int] = None
    seller_lead_id: Optional[int] = None
    property_opportunity_id: Optional[int] = None
    channel: Optional[str] = None
    direction: Optional[str] = None
    sent_at: Optional[datetime] = None
    delivered_at: Optional[datetime] = None
    read_at: Optional[datetime] = None
    has_media: Optional[bool] = None
    body: Optional[str] = None
    char_count: Optional[int] = None
    state: Optional[int] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    created_by: Optional[str] = None
    updated_by: Optional[str] = None


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
CHAT_MESSAGE_TABLE_NAME = 'chat_messages'