from dataclasses import dataclass
from datetime import datetime
from typing import Optional, List, Dict, Any

@dataclass
class Buyer_Lead:
    """Buyer Lead data model."""
    buyer_lead_id: int
    organization_id: int
    user_id: int
    name: Optional[str] = None
    last_name: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    origin_channel: Optional[str] = None
    origin_detail: Optional[str] = None
    status: Optional[str] = None
    chatbot_enabled: Optional[bool] = None
    next_appointment_at: Optional[datetime] = None
    lead_intent: Optional[str] = None
    property_type_pref: Optional[str] = None
    state: Optional[int] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    created_by: Optional[str] = None
    updated_by: Optional[str] = None


    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Buyer_Lead':
        """Create a Buyer Lead instance from a dictionary."""
        return cls(**data)

    def to_dict(self) -> Dict[str, Any]:
        """Convert the Buyer Lead instance to a dictionary."""
        return {k: v for k, v in self.__dict__.items() if v is not None}
    
# Schema paths
BUYER_LEAD_SCHEMA_PATH = 'schemas/buyer_leads.json'

# Table names
BUYER_LEAD_TABLE_NAME = 'buyer_leads'