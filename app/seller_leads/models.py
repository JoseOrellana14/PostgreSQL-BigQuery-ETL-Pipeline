from dataclasses import dataclass
from datetime import datetime
from typing import Optional, List, Dict, Any

@dataclass
class Seller_Lead:
    """Seller Lead data model."""
    seller_lead_id: int
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
    seller_price: Optional[float] = None
    stage_seller: Optional[str] = None
    accepted: Optional[bool] = None
    accepted_at: Optional[datetime] = None
    rejected_reason: Optional[str] = None
    property_id: Optional[int] = None
    intake_payload: Optional[Dict[str, Any]] = None
    intake_features: Optional[Dict[str, Any]] = None
    pre_capture_score: Optional[float] = None
    pre_score_version: Optional[str] = None
    pre_scored_at: Optional[datetime] = None
    state: Optional[int] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    created_by: Optional[str] = None
    updated_by: Optional[str] = None


    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Seller_Lead':
        """Create a Seller Lead instance from a dictionary."""
        return cls(**data)

    def to_dict(self) -> Dict[str, Any]:
        """Convert the Seller Lead instance to a dictionary."""
        return {k: v for k, v in self.__dict__.items() if v is not None}
    
# Schema paths
SELLER_LEAD_SCHEMA_PATH = 'schemas/seller_leads.json'

# Table names
SELLER_LEAD_TABLE_NAME = 'seller_leads'