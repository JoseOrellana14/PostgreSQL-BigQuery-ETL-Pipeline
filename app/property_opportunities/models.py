from dataclasses import dataclass
from datetime import datetime
from typing import Optional, List, Dict, Any

@dataclass
class Property_Opportunity:
    """Property Opportunity data model."""
    property_opportunity_id: int
    organization_id: int
    user_id: int
    buyer_lead_id: int
    seller_lead_id: int
    property_unit_id: int
    stage: Optional[str] = None
    bidding_state: Optional[str] = None
    next_appointment_at: Optional[datetime] = None
    last_contacted_at: Optional[datetime] = None
    initial_price: Optional[float] = None
    offer_price_buyer: Optional[float] = None
    counter_price_seller: Optional[float] = None
    last_offer_at: Optional[datetime] = None
    last_counter_at: Optional[datetime] = None
    pair_score: Optional[float] = None
    pair_score_version: Optional[str] = None
    pair_scored_at: Optional[datetime] = None
    is_won: Optional[bool] = None
    state: Optional[int] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    created_by: Optional[str] = None
    updated_by: Optional[str] = None


    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Property_Opportunity':
        """Create a Property Opportunity instance from a dictionary."""
        return cls(**data)

    def to_dict(self) -> Dict[str, Any]:
        """Convert the Property Opportunity instance to a dictionary."""
        return {k: v for k, v in self.__dict__.items() if v is not None}
    
# Schema paths
PROPERTY_OPPORTUNITY_SCHEMA_PATH = 'schemas/property_opprotunities.json'

# Table names
PROPERTY_OPPORTUNITY_TABLE_NAME = 'property_opprotunities'