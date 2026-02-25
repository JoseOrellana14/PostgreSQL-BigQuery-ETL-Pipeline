from dataclasses import dataclass
from datetime import datetime
from typing import Optional, List, Dict, Any

@dataclass
class Opportunity:
    """Opportunity data model."""
    opportunity_id: int
    user_id: int
    lead_id: int
    listing_id: Optional[int] = None
    listing_project_id: Optional[int] = None
    lead_role: Optional[str] = None
    lead_name: Optional[str] = None
    property_name: Optional[str] = None
    operation_type: Optional[str] = None
    property_type: Optional[str] = None
    property_regime: Optional[str] = None
    neighborhood_macro: Optional[str] = None
    neighborhood: Optional[str] = None
    bedrooms: Optional[int] = None
    bathrooms: Optional[int] = None
    area_sqm: Optional[float] = None
    amenities: Optional[List[str]] = None
    budget: Optional[float] = None
    target_currency: Optional[str] = None
    counter_price: Optional[float] = None
    counter_price_at: Optional[datetime] = None
    final_price: Optional[float] = None
    final_price_at: Optional[datetime] = None
    price_sqm: Optional[float] = None
    qualitative_description: Optional[str] = None
    additional_criteria: Optional[str] = None
    financing_available: Optional[str] = None
    counterparty_preference: Optional[str] = None
    stage: Optional[str] = None
    next_appointment: Optional[datetime] = None
    channel_appointment: Optional[str] = None
    source_appointment: Optional[str] = None
    showup_last_appointment: Optional[str] = None
    qualitative_interest: Optional[str] = None
    opportunity_scoring: Optional[float] = None
    primary_motive: Optional[str] = None
    timeframe: Optional[int] = None
    price_flexibility: Optional[str] = None
    personal_notes: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    notion_record_created_by: Optional[str] = None
    internal_record_created_by: Optional[str] = None
    notion_record_deleted_at: Optional[datetime] = None
    notion_record_status: Optional[str] = None
    floor_level: Optional[int] = None
    last_opportunity: Optional[bool] = None



    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Opportunity':
        """Create an Opportunity instance from a dictionary."""
        return cls(**data)

    def to_dict(self) -> Dict[str, Any]:
        """Convert the Opportunity instance to a dictionary."""
        return {k: v for k, v in self.__dict__.items() if v is not None}
    
# Schema paths
OPPORTUNITY_SCHEMA_PATH = 'schemas/opportunities.json'

# Table names
OPPORTUNITY_TABLE_NAME = 'raw_opportunities'