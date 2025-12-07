from dataclasses import dataclass
from datetime import datetime
from typing import Optional, List, Dict, Any

@dataclass
class Property_Unit:
    """Property Unit data model."""
    property_unit_id: int
    organization_id: int
    user_id: int
    seller_lead_id: int
    property_type: Optional[str] = None
    neighborhood: Optional[str] = None
    bedrooms: Optional[int] = None
    area_sqm: Optional[float] = None
    price: Optional[float] = None
    currency: Optional[str] = None
    operation_type: Optional[str] = None
    listing_status: Optional[str] = None
    amenities: Optional[Dict[str, Any]] = None
    state: Optional[int] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    created_by: Optional[str] = None
    updated_by: Optional[str] = None


    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Property_Unit':
        """Create a Property Unit instance from a dictionary."""
        return cls(**data)

    def to_dict(self) -> Dict[str, Any]:
        """Convert the Property Unit instance to a dictionary."""
        return {k: v for k, v in self.__dict__.items() if v is not None}
    
# Schema paths
PROPERTY_UNIT_SCHEMA_PATH = 'schemas/property_units.json'

# Table names
PROPERTY_UNIT_TABLE_NAME = 'property_units'