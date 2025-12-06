from dataclasses import dataclass
from datetime import datetime
from typing import Optional, List, Dict, Any

@dataclass
class Property_Sale:
    """Property Sale data model."""
    property_sale_id: int
    organization_id: int
    user_id: int
    buyer_lead_id: int
    seller_lead_id: int
    property_opportunity_id: int
    property_unit_id: int
    sale_price: Optional[float] = None
    currency: Optional[str] = None
    closed_at: Optional[datetime] = None
    commission_percent: Optional[float] = None
    commission_amount: Optional[float] = None
    state: Optional[int] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    created_by: Optional[str] = None
    updated_by: Optional[str] = None


    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Property_Sale':
        """Create a Property Sale instance from a dictionary."""
        return cls(**data)

    def to_dict(self) -> Dict[str, Any]:
        """Convert the Property Sale instance to a dictionary."""
        return {k: v for k, v in self.__dict__.items() if v is not None}
    
# Schema paths
PROPERTY_SALE_SCHEMA_PATH = 'schemas/property_sales.json'

# Table names
PROPERTY_SALE_TABLE_NAME = 'property_sales'