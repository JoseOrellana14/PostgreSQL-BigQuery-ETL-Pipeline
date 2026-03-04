from dataclasses import dataclass
from datetime import datetime
from typing import Optional, List, Dict, Any

@dataclass
class Listing_Sale:
    """Listing Sale data model."""
    listing_sale_id: int
    organization_id: int
    user_id: int
    lead_id: Optional[int] = None
    opportunity_id: Optional[int] = None
    listing_id: Optional[int] = None
    listing_project_id: Optional[int] = None
    sale_price: Optional[float] = None
    sale_operation: Optional[str] = None
    currency: Optional[str] = None
    closed_at: Optional[datetime] = None
    state: Optional[int] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    created_by: Optional[str] = None
    updated_by: Optional[str] = None
    deleted_at: Optional[datetime] = None


    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Listing_Sale':
        """Create a Listing_Sale instance from a dictionary."""
        return cls(**data)

    def to_dict(self) -> Dict[str, Any]:
        """Convert the Listing_Sale instance to a dictionary."""
        return {k: v for k, v in self.__dict__.items() if v is not None}
    
# Schema paths
LISTING_SALE_SCHEMA_PATH = 'schemas/listing_sales.json'

# Table names
LISTING_SALE_TABLE_NAME = 'raw_listing_sales'