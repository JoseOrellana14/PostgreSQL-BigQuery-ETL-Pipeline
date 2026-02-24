from dataclasses import dataclass
from datetime import datetime
from typing import Optional, List, Dict, Any

@dataclass
class Listing_Project:
    """Listing Project data model."""
    listing_project_id: int
    user_id: int
    name: Optional[str] = None
    developer: Optional[str] = None
    city: Optional[str] = None
    neighborhood: Optional[str] = None
    neighborhood_macro: Optional[str] = None
    project_type: Optional[str] = None
    project_status: Optional[str] = None
    delivery_date: Optional[datetime] = None
    total_units: Optional[int] = None
    description: Optional[str] = None
    record_status: Optional[int] = None
    record_created_at: Optional[datetime] = None
    record_updated_at: Optional[datetime] = None
    floors_count: Optional[int] = None
    estado: Optional[str] = None
    visibility: Optional[str] = None
    id_registro_notion: Optional[str] = None
    record_deleted_at: Optional[datetime] = None
    record_created_by: Optional[str] = None
    record_updated_by: Optional[str] = None
    brochure_name: Optional[str] = None
    url_brochure: Optional[Dict[str, Any]] = None
    notion_brochure_key: Optional[Dict[str, Any]] = None
    photo_name: Optional[str] = None
    url_photo: Optional[Dict[str, Any]] = None
    notion_photo_key: Optional[Dict[str, Any]] = None
    listing_price_brochure_name: Optional[str] = None
    url_listing_price_brochure: Optional[Dict[str, Any]] = None
    notion_listing_price_brochure_key: Optional[Dict[str, Any]] = None
    down_payment: Optional[Dict[str, Any]] = None
 

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Listing_Project':
        """Create a Listing_Project instance from a dictionary."""
        return cls(**data)

    def to_dict(self) -> Dict[str, Any]:
        """Convert the Listing_Project instance to a dictionary."""
        return {k: v for k, v in self.__dict__.items() if v is not None}
    
# Schema paths
LISTING_PROJECT_SCHEMA_PATH = 'schemas/listing_projects.json'

# Table names
LISTING_PROJECT_TABLE_NAME = 'raw_listings_projects'