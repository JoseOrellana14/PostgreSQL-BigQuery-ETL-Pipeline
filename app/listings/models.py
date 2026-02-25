from dataclasses import dataclass
from datetime import datetime
from typing import Optional, List, Dict, Any

@dataclass
class Listing:
    """Listing data model."""
    listing_id: int
    user_id: int
    listing_project_id: Optional[int] = None
    operation_type: Optional[str] = None
    property_type: Optional[str] = None
    property_regime: Optional[str] = None
    neighborhood: Optional[str] = None
    neighborhood_macro: Optional[str] = None
    bedrooms: Optional[int] = None
    bathrooms: Optional[int] = None
    area_sqm: Optional[float] = None
    amenities: Optional[Dict[str, Any]] = None
    price: Optional[float] = None
    target_currency: Optional[str] = None
    price_sqm: Optional[float] = None
    qualitative_description: Optional[str] = None
    record_status: Optional[int] = None
    created_at: Optional[datetime] = None
    record_deleted_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    record_created_by: Optional[str] = None
    record_updated_by: Optional[str] = None
    name: Optional[str] = None
    collection_method: Optional[str] = None
    promoted_opportunity_id: Optional[int] = None
    estado: Optional[str] = None
    floor_level: Optional[int] = None
    floor_level_tag: Optional[str] = None
    floors_count: Optional[int] = None
    address: Optional[str] = None
    visibility: Optional[str] = None
    location_reference_raw: Optional[str] = None
    main_road: Optional[str] = None
    intersecting_road: Optional[str] = None
    anillo_min: Optional[int] = None
    anillo_max: Optional[int] = None
    lat: Optional[float] = None
    lng: Optional[float] = None
    google_maps_link: Optional[str] = None
    owner_name: Optional[str] = None
    personal_notes: Optional[str] = None
    brochure_name: Optional[str] = None
    url_brochure: Optional[Dict[str, Any]] = None
    notion_brochure_key: Optional[Dict[str, Any]] = None
    photo_name: Optional[str] = None
    url_photo: Optional[Dict[str, Any]] = None
    notion_photo_key: Optional[Dict[str, Any]] = None
    

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Listing':
        """Create a Listing instance from a dictionary."""
        return cls(**data)

    def to_dict(self) -> Dict[str, Any]:
        """Convert the Listing instance to a dictionary."""
        return {k: v for k, v in self.__dict__.items() if v is not None}
    
# Schema paths
LISTING_SCHEMA_PATH = 'schemas/listings.json'

# Table names
LISTING_TABLE_NAME = 'raw_listings'