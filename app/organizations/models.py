from dataclasses import dataclass
from datetime import datetime
from typing import Optional, List, Dict, Any

@dataclass
class Organization:
    """Organization data model."""
    organization_id: int
    legal_name: str
    commercial_name: Optional[str] = None
    chatbot_enabled_default: Optional[bool] = None
    contractor_status: Optional[str] = None
    record_created_at: Optional[datetime] = None
    record_updated_at: Optional[datetime] = None
    record_created_by: Optional[str] = None
    updated_by: Optional[str] = None
    contractor: Optional[str] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Organization':
        """Create a Organization instance from a dictionary."""
        return cls(**data)

    def to_dict(self) -> Dict[str, Any]:
        """Convert the Organization instance to a dictionary."""
        return {k: v for k, v in self.__dict__.items() if v is not None}
    
# Schema paths
ORGANIZATION_SCHEMA_PATH = 'schemas/organizations.json'

# Table names
ORGANIZATION_TABLE_NAME = 'raw_organizations'