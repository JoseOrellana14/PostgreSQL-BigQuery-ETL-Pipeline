from dataclasses import dataclass
from datetime import datetime
from typing import Optional, List, Dict, Any

@dataclass
class Appointment:
    """Appointment data model."""
    appointment_id: int
    organization_id: int
    user_id: int
    buyer_lead_id: int
    seller_lead_id: int
    property_opportunity_id: int
    property_unit_id: int
    scheduled_start_at: Optional[datetime] = None
    scheduled_end_at: Optional[datetime] = None
    status: Optional[str] = None
    channel: Optional[str] = None
    created_source: Optional[str] = None
    cancel_reason: Optional[str] = None
    notes: Optional[str] = None
    state: Optional[int] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    created_by: Optional[str] = None
    updated_by: Optional[str] = None
    

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Appointment':
        """Create a Appointment instance from a dictionary."""
        return cls(**data)

    def to_dict(self) -> Dict[str, Any]:
        """Convert the Appointment instance to a dictionary."""
        return {k: v for k, v in self.__dict__.items() if v is not None}
    
# Schema paths
APPOINTMENT_SCHEMA_PATH = 'schemas/appointments.json'

# Table names
APPOINTMENT_TABLE_NAME = 'appointments'