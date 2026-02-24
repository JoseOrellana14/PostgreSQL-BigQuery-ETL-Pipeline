from dataclasses import dataclass
from datetime import datetime
from typing import Optional, List, Dict, Any

@dataclass
class Appointment:
    """Appointment data model."""
    appointment_id: int
    user_id: int
    listing_id: Optional[int] = None
    lead_id: Optional[int] = None
    opportunity_id: Optional[int] = None
    gcal_event_id: Optional[str] = None
    start_at: Optional[datetime] = None
    end_at: Optional[datetime] = None
    modality: Optional[str] = None
    showup: Optional[bool] = None
    record_status: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    created_by: Optional[str] = None
    updated_by: Optional[str] = None
    status: Optional[str] = None
    remind_at: Optional[datetime] = None
    reminder_sent_at: Optional[datetime] = None
    summary: Optional[str] = None
    post_followup_at: Optional[datetime] = None
    post_followup_sent_at: Optional[datetime] = None
    

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
APPOINTMENT_TABLE_NAME = 'raw_appointments'