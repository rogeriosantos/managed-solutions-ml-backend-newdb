"""
Base Pydantic schemas
"""
from pydantic import BaseModel
from datetime import datetime
from typing import Optional


class BaseSchema(BaseModel):
    """Base schema with common fields"""
    
    class Config:
        orm_mode = True
        allow_population_by_field_name = True


class TimestampMixin(BaseModel):
    """Mixin for timestamp fields"""
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None