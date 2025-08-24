"""
Dependency injection for FastAPI
"""
from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession
import redis.asyncio as redis

from app.core.database import get_cimco_db, get_postgres_db, get_redis


# Database dependencies - these are the actual dependency functions
get_cimco_session = Depends(get_cimco_db)
get_postgres_session = Depends(get_postgres_db)
get_redis_client = Depends(get_redis)