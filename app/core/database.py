"""
Database connection and session management
"""
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.orm import declarative_base
import redis.asyncio as redis
from typing import AsyncGenerator

from app.core.config import settings

# SQLAlchemy Base
Base = declarative_base()

# Database engines
cimco_engine = None
postgres_engine = None
redis_client = None

# Session makers
CimcoSessionLocal = None
PostgresSessionLocal = None


async def init_databases():
    """Initialize database connections"""
    global cimco_engine, postgres_engine, redis_client
    global CimcoSessionLocal, PostgresSessionLocal
    
    # CIMCO MySQL Engine
    cimco_engine = create_async_engine(
        settings.CIMCO_DATABASE_URL,
        echo=False,
        pool_pre_ping=True,
        pool_recycle=300,
    )
    CimcoSessionLocal = async_sessionmaker(
        cimco_engine, 
        class_=AsyncSession, 
        expire_on_commit=False
    )
    
    # PostgreSQL Engine
    postgres_engine = create_async_engine(
        settings.POSTGRES_DATABASE_URL,
        echo=False,
        pool_pre_ping=True,
        pool_recycle=300,
    )
    PostgresSessionLocal = async_sessionmaker(
        postgres_engine, 
        class_=AsyncSession, 
        expire_on_commit=False
    )
    
    # Redis Client
    redis_client = redis.from_url(
        settings.REDIS_URL,
        encoding="utf-8",
        decode_responses=True
    )


async def get_cimco_db() -> AsyncGenerator[AsyncSession, None]:
    """Get CIMCO database session"""
    async with CimcoSessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()


async def get_postgres_db() -> AsyncGenerator[AsyncSession, None]:
    """Get PostgreSQL database session"""
    async with PostgresSessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()


async def get_redis() -> redis.Redis:
    """Get Redis client"""
    return redis_client


async def close_databases():
    """Close database connections"""
    if cimco_engine:
        await cimco_engine.dispose()
    if postgres_engine:
        await postgres_engine.dispose()
    if redis_client:
        await redis_client.close()