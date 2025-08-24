"""
Database management service
"""
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from typing import Dict, Any
import logging

from app.models.analytics import Base

logger = logging.getLogger(__name__)


class DatabaseService:
    """Service for database management operations"""
    
    def __init__(self, db_session: AsyncSession = None):
        self.db_session = db_session
    
    async def create_tables(self, db_session: AsyncSession = None) -> Dict[str, Any]:
        """Create all analytics tables"""
        session = db_session or self.db_session
        if not session:
            return {
                "status": "error",
                "error": "No database session provided"
            }
        
        try:
            # Use the session's bind (engine) to create tables
            async with session.bind.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
            
            return {
                "status": "success",
                "message": "Analytics tables created successfully"
            }
        except Exception as e:
            logger.error(f"Failed to create tables: {str(e)}")
            return {
                "status": "error",
                "error": str(e)
            }
    
    async def check_tables(self, db_session: AsyncSession = None) -> Dict[str, Any]:
        """Check if analytics tables exist"""
        session = db_session or self.db_session
        if not session:
            return {
                "status": "error",
                "error": "No database session provided"
            }
        
        try:
            # Check if main tables exist
            tables_to_check = [
                'machines', 'operators', 'jobs', 'downtime_records',
                'predictions', 'model_metadata', 'analytics_reports', 'sync_logs'
            ]
            
            existing_tables = []
            missing_tables = []
            
            for table in tables_to_check:
                result = await session.execute(text(
                    "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = :table_name)"
                ), {"table_name": table})
                exists = result.scalar()
                
                if exists:
                    existing_tables.append(table)
                else:
                    missing_tables.append(table)
            
            return {
                "status": "success",
                "existing_tables": existing_tables,
                "missing_tables": missing_tables,
                "all_tables_exist": len(missing_tables) == 0
            }
        except Exception as e:
            logger.error(f"Failed to check tables: {str(e)}")
            return {
                "status": "error",
                "error": str(e)
            }
    
    async def get_table_counts(self, db_session: AsyncSession = None) -> Dict[str, Any]:
        """Get record counts for all analytics tables"""
        session = db_session or self.db_session
        if not session:
            return {
                "status": "error",
                "error": "No database session provided"
            }
        
        try:
            tables = ['machines', 'operators', 'jobs', 'downtime_records', 
                     'predictions', 'model_metadata', 'analytics_reports', 'sync_logs']
            
            counts = {}
            for table in tables:
                try:
                    result = await session.execute(text(f"SELECT COUNT(*) FROM {table}"))
                    counts[table] = result.scalar()
                except Exception as e:
                    counts[table] = f"Error: {str(e)}"
            
            return {
                "status": "success",
                "table_counts": counts
            }
        except Exception as e:
            logger.error(f"Failed to get table counts: {str(e)}")
            return {
                "status": "error",
                "error": str(e)
            }
    
    async def test_postgres_connection(self, db_session: AsyncSession = None) -> Dict[str, Any]:
        """Test PostgreSQL connection"""
        session = db_session or self.db_session
        if not session:
            return {
                "status": "error",
                "error": "No database session provided"
            }
        
        try:
            result = await session.execute(text("SELECT version()"))
            version = result.scalar()
            
            return {
                "status": "connected",
                "postgres_version": version,
                "message": "Successfully connected to PostgreSQL analytics database"
            }
        except Exception as e:
            logger.error(f"PostgreSQL connection failed: {str(e)}")
            return {
                "status": "failed",
                "error": str(e),
                "message": "Failed to connect to PostgreSQL analytics database"
            }