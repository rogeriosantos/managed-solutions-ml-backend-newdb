"""
CIMCO Database Service
Handles connection and data retrieval from CIMCO MySQL database
"""
from typing import List, Dict, Any, Optional
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
import logging

logger = logging.getLogger(__name__)


class CimcoService:
    """Service for interacting with CIMCO MySQL database"""
    
    def __init__(self, db_session: AsyncSession):
        self.db = db_session
    
    async def test_connection(self) -> Dict[str, Any]:
        """Test connection to CIMCO database"""
        try:
            result = await self.db.execute(text("SELECT 1 as test"))
            row = result.fetchone()
            return {
                "status": "connected",
                "test_query": row[0] if row else None,
                "message": "Successfully connected to CIMCO database"
            }
        except Exception as e:
            logger.error(f"CIMCO database connection failed: {str(e)}")
            return {
                "status": "failed",
                "error": str(e),
                "message": "Failed to connect to CIMCO database"
            }
    
    async def get_database_info(self) -> Dict[str, Any]:
        """Get basic database information"""
        try:
            # Get database version
            version_result = await self.db.execute(text("SELECT VERSION() as version"))
            version_row = version_result.fetchone()
            
            # Get current database name
            db_result = await self.db.execute(text("SELECT DATABASE() as db_name"))
            db_row = db_result.fetchone()
            
            return {
                "status": "success",
                "database_name": db_row[0] if db_row else None,
                "mysql_version": version_row[0] if version_row else None,
                "connection_info": "CIMCO MySQL on Railway"
            }
        except Exception as e:
            logger.error(f"Failed to get database info: {str(e)}")
            return {
                "status": "error",
                "error": str(e)
            }
    
    async def list_tables(self) -> Dict[str, Any]:
        """List all tables in the CIMCO database"""
        try:
            result = await self.db.execute(text("SHOW TABLES"))
            tables = [row[0] for row in result.fetchall()]
            
            return {
                "status": "success",
                "tables": tables,
                "table_count": len(tables)
            }
        except Exception as e:
            logger.error(f"Failed to list tables: {str(e)}")
            return {
                "status": "error",
                "error": str(e)
            }
    
    async def get_table_schema(self, table_name: str) -> Dict[str, Any]:
        """Get schema information for a specific table"""
        try:
            result = await self.db.execute(text(f"DESCRIBE {table_name}"))
            columns = []
            for row in result.fetchall():
                columns.append({
                    "field": row[0],
                    "type": row[1],
                    "null": row[2],
                    "key": row[3],
                    "default": row[4],
                    "extra": row[5]
                })
            
            return {
                "status": "success",
                "table_name": table_name,
                "columns": columns,
                "column_count": len(columns)
            }
        except Exception as e:
            logger.error(f"Failed to get table schema for {table_name}: {str(e)}")
            return {
                "status": "error",
                "table_name": table_name,
                "error": str(e)
            }
    
    async def get_table_sample(self, table_name: str, limit: int = 5) -> Dict[str, Any]:
        """Get sample data from a table"""
        try:
            result = await self.db.execute(text(f"SELECT * FROM {table_name} LIMIT {limit}"))
            rows = result.fetchall()
            columns = result.keys()
            
            # Convert rows to list of dictionaries
            data = []
            for row in rows:
                data.append(dict(zip(columns, row)))
            
            return {
                "status": "success",
                "table_name": table_name,
                "sample_data": data,
                "row_count": len(data)
            }
        except Exception as e:
            logger.error(f"Failed to get sample data from {table_name}: {str(e)}")
            return {
                "status": "error",
                "table_name": table_name,
                "error": str(e)
            }
    
    async def get_joblog_data(self, limit: int = 100, machine: Optional[str] = None, 
                             start_date: Optional[str] = None, end_date: Optional[str] = None) -> Dict[str, Any]:
        """Get job log data with optional filtering"""
        try:
            query = "SELECT * FROM joblog_ob WHERE 1=1"
            params = {}
            
            if machine:
                query += " AND machine = :machine"
                params["machine"] = machine
            
            if start_date:
                query += " AND StartTime >= :start_date"
                params["start_date"] = start_date
            
            if end_date:
                query += " AND StartTime <= :end_date"
                params["end_date"] = end_date
            
            query += " ORDER BY StartTime DESC LIMIT :limit"
            params["limit"] = limit
            
            result = await self.db.execute(text(query), params)
            rows = result.fetchall()
            columns = result.keys()
            
            # Convert rows to list of dictionaries
            data = []
            for row in rows:
                row_dict = dict(zip(columns, row))
                # Handle datetime conversion for display
                if row_dict.get('StartTime'):
                    row_dict['StartTime'] = row_dict['StartTime'].isoformat() if row_dict['StartTime'] else None
                if row_dict.get('EndTime'):
                    row_dict['EndTime'] = row_dict['EndTime'].isoformat() if row_dict['EndTime'] else None
                data.append(row_dict)
            
            return {
                "status": "success",
                "data": data,
                "row_count": len(data),
                "filters": {
                    "machine": machine,
                    "start_date": start_date,
                    "end_date": end_date,
                    "limit": limit
                }
            }
        except Exception as e:
            logger.error(f"Failed to get joblog data: {str(e)}")
            return {
                "status": "error",
                "error": str(e)
            }
    
    async def get_machine_list(self) -> Dict[str, Any]:
        """Get list of unique machines from joblog_ob"""
        try:
            result = await self.db.execute(text("SELECT DISTINCT machine FROM joblog_ob ORDER BY machine"))
            machines = [row[0] for row in result.fetchall()]
            
            return {
                "status": "success",
                "machines": machines,
                "machine_count": len(machines)
            }
        except Exception as e:
            logger.error(f"Failed to get machine list: {str(e)}")
            return {
                "status": "error",
                "error": str(e)
            }
    
    async def get_joblog_summary(self, machine: Optional[str] = None) -> Dict[str, Any]:
        """Get summary statistics for job log data"""
        try:
            base_query = "SELECT COUNT(*) as total_jobs, MIN(StartTime) as earliest_job, MAX(StartTime) as latest_job, SUM(PartsProduced) as total_parts FROM joblog_ob"
            
            if machine:
                base_query += " WHERE machine = :machine"
                params = {"machine": machine}
            else:
                params = {}
            
            result = await self.db.execute(text(base_query), params)
            summary_row = result.fetchone()
            
            # Get job state counts
            state_query = "SELECT State, COUNT(*) as count FROM joblog_ob"
            if machine:
                state_query += " WHERE machine = :machine"
            state_query += " GROUP BY State"
            
            state_result = await self.db.execute(text(state_query), params)
            job_states = {row[0]: row[1] for row in state_result.fetchall()}
            
            # Get average times
            avg_query = "SELECT AVG(JobDuration) as avg_duration, AVG(SetupTime) as avg_setup FROM joblog_ob"
            if machine:
                avg_query += " WHERE machine = :machine"
            
            avg_result = await self.db.execute(text(avg_query), params)
            avg_row = avg_result.fetchone()
            
            return {
                "status": "success",
                "summary": {
                    "total_jobs": summary_row[0] if summary_row else 0,
                    "earliest_job": summary_row[1].isoformat() if summary_row and summary_row[1] else None,
                    "latest_job": summary_row[2].isoformat() if summary_row and summary_row[2] else None,
                    "total_parts_produced": summary_row[3] if summary_row else 0,
                    "job_states": job_states,
                    "average_job_duration": float(avg_row[0]) if avg_row and avg_row[0] else 0,
                    "average_setup_time": float(avg_row[1]) if avg_row and avg_row[1] else 0,
                    "machine_filter": machine
                }
            }
        except Exception as e:
            logger.error(f"Failed to get joblog summary: {str(e)}")
            return {
                "status": "error",
                "error": str(e)
            }