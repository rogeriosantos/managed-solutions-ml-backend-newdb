"""
CIMCO database schema discovery service
"""
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text, MetaData, Table
from typing import Dict, List, Any, Optional
import logging
from app.core.database import get_cimco_db

logger = logging.getLogger(__name__)

class SchemaDiscoveryService:
    """Service for discovering and mapping CIMCO database schema"""
    
    def __init__(self):
        self.metadata = MetaData()
    
    async def discover_tables(self, db: AsyncSession) -> Dict[str, Any]:
        """Discover all tables in CIMCO database"""
        try:
            # Get all table names
            result = await db.execute(text("""
                SELECT TABLE_NAME, TABLE_COMMENT, TABLE_ROWS, DATA_LENGTH
                FROM information_schema.TABLES 
                WHERE TABLE_SCHEMA = DATABASE()
                ORDER BY TABLE_NAME
            """))
            
            tables = []
            for row in result:
                tables.append({
                    "name": row[0],
                    "comment": row[1] or "",
                    "estimated_rows": row[2] or 0,
                    "data_length": row[3] or 0
                })
            
            return {
                "status": "success",
                "table_count": len(tables),
                "tables": tables
            }
            
        except Exception as e:
            logger.error(f"Failed to discover tables: {str(e)}")
            return {
                "status": "error",
                "error": str(e)
            }
    
    async def analyze_table_structure(self, db: AsyncSession, table_name: str) -> Dict[str, Any]:
        """Analyze structure of a specific table"""
        try:
            # Get column information
            result = await db.execute(text("""
                SELECT 
                    COLUMN_NAME,
                    DATA_TYPE,
                    IS_NULLABLE,
                    COLUMN_DEFAULT,
                    COLUMN_KEY,
                    EXTRA,
                    COLUMN_COMMENT,
                    CHARACTER_MAXIMUM_LENGTH,
                    NUMERIC_PRECISION,
                    NUMERIC_SCALE
                FROM information_schema.COLUMNS 
                WHERE TABLE_SCHEMA = DATABASE() 
                AND TABLE_NAME = :table_name
                ORDER BY ORDINAL_POSITION
            """), {"table_name": table_name})
            
            columns = []
            for row in result:
                columns.append({
                    "name": row[0],
                    "data_type": row[1],
                    "nullable": row[2] == "YES",
                    "default": row[3],
                    "key": row[4],
                    "extra": row[5],
                    "comment": row[6] or "",
                    "max_length": row[7],
                    "precision": row[8],
                    "scale": row[9]
                })
            
            # Get indexes
            index_result = await db.execute(text("""
                SELECT 
                    INDEX_NAME,
                    COLUMN_NAME,
                    NON_UNIQUE,
                    SEQ_IN_INDEX
                FROM information_schema.STATISTICS 
                WHERE TABLE_SCHEMA = DATABASE() 
                AND TABLE_NAME = :table_name
                ORDER BY INDEX_NAME, SEQ_IN_INDEX
            """), {"table_name": table_name})
            
            indexes = {}
            for row in index_result:
                index_name = row[0]
                if index_name not in indexes:
                    indexes[index_name] = {
                        "unique": row[2] == 0,
                        "columns": []
                    }
                indexes[index_name]["columns"].append(row[1])
            
            return {
                "status": "success",
                "table_name": table_name,
                "column_count": len(columns),
                "columns": columns,
                "indexes": indexes
            }
            
        except Exception as e:
            logger.error(f"Failed to analyze table {table_name}: {str(e)}")
            return {
                "status": "error",
                "error": str(e)
            }
    
    async def get_sample_data(self, db: AsyncSession, table_name: str, limit: int = 5) -> Dict[str, Any]:
        """Get sample data from a table"""
        try:
            # Get sample rows
            result = await db.execute(text(f"""
                SELECT * FROM {table_name} 
                ORDER BY RAND() 
                LIMIT :limit
            """), {"limit": limit})
            
            # Get column names
            columns = list(result.keys())
            
            # Convert rows to list of dictionaries
            rows = []
            for row in result:
                row_dict = {}
                for i, value in enumerate(row):
                    # Convert datetime and other types to string for JSON serialization
                    if hasattr(value, 'isoformat'):
                        row_dict[columns[i]] = value.isoformat()
                    else:
                        row_dict[columns[i]] = value
                rows.append(row_dict)
            
            return {
                "status": "success",
                "table_name": table_name,
                "columns": columns,
                "sample_rows": rows,
                "row_count": len(rows)
            }
            
        except Exception as e:
            logger.error(f"Failed to get sample data from {table_name}: {str(e)}")
            return {
                "status": "error",
                "error": str(e)
            }
    
    async def analyze_job_data_patterns(self, db: AsyncSession) -> Dict[str, Any]:
        """Analyze patterns in job data for ML feature engineering"""
        try:
            analyses = {}
            
            # Check if jobs table exists and analyze it
            job_tables = ["joblog_ob", "joblog", "jobs", "job", "job_log"]
            job_table = None
            
            for table in job_tables:
                try:
                    await db.execute(text(f"SELECT 1 FROM {table} LIMIT 1"))
                    job_table = table
                    break
                except:
                    continue
            
            if not job_table:
                return {
                    "status": "error",
                    "error": "No job table found"
                }
            
            # Analyze job states
            result = await db.execute(text(f"""
                SELECT State, COUNT(*) as count
                FROM {job_table}
                GROUP BY State
                ORDER BY count DESC
            """))
            
            job_states = []
            for row in result:
                job_states.append({
                    "state": row[0],
                    "count": row[1]
                })
            
            analyses["job_states"] = job_states
            
            # Analyze time patterns
            result = await db.execute(text(f"""
                SELECT 
                    HOUR(StartTime) as hour,
                    COUNT(*) as job_count,
                    AVG(TIMESTAMPDIFF(SECOND, StartTime, EndTime)) as avg_duration
                FROM {job_table}
                WHERE StartTime IS NOT NULL AND EndTime IS NOT NULL
                GROUP BY HOUR(StartTime)
                ORDER BY hour
            """))
            
            hourly_patterns = []
            for row in result:
                hourly_patterns.append({
                    "hour": row[0],
                    "job_count": row[1],
                    "avg_duration_seconds": float(row[2]) if row[2] else 0
                })
            
            analyses["hourly_patterns"] = hourly_patterns
            
            # Analyze machine utilization
            result = await db.execute(text(f"""
                SELECT 
                    machine,
                    COUNT(*) as total_jobs,
                    COUNT(CASE WHEN State = 'CLOSED' THEN 1 END) as completed_jobs,
                    AVG(CASE WHEN State = 'CLOSED' THEN TIMESTAMPDIFF(SECOND, StartTime, EndTime) END) as avg_job_duration
                FROM {job_table}
                WHERE machine IS NOT NULL
                GROUP BY machine
                ORDER BY total_jobs DESC
                LIMIT 20
            """))
            
            machine_utilization = []
            for row in result:
                machine_utilization.append({
                    "machine_id": row[0],
                    "total_jobs": row[1],
                    "completed_jobs": row[2] or 0,
                    "completion_rate": (row[2] or 0) / row[1] if row[1] > 0 else 0,
                    "avg_duration_seconds": float(row[3]) if row[3] else 0
                })
            
            analyses["machine_utilization"] = machine_utilization
            
            return {
                "status": "success",
                "job_table": job_table,
                "analyses": analyses
            }
            
        except Exception as e:
            logger.error(f"Failed to analyze job patterns: {str(e)}")
            return {
                "status": "error",
                "error": str(e)
            }
    
    async def map_to_analytics_schema(self, db: AsyncSession) -> Dict[str, Any]:
        """Map CIMCO schema to our analytics models"""
        try:
            mapping = {
                "machines": {
                    "source_table": None,
                    "field_mapping": {},
                    "status": "not_found"
                },
                "operators": {
                    "source_table": None,
                    "field_mapping": {},
                    "status": "not_found"
                },
                "jobs": {
                    "source_table": None,
                    "field_mapping": {},
                    "status": "not_found"
                }
            }
            
            # Discover tables
            tables_result = await self.discover_tables(db)
            if tables_result["status"] != "success":
                return tables_result
            
            # Create mapping of lowercase to original names
            table_name_map = {t["name"].lower(): t["name"] for t in tables_result["tables"]}
            table_names = list(table_name_map.keys())
            
            # Map machines
            machine_candidates = ["machines", "machine", "equipment"]
            for candidate in machine_candidates:
                if candidate in table_names:
                    mapping["machines"]["source_table"] = table_name_map[candidate]
                    mapping["machines"]["status"] = "found"
                    break
            
            # Map operators  
            operator_candidates = ["operators", "operator", "employees", "users"]
            for candidate in operator_candidates:
                if candidate in table_names:
                    mapping["operators"]["source_table"] = table_name_map[candidate]
                    mapping["operators"]["status"] = "found"
                    break
            
            # Map jobs
            job_candidates = ["joblog_ob", "joblog", "jobs", "job", "job_log"]
            for candidate in job_candidates:
                if candidate in table_names:
                    mapping["jobs"]["source_table"] = table_name_map[candidate]
                    mapping["jobs"]["status"] = "found"
                    break
            
            # Analyze field mappings for found tables
            for entity, config in mapping.items():
                if config["status"] == "found":
                    table_analysis = await self.analyze_table_structure(db, config["source_table"])
                    if table_analysis["status"] == "success":
                        config["columns"] = table_analysis["columns"]
                        config["field_mapping"] = self._suggest_field_mapping(entity, table_analysis["columns"])
            
            return {
                "status": "success",
                "mapping": mapping,
                "available_tables": table_names
            }
            
        except Exception as e:
            logger.error(f"Failed to map schema: {str(e)}")
            return {
                "status": "error",
                "error": str(e)
            }
    
    def _suggest_field_mapping(self, entity: str, columns: List[Dict]) -> Dict[str, str]:
        """Suggest field mappings based on column names"""
        mapping = {}
        column_names = [col["name"].lower() for col in columns]
        
        if entity == "machines":
            # Common machine field mappings
            field_suggestions = {
                "machine_id": ["machineid", "machine_id", "id", "machine_number"],
                "name": ["name", "machine_name", "description"],
                "type": ["type", "machine_type", "category"],
                "location": ["location", "area", "department"]
            }
        elif entity == "operators":
            # Common operator field mappings
            field_suggestions = {
                "emp_id": ["empid", "emp_id", "employee_id", "id"],
                "operator_name": ["operatorname", "operator_name", "name", "full_name"],
                "op_number": ["opnumber", "op_number", "operator_number"]
            }
        elif entity == "jobs":
            # Common job field mappings
            field_suggestions = {
                "job_number": ["jobnumber", "job_number", "id", "job_id"],
                "machine_id": ["machineid", "machine_id"],
                "part_number": ["partnumber", "part_number", "part"],
                "state": ["state", "status", "job_state"],
                "start_time": ["starttime", "start_time", "begin_time"],
                "end_time": ["endtime", "end_time", "finish_time"],
                "operator_name": ["operatorname", "operator_name", "operator"],
                "running_time": ["runningtime", "running_time", "run_time"],
                "setup_time": ["setuptime", "setup_time"],
                "idle_time": ["idletime", "idle_time"]
            }
        else:
            return mapping
        
        # Find best matches
        for target_field, candidates in field_suggestions.items():
            for candidate in candidates:
                if candidate in column_names:
                    mapping[target_field] = candidate
                    break
        
        return mapping