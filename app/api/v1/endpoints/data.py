"""
Data management endpoints
"""
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from typing import Optional

from app.core.database import get_cimco_db, get_postgres_db
from app.services.cimco_service import CimcoService
from app.services.database_service import DatabaseService
from app.services.schema_discovery_service import SchemaDiscoveryService
from app.services.sync_service import SyncService

router = APIRouter()


@router.get("/cimco/test-connection")
async def test_cimco_connection(
    cimco_db: AsyncSession = Depends(get_cimco_db)
):
    """Test connection to CIMCO database"""
    service = CimcoService(cimco_db)
    result = await service.test_connection()
    
    if result["status"] == "failed":
        raise HTTPException(status_code=500, detail=result)
    
    return result


@router.get("/cimco/info")
async def get_cimco_database_info(
    cimco_db: AsyncSession = Depends(get_cimco_db)
):
    """Get CIMCO database information"""
    service = CimcoService(cimco_db)
    result = await service.get_database_info()
    
    if result["status"] == "error":
        raise HTTPException(status_code=500, detail=result)
    
    return result


@router.get("/cimco/tables")
async def list_cimco_tables(
    cimco_db: AsyncSession = Depends(get_cimco_db)
):
    """List all tables in CIMCO database"""
    service = CimcoService(cimco_db)
    result = await service.list_tables()
    
    if result["status"] == "error":
        raise HTTPException(status_code=500, detail=result)
    
    return result


@router.get("/cimco/tables/{table_name}/schema")
async def get_cimco_table_schema(
    table_name: str,
    cimco_db: AsyncSession = Depends(get_cimco_db)
):
    """Get schema for a specific CIMCO table"""
    service = CimcoService(cimco_db)
    result = await service.get_table_schema(table_name)
    
    if result["status"] == "error":
        raise HTTPException(status_code=404, detail=result)
    
    return result


@router.get("/cimco/tables/{table_name}/sample")
async def get_cimco_table_sample(
    table_name: str,
    limit: int = 5,
    cimco_db: AsyncSession = Depends(get_cimco_db)
):
    """Get sample data from a CIMCO table"""
    if limit > 100:
        raise HTTPException(status_code=400, detail="Limit cannot exceed 100 rows")
    
    service = CimcoService(cimco_db)
    result = await service.get_table_sample(table_name, limit)
    
    if result["status"] == "error":
        raise HTTPException(status_code=404, detail=result)
    
    return result


@router.get("/sync-status")
async def get_sync_status(
    postgres_db: AsyncSession = Depends(get_postgres_db)
):
    """Get data synchronization status"""
    # Create service with only postgres (CIMCO not needed for status)
    sync_service = SyncService(None, postgres_db)
    result = await sync_service.get_sync_status()
    
    if result["status"] == "error":
        raise HTTPException(status_code=500, detail=result)
    
    return result


@router.get("/joblog")
async def get_joblog_data(
    limit: int = 100,
    machine: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    cimco_db: AsyncSession = Depends(get_cimco_db)
):
    """Get job log data with optional filtering"""
    if limit > 1000:
        raise HTTPException(status_code=400, detail="Limit cannot exceed 1000 rows")
    
    service = CimcoService(cimco_db)
    result = await service.get_joblog_data(limit, machine, start_date, end_date)
    
    if result["status"] == "error":
        raise HTTPException(status_code=500, detail=result)
    
    return result


@router.get("/machines")
async def get_machine_list(
    cimco_db: AsyncSession = Depends(get_cimco_db)
):
    """Get list of all machines"""
    service = CimcoService(cimco_db)
    result = await service.get_machine_list()
    
    if result["status"] == "error":
        raise HTTPException(status_code=500, detail=result)
    
    return result


@router.get("/joblog/summary")
async def get_joblog_summary(
    machine: Optional[str] = None,
    cimco_db: AsyncSession = Depends(get_cimco_db)
):
    """Get summary statistics for job log data"""
    service = CimcoService(cimco_db)
    result = await service.get_joblog_summary(machine)
    
    if result["status"] == "error":
        raise HTTPException(status_code=500, detail=result)
    
    return result


@router.post("/sync/machines")
async def sync_machines(
    cimco_db: AsyncSession = Depends(get_cimco_db),
    postgres_db: AsyncSession = Depends(get_postgres_db)
):
    """Synchronize machine list from CIMCO to analytics database"""
    sync_service = SyncService(cimco_db, postgres_db)
    result = await sync_service.sync_machines()
    
    if result["status"] == "error":
        raise HTTPException(status_code=500, detail=result)
    
    return result


@router.post("/sync/operators")
async def sync_operators(
    cimco_db: AsyncSession = Depends(get_cimco_db),
    postgres_db: AsyncSession = Depends(get_postgres_db)
):
    """Synchronize operators from CIMCO to analytics database"""
    sync_service = SyncService(cimco_db, postgres_db)
    result = await sync_service.sync_operators()
    
    if result["status"] == "error":
        raise HTTPException(status_code=500, detail=result)
    
    return result


@router.post("/sync/jobs")
async def sync_job_records(
    limit: int = 1000,
    machine_id: Optional[str] = None,
    incremental: bool = True,
    start_date: Optional[str] = None,
    cimco_db: AsyncSession = Depends(get_cimco_db),
    postgres_db: AsyncSession = Depends(get_postgres_db)
):
    """Synchronize job records from CIMCO to analytics database"""
    if limit > 5000:
        raise HTTPException(status_code=400, detail="Limit cannot exceed 5000 records")
    
    sync_service = SyncService(cimco_db, postgres_db)
    result = await sync_service.sync_jobs(limit, machine_id, incremental, start_date)
    
    if result["status"] == "error":
        raise HTTPException(status_code=500, detail=result)
    
    return result


@router.post("/sync/all")
async def trigger_full_sync(
    job_limit: int = 1000,
    cimco_db: AsyncSession = Depends(get_cimco_db),
    postgres_db: AsyncSession = Depends(get_postgres_db)
):
    """Trigger comprehensive data synchronization (machines + operators + jobs)"""
    if job_limit > 5000:
        raise HTTPException(status_code=400, detail="Job limit cannot exceed 5000 records")
    
    sync_service = SyncService(cimco_db, postgres_db)
    result = await sync_service.sync_all(job_limit)
    
    if result["status"] == "error":
        raise HTTPException(status_code=500, detail=result)
    
    return result


@router.get("/sync/schema-mapping")
async def get_sync_schema_mapping(
    cimco_db: AsyncSession = Depends(get_cimco_db),
    postgres_db: AsyncSession = Depends(get_postgres_db)
):
    """Get schema mapping for synchronization"""
    sync_service = SyncService(cimco_db, postgres_db)
    result = await sync_service.discover_and_map_schema()
    
    if result["status"] == "error":
        raise HTTPException(status_code=500, detail=result)
    
    return result

@router.get("/cimco/schema/discover")
async def discover_cimco_schema(cimco_db: AsyncSession = Depends(get_cimco_db)):
    """Discover all tables in CIMCO database with detailed analysis"""
    service = SchemaDiscoveryService()
    result = await service.discover_tables(cimco_db)
    if result["status"] == "error":
        raise HTTPException(status_code=500, detail=result)
    return result


@router.get("/cimco/schema/table/{table_name}")
async def analyze_table_structure(
    table_name: str,
    cimco_db: AsyncSession = Depends(get_cimco_db)
):
    """Analyze detailed structure of a specific CIMCO table"""
    service = SchemaDiscoveryService()
    result = await service.analyze_table_structure(cimco_db, table_name)
    if result["status"] == "error":
        raise HTTPException(status_code=500, detail=result)
    return result


@router.get("/cimco/schema/sample/{table_name}")
async def get_table_sample_data(
    table_name: str,
    limit: int = 5,
    cimco_db: AsyncSession = Depends(get_cimco_db)
):
    """Get sample data from a CIMCO table for schema analysis"""
    if limit > 20:
        limit = 20  # Safety limit for schema discovery
    service = SchemaDiscoveryService()
    result = await service.get_sample_data(cimco_db, table_name, limit)
    if result["status"] == "error":
        raise HTTPException(status_code=500, detail=result)
    return result


@router.get("/cimco/schema/mapping")
async def get_schema_mapping(cimco_db: AsyncSession = Depends(get_cimco_db)):
    """Get suggested mapping from CIMCO schema to analytics models"""
    service = SchemaDiscoveryService()
    result = await service.map_to_analytics_schema(cimco_db)
    if result["status"] == "error":
        raise HTTPException(status_code=500, detail=result)
    return result


@router.get("/cimco/analysis/job-patterns")
async def analyze_job_patterns(cimco_db: AsyncSession = Depends(get_cimco_db)):
    """Analyze job data patterns for ML feature engineering"""
    service = SchemaDiscoveryService()
    result = await service.analyze_job_data_patterns(cimco_db)
    if result["status"] == "error":
        raise HTTPException(status_code=500, detail=result)
    return result


@router.get("/postgres/test-connection")
async def test_postgres_connection(
    postgres_db: AsyncSession = Depends(get_postgres_db)
):
    """Test connection to PostgreSQL analytics database"""
    service = DatabaseService()
    result = await service.test_postgres_connection(postgres_db)
    if result["status"] == "failed":
        raise HTTPException(status_code=500, detail=result)
    return result


@router.get("/postgres/tables/check")
async def check_analytics_tables(
    postgres_db: AsyncSession = Depends(get_postgres_db)
):
    """Check if analytics tables exist"""
    service = DatabaseService()
    result = await service.check_tables(postgres_db)
    if result["status"] == "error":
        raise HTTPException(status_code=500, detail=result)
    return result


@router.post("/postgres/tables/create")
async def create_analytics_tables(
    postgres_db: AsyncSession = Depends(get_postgres_db)
):
    """Create analytics database tables"""
    service = DatabaseService()
    result = await service.create_tables(postgres_db)
    if result["status"] == "error":
        raise HTTPException(status_code=500, detail=result)
    return result


@router.get("/postgres/tables/counts")
async def get_table_counts(
    postgres_db: AsyncSession = Depends(get_postgres_db)
):
    """Get record counts for analytics tables"""
    service = DatabaseService()
    result = await service.get_table_counts(postgres_db)
    if result["status"] == "error":
        raise HTTPException(status_code=500, detail=result)
    return result