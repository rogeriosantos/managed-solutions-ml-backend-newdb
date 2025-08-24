"""
Enhanced Data Synchronization Service
Handles comprehensive syncing from CIMCO MySQL to PostgreSQL analytics database
"""
from typing import Dict, Any, List, Optional, Tuple
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text, select, and_, or_, insert, update
from sqlalchemy.orm import selectinload
from datetime import datetime, timedelta
import logging
import asyncio
import uuid

from app.services.cimco_service import CimcoService
from app.services.schema_discovery_service import SchemaDiscoveryService
from app.models.analytics import Machine, Operator, JobRecord, DowntimeRecord, SyncLog
from app.utils.data_conversion import DataConverter

logger = logging.getLogger(__name__)


class SyncService:
    """Enhanced service for synchronizing CIMCO data to analytics database"""
    
    def __init__(self, cimco_db: AsyncSession, postgres_db: AsyncSession):
        self.cimco_db = cimco_db
        self.postgres_db = postgres_db
        self.cimco_service = CimcoService(cimco_db) if cimco_db else None
        self.schema_service = SchemaDiscoveryService()
        self.converter = DataConverter()
    
    async def _create_sync_log(self, sync_type: str, source_table: str) -> SyncLog:
        """Create a new sync log entry"""
        sync_log = SyncLog(
            sync_id=str(uuid.uuid4()),
            sync_type=sync_type,
            source_table=source_table,
            start_time=datetime.utcnow(),
            status='running'
        )
        self.postgres_db.add(sync_log)
        await self.postgres_db.flush()
        return sync_log
    
    async def _complete_sync_log(self, sync_log: SyncLog, 
                                processed: int, inserted: int, updated: int, failed: int = 0,
                                error_message: str = None) -> None:
        """Complete sync log with results"""
        sync_log.end_time = datetime.utcnow()
        sync_log.duration = (sync_log.end_time - sync_log.start_time).total_seconds()
        sync_log.records_processed = processed
        sync_log.records_inserted = inserted
        sync_log.records_updated = updated
        sync_log.records_failed = failed
        sync_log.status = 'failed' if error_message else 'completed'
        sync_log.error_message = error_message
        await self.postgres_db.commit()
    
    async def discover_and_map_schema(self) -> Dict[str, Any]:
        """Discover CIMCO schema and create mapping for sync"""
        try:
            if not self.cimco_db:
                return {"status": "error", "error": "CIMCO database not available"}
            
            mapping_result = await self.schema_service.map_to_analytics_schema(self.cimco_db)
            if mapping_result["status"] != "success":
                return mapping_result
            
            return {
                "status": "success",
                "schema_mapping": mapping_result["mapping"],
                "available_tables": mapping_result["available_tables"]
            }
        except Exception as e:
            logger.error(f"Schema discovery failed: {str(e)}")
            return {"status": "error", "error": str(e)}
    
    async def sync_machines(self, batch_size: int = 100) -> Dict[str, Any]:
        """Sync machines from CIMCO to analytics database"""
        sync_log = None
        try:
            if not self.cimco_db:
                return {"status": "error", "error": "CIMCO database not available"}
            
            sync_log = await self._create_sync_log("full", "machines")
            
            # Discover machine table mapping
            mapping_result = await self.schema_service.map_to_analytics_schema(self.cimco_db)
            if mapping_result["status"] != "success":
                raise Exception("Failed to discover schema mapping")
            
            machine_config = mapping_result["mapping"]["machines"]
            if machine_config["status"] != "found":
                # Fallback: get machines from job data
                machines = await self._extract_machines_from_jobs()
            else:
                # Get machines from dedicated table
                machines = await self._get_machines_from_table(machine_config)
            
            inserted, updated = 0, 0
            
            for machine_data in machines:
                try:
                    machine_id = self.converter.clean_machine_id(machine_data.get("machine_id"))
                    if not machine_id:
                        continue
                    
                    # Check if machine exists
                    existing = await self.postgres_db.execute(
                        select(Machine).where(Machine.machine_id == machine_id)
                    )
                    existing_machine = existing.scalar_one_or_none()
                    
                    if existing_machine:
                        # Update existing machine
                        existing_machine.name = machine_data.get("name", machine_id)
                        existing_machine.type = machine_data.get("type")
                        existing_machine.location = machine_data.get("location")
                        existing_machine.updated_at = datetime.utcnow()
                        updated += 1
                    else:
                        # Create new machine
                        new_machine = Machine(
                            machine_id=machine_id,
                            name=machine_data.get("name", machine_id),
                            type=machine_data.get("type"),
                            location=machine_data.get("location"),
                            is_active=True
                        )
                        self.postgres_db.add(new_machine)
                        inserted += 1
                        
                except Exception as e:
                    logger.error(f"Failed to process machine {machine_data}: {str(e)}")
                    continue
            
            await self._complete_sync_log(sync_log, len(machines), inserted, updated)
            
            return {
                "status": "success",
                "machines_processed": len(machines),
                "machines_inserted": inserted,
                "machines_updated": updated,
                "sync_id": sync_log.sync_id
            }
            
        except Exception as e:
            logger.error(f"Machine sync failed: {str(e)}")
            if sync_log:
                await self._complete_sync_log(sync_log, 0, 0, 0, 0, str(e))
            return {"status": "error", "error": str(e)}
    
    async def _extract_machines_from_jobs(self) -> List[Dict[str, Any]]:
        """Extract unique machines from job data"""
        try:
            # Get unique machine IDs from job data
            result = await self.cimco_db.execute(text("""
                SELECT DISTINCT machine as machine_id
                FROM joblog_ob 
                WHERE machine IS NOT NULL AND machine != ''
                ORDER BY machine
            """))
            
            machines = []
            for row in result:
                machines.append({
                    "machine_id": row[0],
                    "name": row[0],
                    "type": None,
                    "location": None
                })
            
            return machines
            
        except Exception as e:
            logger.error(f"Failed to extract machines from jobs: {str(e)}")
            return []
    
    async def _get_machines_from_table(self, machine_config: Dict) -> List[Dict[str, Any]]:
        """Get machines from dedicated machine table"""
        try:
            table_name = machine_config["source_table"]
            field_mapping = machine_config["field_mapping"]
            
            # Build query based on field mapping
            select_fields = []
            for target_field, source_field in field_mapping.items():
                select_fields.append(f"{source_field} as {target_field}")
            
            if not select_fields:
                select_fields = ["*"]
            
            query = f"SELECT {', '.join(select_fields)} FROM {table_name}"
            result = await self.cimco_db.execute(text(query))
            
            machines = []
            for row in result:
                machine_dict = dict(row._mapping)
                machines.append(machine_dict)
            
            return machines
            
        except Exception as e:
            logger.error(f"Failed to get machines from table: {str(e)}")
            return []   

    async def sync_operators(self, batch_size: int = 100) -> Dict[str, Any]:
        """Sync operators from CIMCO to analytics database"""
        sync_log = None
        try:
            if not self.cimco_db:
                return {"status": "error", "error": "CIMCO database not available"}
            
            sync_log = await self._create_sync_log("full", "operators")
            
            # Get unique operators from job data
            operators = await self._extract_operators_from_jobs()
            
            inserted, updated = 0, 0
            
            for operator_data in operators:
                try:
                    emp_id = operator_data.get("emp_id")
                    if not emp_id:
                        continue
                    
                    # Check if operator exists
                    existing = await self.postgres_db.execute(
                        select(Operator).where(Operator.emp_id == emp_id)
                    )
                    existing_operator = existing.scalar_one_or_none()
                    
                    if existing_operator:
                        # Update existing operator
                        existing_operator.operator_name = operator_data.get("operator_name")
                        existing_operator.op_number = operator_data.get("op_number")
                        existing_operator.updated_at = datetime.utcnow()
                        updated += 1
                    else:
                        # Create new operator
                        new_operator = Operator(
                            emp_id=emp_id,
                            operator_name=operator_data.get("operator_name"),
                            op_number=operator_data.get("op_number"),
                            is_active=True
                        )
                        self.postgres_db.add(new_operator)
                        inserted += 1
                        
                except Exception as e:
                    logger.error(f"Failed to process operator {operator_data}: {str(e)}")
                    continue
            
            await self._complete_sync_log(sync_log, len(operators), inserted, updated)
            
            return {
                "status": "success",
                "operators_processed": len(operators),
                "operators_inserted": inserted,
                "operators_updated": updated,
                "sync_id": sync_log.sync_id
            }
            
        except Exception as e:
            logger.error(f"Operator sync failed: {str(e)}")
            if sync_log:
                await self._complete_sync_log(sync_log, 0, 0, 0, 0, str(e))
            return {"status": "error", "error": str(e)}
    
    async def _extract_operators_from_jobs(self) -> List[Dict[str, Any]]:
        """Extract unique operators from job data"""
        try:
            result = await self.cimco_db.execute(text("""
                SELECT DISTINCT 
                    EmpID as emp_id,
                    OperatorName as operator_name,
                    OpNumber as op_number
                FROM joblog_ob 
                WHERE EmpID IS NOT NULL AND EmpID != ''
                ORDER BY EmpID
            """))
            
            operators = []
            for row in result:
                operators.append({
                    "emp_id": row[0],
                    "operator_name": row[1],
                    "op_number": row[2]
                })
            
            return operators
            
        except Exception as e:
            logger.error(f"Failed to extract operators from jobs: {str(e)}")
            return []
    
    async def sync_jobs(self, limit: int = 1000, machine_id: Optional[str] = None, 
                       incremental: bool = True, start_date: Optional[str] = None) -> Dict[str, Any]:
        """Sync job records from CIMCO to analytics database"""
        sync_log = None
        try:
            if not self.cimco_db:
                return {"status": "error", "error": "CIMCO database not available"}
            
            sync_type = "incremental" if incremental else "full"
            sync_log = await self._create_sync_log(sync_type, "jobs")
            
            # Determine date range for incremental sync
            if incremental and not start_date:
                last_job = await self.postgres_db.execute(
                    select(Job.start_time).order_by(Job.start_time.desc()).limit(1)
                )
                last_sync_time = last_job.scalar_one_or_none()
                if last_sync_time:
                    start_date = last_sync_time.strftime('%Y-%m-%d %H:%M:%S')
            
            # Get job data from CIMCO
            jobs_data = await self._get_jobs_from_cimco(limit, machine_id, start_date)
            
            inserted, updated, failed = 0, 0, 0
            
            # Process jobs in batches
            batch_size = 100
            for i in range(0, len(jobs_data), batch_size):
                batch = jobs_data[i:i + batch_size]
                batch_results = await self._process_job_batch(batch)
                inserted += batch_results["inserted"]
                updated += batch_results["updated"]
                failed += batch_results["failed"]
            
            await self._complete_sync_log(sync_log, len(jobs_data), inserted, updated, failed)
            
            return {
                "status": "success",
                "sync_type": sync_type,
                "jobs_processed": len(jobs_data),
                "jobs_inserted": inserted,
                "jobs_updated": updated,
                "jobs_failed": failed,
                "sync_id": sync_log.sync_id
            }
            
        except Exception as e:
            logger.error(f"Job sync failed: {str(e)}")
            if sync_log:
                await self._complete_sync_log(sync_log, 0, 0, 0, 0, str(e))
            return {"status": "error", "error": str(e)}

    async def _get_jobs_from_cimco(self, limit: int, machine_id: Optional[str], 
                                  start_date: Optional[str]) -> List[Dict[str, Any]]:
        """Get job data from CIMCO database"""
        try:
            # Build query conditions
            conditions = []
            params = {"limit": limit}
            
            if machine_id:
                conditions.append("machine = :machine_id")
                params["machine_id"] = machine_id
            
            if start_date:
                conditions.append("StartTime >= :start_date")
                params["start_date"] = start_date
            
            where_clause = ""
            if conditions:
                where_clause = "WHERE " + " AND ".join(conditions)
            
            query = f"""
                SELECT 
                    JobNumber, machine as MachineID, PartNumber, State,
                    StartTime, EndTime, EmpID, OperatorName, OpNumber,
                    PartsProduced, JobDuration,
                    RunningTime, SetupTime, WaitingSetupTime, NotFeedingTime,
                    AdjustmentTime, DressingTime, ToolingTime, EngineeringTime,
                    MaintenanceTime, BuyInTime, BreakShiftChangeTime, IdleTime
                FROM joblog_ob 
                {where_clause}
                ORDER BY StartTime DESC
                LIMIT :limit
            """
            
            result = await self.cimco_db.execute(text(query), params)
            
            jobs = []
            for row in result:
                job_dict = dict(row._mapping)
                jobs.append(job_dict)
            
            return jobs
            
        except Exception as e:
            logger.error(f"Failed to get jobs from CIMCO: {str(e)}")
            return []
    
    async def _process_job_batch(self, jobs_batch: List[Dict[str, Any]]) -> Dict[str, int]:
        """Process a batch of job records"""
        inserted, updated, failed = 0, 0, 0
        
        for job_data in jobs_batch:
            try:
                # Convert and validate job data
                processed_job = await self._convert_job_data(job_data)
                if not processed_job:
                    failed += 1
                    continue
                
                # Check if job exists (using composite key)
                existing = await self.postgres_db.execute(
                    select(JobRecord).where(
                        and_(
                            JobRecord.job_number == processed_job["job_number"],
                            JobRecord.machine_id == processed_job["machine_id"],
                            JobRecord.start_time == processed_job["start_time"]
                        )
                    )
                )
                existing_job = existing.scalar_one_or_none()
                
                if existing_job:
                    # Update existing job
                    for key, value in processed_job.items():
                        if hasattr(existing_job, key):
                            setattr(existing_job, key, value)
                    existing_job.updated_at = datetime.utcnow()
                    updated += 1
                else:
                    # Create new job
                    new_job = JobRecord(**processed_job)
                    self.postgres_db.add(new_job)
                    inserted += 1
                
                # Process downtime records
                await self._process_downtime_records(processed_job, existing_job or new_job)
                
            except Exception as e:
                logger.error(f"Failed to process job: {str(e)}")
                failed += 1
                continue
        
        return {"inserted": inserted, "updated": updated, "failed": failed}
    
    async def _convert_job_data(self, job_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Convert CIMCO job data to analytics format"""
        try:
            # Get machine and operator IDs
            machine_id = self.converter.clean_machine_id(job_data.get("MachineID"))
            if not machine_id:
                return None
            
            # Get or create machine reference
            machine = await self._get_or_create_machine_ref(machine_id)
            operator = await self._get_or_create_operator_ref(job_data)
            
            # Convert timestamps
            start_time = self.converter.convert_value(job_data.get("StartTime"), "datetime")
            end_time = self.converter.convert_value(job_data.get("EndTime"), "datetime")
            
            # Handle invalid end times (1969 dates)
            if end_time and end_time.year < 1970:
                end_time = None
            
            # Calculate metrics
            metrics = self.converter.calculate_job_metrics(job_data)
            
            return {
                "job_number": str(job_data.get("JobNumber", "")),
                "machine_id": machine_id,
                "emp_id": str(job_data.get("EmpID", "")),
                "operator_name": str(job_data.get("OperatorName", "")),
                "part_number": str(job_data.get("PartNumber", "")),
                "state": self.converter.normalize_job_state(job_data.get("State")),
                "start_time": start_time,
                "end_time": end_time,
                "job_duration": metrics.get("job_duration"),
                "parts_produced": self.converter.convert_value(job_data.get("PartsProduced"), "int") or 0,
                "op_number": self.converter.convert_value(job_data.get("OpNumber"), "int"),
                "running_time": metrics.get("running_time", 0),
                "setup_time": metrics.get("setup_time", 0),
                "waiting_setup_time": metrics.get("waiting_setup_time", 0),
                "not_feeding_time": metrics.get("not_feeding_time", 0),
                "adjustment_time": metrics.get("adjustment_time", 0),
                "dressing_time": metrics.get("dressing_time", 0),
                "tooling_time": metrics.get("tooling_time", 0),
                "engineering_time": metrics.get("engineering_time", 0),
                "maintenance_time": metrics.get("maintenance_time", 0),
                "buy_in_time": metrics.get("buy_in_time", 0),
                "break_shift_change_time": metrics.get("break_shift_change_time", 0),
                "idle_time": metrics.get("idle_time", 0)
            }
            
        except Exception as e:
            logger.error(f"Failed to convert job data: {str(e)}")
            return None

    async def _get_or_create_machine_ref(self, machine_id: str) -> Optional[Machine]:
        """Get or create machine reference"""
        try:
            existing = await self.postgres_db.execute(
                select(Machine).where(Machine.machine_id == machine_id)
            )
            machine = existing.scalar_one_or_none()
            
            if not machine:
                machine = Machine(
                    machine_id=machine_id,
                    name=machine_id,
                    is_active=True
                )
                self.postgres_db.add(machine)
                await self.postgres_db.flush()
            
            return machine
            
        except Exception as e:
            logger.error(f"Failed to get/create machine {machine_id}: {str(e)}")
            return None
    
    async def _get_or_create_operator_ref(self, job_data: Dict[str, Any]) -> Optional[Operator]:
        """Get or create operator reference"""
        try:
            emp_id = job_data.get("EmpID")
            if not emp_id:
                return None
            
            existing = await self.postgres_db.execute(
                select(Operator).where(Operator.emp_id == emp_id)
            )
            operator = existing.scalar_one_or_none()
            
            if not operator:
                operator = Operator(
                    emp_id=emp_id,
                    operator_name=job_data.get("OperatorName"),
                    op_number=self.converter.convert_value(job_data.get("OpNumber"), "int"),
                    is_active=True
                )
                self.postgres_db.add(operator)
                await self.postgres_db.flush()
            
            return operator
            
        except Exception as e:
            logger.error(f"Failed to get/create operator: {str(e)}")
            return None
    
    async def _process_downtime_records(self, job_data: Dict[str, Any], job: JobRecord) -> None:
        """Process downtime records for a job"""
        try:
            # Extract downtime categories
            downtime_categories = self.converter.extract_downtime_categories(job_data)
            total_duration = job_data.get("job_duration", 0)
            
            # Create downtime records for non-zero categories
            for category, duration in downtime_categories.items():
                if duration > 0 and category != "running_time":
                    # Check if downtime record exists
                    existing = await self.postgres_db.execute(
                        select(DowntimeRecord).where(
                            and_(
                                DowntimeRecord.job_id == job.id,
                                DowntimeRecord.downtime_type == category
                            )
                        )
                    )
                    existing_record = existing.scalar_one_or_none()
                    
                    percentage = (duration / total_duration * 100) if total_duration > 0 else 0
                    
                    if existing_record:
                        existing_record.duration = duration
                        existing_record.percentage_of_job = percentage
                    else:
                        downtime_record = DowntimeRecord(
                            job_id=job.id,
                            downtime_type=category,
                            duration=duration,
                            percentage_of_job=percentage,
                            is_planned=category in ["setup_time", "break_shift_change_time"]
                        )
                        self.postgres_db.add(downtime_record)
                        
        except Exception as e:
            logger.error(f"Failed to process downtime records: {str(e)}")
    
    async def sync_all(self, job_limit: int = 1000) -> Dict[str, Any]:
        """Perform complete synchronization of all data"""
        try:
            results = {
                "status": "success",
                "sync_timestamp": datetime.utcnow().isoformat(),
                "results": {}
            }
            
            # 1. Sync machines first
            logger.info("Starting machine synchronization...")
            machine_result = await self.sync_machines()
            results["results"]["machines"] = machine_result
            
            if machine_result["status"] != "success":
                results["status"] = "partial_failure"
            
            # 2. Sync operators
            logger.info("Starting operator synchronization...")
            operator_result = await self.sync_operators()
            results["results"]["operators"] = operator_result
            
            if operator_result["status"] != "success":
                results["status"] = "partial_failure"
            
            # 3. Sync jobs (incremental)
            logger.info("Starting job synchronization...")
            job_result = await self.sync_jobs(limit=job_limit, incremental=True)
            results["results"]["jobs"] = job_result
            
            if job_result["status"] != "success":
                results["status"] = "partial_failure"
            
            return results
            
        except Exception as e:
            logger.error(f"Full sync failed: {str(e)}")
            return {
                "status": "error",
                "error": str(e),
                "sync_timestamp": datetime.utcnow().isoformat()
            }
    
    async def get_sync_status(self) -> Dict[str, Any]:
        """Get recent synchronization status"""
        try:
            # Get recent sync logs
            recent_syncs = await self.postgres_db.execute(
                select(SyncLog)
                .order_by(SyncLog.start_time.desc())
                .limit(20)
            )
            syncs = recent_syncs.scalars().all()
            
            sync_list = []
            for sync in syncs:
                sync_list.append({
                    "sync_id": sync.sync_id,
                    "sync_type": sync.sync_type,
                    "source_table": sync.source_table,
                    "status": sync.status,
                    "start_time": sync.start_time.isoformat(),
                    "end_time": sync.end_time.isoformat() if sync.end_time else None,
                    "duration": sync.duration,
                    "records_processed": sync.records_processed,
                    "records_inserted": sync.records_inserted,
                    "records_updated": sync.records_updated,
                    "records_failed": sync.records_failed,
                    "error_message": sync.error_message
                })
            
            return {
                "status": "success",
                "recent_syncs": sync_list
            }
            
        except Exception as e:
            logger.error(f"Failed to get sync status: {str(e)}")
            return {
                "status": "error",
                "error": str(e)
            }