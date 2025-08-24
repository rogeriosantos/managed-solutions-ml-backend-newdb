"""
SQLAlchemy models for analytics database
"""
from sqlalchemy import Column, String, Integer, DateTime, Float, Boolean, Text, Index
from sqlalchemy.dialects.postgresql import UUID
from datetime import datetime
import uuid

from app.core.database import Base


class Machine(Base):
    """Machine model for analytics database"""
    __tablename__ = "machines"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    machine_id = Column(String(50), unique=True, nullable=False, index=True)
    name = Column(String(100))
    location = Column(String(100))
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class Operator(Base):
    """Operator model for analytics database"""
    __tablename__ = "operators"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    emp_id = Column(String(50), unique=True, nullable=False, index=True)
    name = Column(String(100))
    department = Column(String(100))
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class Job(Base):
    """Job model for analytics database"""
    __tablename__ = "jobs"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    job_number = Column(String(50), nullable=False, index=True)
    machine_id = Column(String(50), nullable=False, index=True)
    part_number = Column(String(50), index=True)
    state = Column(String(20), nullable=False, index=True)
    start_time = Column(DateTime, nullable=False, index=True)
    end_time = Column(DateTime, index=True)
    emp_id = Column(String(50), index=True)
    operator_name = Column(String(100))
    parts_produced = Column(Integer, default=0)
    job_duration = Column(Integer, default=0)
    running_time = Column(Integer, default=0)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class DowntimeRecord(Base):
    """Downtime record model for analytics database"""
    __tablename__ = "downtime_records"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    job_id = Column(UUID(as_uuid=True), nullable=False, index=True)
    downtime_type = Column(String(50), nullable=False, index=True)
    duration = Column(Integer, default=0)  # seconds
    start_time = Column(DateTime, index=True)
    end_time = Column(DateTime, index=True)
    reason = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow)


class SyncLog(Base):
    """Sync log model for analytics database"""
    __tablename__ = "sync_logs"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    sync_id = Column(String(100), unique=True, nullable=False, index=True)
    sync_type = Column(String(50), nullable=False)
    source_table = Column(String(100), nullable=False)
    status = Column(String(20), nullable=False)
    records_processed = Column(Integer, default=0)
    records_inserted = Column(Integer, default=0)
    records_updated = Column(Integer, default=0)
    records_failed = Column(Integer, default=0)
    start_time = Column(DateTime, nullable=False)
    end_time = Column(DateTime)
    duration = Column(Float)
    error_message = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow)


class JobRecord(Base):
    """Job record model for analytics database"""
    __tablename__ = "job_records"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    
    # Core job information
    machine_id = Column(String(50), nullable=False, index=True)
    job_number = Column(String(50), nullable=False, index=True)
    part_number = Column(String(50), index=True)
    state = Column(String(20), nullable=False, index=True)
    
    # Timing information
    start_time = Column(DateTime, nullable=False, index=True)
    end_time = Column(DateTime, index=True)
    
    # Operator information
    emp_id = Column(String(50), index=True)
    operator_name = Column(String(100))
    op_number = Column(Integer)
    
    # Production metrics
    parts_produced = Column(Integer, default=0)
    job_duration = Column(Integer, default=0)  # seconds
    running_time = Column(Integer, default=0)  # seconds
    
    # Downtime categories (all in seconds)
    setup_time = Column(Integer, default=0)
    waiting_setup_time = Column(Integer, default=0)
    not_feeding_time = Column(Integer, default=0)
    adjustment_time = Column(Integer, default=0)
    dressing_time = Column(Integer, default=0)
    tooling_time = Column(Integer, default=0)
    engineering_time = Column(Integer, default=0)
    maintenance_time = Column(Integer, default=0)
    buy_in_time = Column(Integer, default=0)
    break_shift_change_time = Column(Integer, default=0)
    idle_time = Column(Integer, default=0)
    
    # Metadata
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    synced_at = Column(DateTime, default=datetime.utcnow)
    
    # Indexes for performance
    __table_args__ = (
        Index('idx_machine_start_time', 'machine_id', 'start_time'),
        Index('idx_job_state', 'job_number', 'state'),
        Index('idx_part_machine', 'part_number', 'machine_id'),
    )


class SyncStatus(Base):
    """Track synchronization status"""
    __tablename__ = "sync_status"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    sync_type = Column(String(50), nullable=False)  # 'full', 'incremental'
    table_name = Column(String(100), nullable=False)
    status = Column(String(20), nullable=False)  # 'running', 'completed', 'failed'
    
    # Sync metrics
    records_processed = Column(Integer, default=0)
    records_inserted = Column(Integer, default=0)
    records_updated = Column(Integer, default=0)
    records_failed = Column(Integer, default=0)
    
    # Timing
    started_at = Column(DateTime, nullable=False)
    completed_at = Column(DateTime)
    duration_seconds = Column(Float)
    
    # Error information
    error_message = Column(Text)
    error_details = Column(Text)
    
    # Metadata
    created_at = Column(DateTime, default=datetime.utcnow)


class MachineMetrics(Base):
    """Aggregated machine metrics for faster analytics"""
    __tablename__ = "machine_metrics"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    machine_id = Column(String(50), nullable=False, index=True)
    
    # Time period
    period_start = Column(DateTime, nullable=False, index=True)
    period_end = Column(DateTime, nullable=False)
    period_type = Column(String(20), nullable=False)  # 'hour', 'day', 'week', 'month'
    
    # Production metrics
    total_jobs = Column(Integer, default=0)
    completed_jobs = Column(Integer, default=0)
    parts_produced = Column(Integer, default=0)
    
    # Time metrics (seconds)
    total_runtime = Column(Integer, default=0)
    total_setup_time = Column(Integer, default=0)
    total_maintenance_time = Column(Integer, default=0)
    total_idle_time = Column(Integer, default=0)
    total_downtime = Column(Integer, default=0)
    
    # Efficiency metrics
    availability = Column(Float, default=0.0)
    performance = Column(Float, default=0.0)
    quality = Column(Float, default=0.0)
    oee = Column(Float, default=0.0)
    
    # Metadata
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    __table_args__ = (
        Index('idx_machine_period', 'machine_id', 'period_start', 'period_type'),
    )