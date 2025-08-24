"""
Pydantic schemas for CIMCO data models
"""
from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional
from enum import Enum

from app.schemas.base import BaseSchema


class JobState(str, Enum):
    """Job state enumeration"""
    OPENED = "OPENED"
    CLOSED = "CLOSED"


class JobLogRecord(BaseSchema):
    """CIMCO JobLog record schema"""
    machine: str = Field(..., description="Machine identifier")
    start_time: datetime = Field(..., alias="StartTime", description="Job start time")
    end_time: Optional[datetime] = Field(None, alias="EndTime", description="Job end time")
    job_number: str = Field(..., alias="JobNumber", description="Job number")
    state: JobState = Field(..., alias="State", description="Job state")
    part_number: str = Field(..., alias="PartNumber", description="Part number")
    emp_id: str = Field(..., alias="EmpID", description="Employee ID")
    operator_name: str = Field(..., alias="OperatorName", description="Operator name")
    op_number: int = Field(..., alias="OpNumber", description="Operation number")
    parts_produced: int = Field(..., alias="PartsProduced", description="Number of parts produced")
    job_duration: int = Field(..., alias="JobDuration", description="Total job duration in seconds")
    running_time: int = Field(..., alias="RunningTime", description="Machine running time in seconds")
    setup_time: int = Field(..., alias="SetupTime", description="Setup time in seconds")
    waiting_setup_time: int = Field(..., alias="WaitingSetupTime", description="Waiting for setup time in seconds")
    not_feeding_time: int = Field(..., alias="NotFeedingTime", description="Not feeding time in seconds")
    adjustment_time: int = Field(..., alias="AdjustmentTime", description="Adjustment time in seconds")
    dressing_time: int = Field(..., alias="DressingTime", description="Dressing time in seconds")
    tooling_time: int = Field(..., alias="ToolingTime", description="Tooling time in seconds")
    engineering_time: int = Field(..., alias="EngineeringTime", description="Engineering time in seconds")
    maintenance_time: int = Field(..., alias="MaintenanceTime", description="Maintenance time in seconds")
    buy_in_time: int = Field(..., alias="BuyInTime", description="Buy-in time in seconds")
    break_shift_change_time: int = Field(..., alias="BreakShiftChangeTime", description="Break/shift change time in seconds")
    idle_time: int = Field(..., alias="IdleTime", description="Idle time in seconds")

    class Config:
        allow_population_by_field_name = True
        use_enum_values = True


class JobLogSummary(BaseSchema):
    """Summary statistics for job log data"""
    total_jobs: int
    machines: list[str]
    date_range: dict[str, Optional[datetime]]
    job_states: dict[str, int]
    total_parts_produced: int
    average_job_duration: float
    average_setup_time: float
    total_downtime: int


class MachineEfficiency(BaseSchema):
    """Machine efficiency metrics"""
    machine: str
    total_jobs: int
    total_runtime: int
    total_setup_time: int
    total_idle_time: int
    total_maintenance_time: int
    efficiency_percentage: float
    oee_availability: float
    oee_performance: float
    oee_quality: float
    overall_oee: float


class DowntimeAnalysis(BaseSchema):
    """Downtime analysis for a machine"""
    machine: str
    period: str
    total_downtime: int
    setup_time: int
    maintenance_time: int
    idle_time: int
    adjustment_time: int
    engineering_time: int
    break_shift_change_time: int
    downtime_breakdown: dict[str, float]  # Percentage breakdown