"""
Data type conversion utilities for CIMCO to Analytics mapping
"""
from typing import Any, Dict, Optional, Union
from datetime import datetime, date
import logging

logger = logging.getLogger(__name__)

class DataConverter:
    """Utility class for converting data types between CIMCO and Analytics schemas"""
    
    @staticmethod
    def mysql_to_postgres_type(mysql_type: str) -> str:
        """Convert MySQL data type to PostgreSQL equivalent"""
        mysql_type = mysql_type.lower()
        
        # Integer types
        if mysql_type in ['tinyint', 'smallint', 'mediumint', 'int', 'integer']:
            return 'INTEGER'
        elif mysql_type == 'bigint':
            return 'BIGINT'
        
        # String types
        elif mysql_type.startswith('varchar'):
            return mysql_type.upper()
        elif mysql_type in ['char', 'text', 'tinytext', 'mediumtext', 'longtext']:
            return 'TEXT'
        
        # Date/Time types
        elif mysql_type == 'datetime':
            return 'TIMESTAMP'
        elif mysql_type == 'date':
            return 'DATE'
        elif mysql_type == 'time':
            return 'TIME'
        elif mysql_type == 'timestamp':
            return 'TIMESTAMP'
        
        # Numeric types
        elif mysql_type.startswith('decimal') or mysql_type.startswith('numeric'):
            return mysql_type.upper()
        elif mysql_type in ['float', 'double']:
            return 'FLOAT'
        
        # Boolean
        elif mysql_type == 'boolean' or mysql_type == 'bool':
            return 'BOOLEAN'
        
        # Default to TEXT for unknown types
        else:
            logger.warning(f"Unknown MySQL type: {mysql_type}, defaulting to TEXT")
            return 'TEXT'
    
    @staticmethod
    def convert_value(value: Any, target_type: str) -> Any:
        """Convert a value to the target type"""
        if value is None:
            return None
        
        target_type = target_type.lower()
        
        try:
            # Integer conversions
            if target_type in ['integer', 'int', 'bigint']:
                if isinstance(value, str) and value.strip() == '':
                    return None
                return int(float(value)) if value is not None else None
            
            # Float conversions
            elif target_type in ['float', 'double', 'decimal', 'numeric']:
                if isinstance(value, str) and value.strip() == '':
                    return None
                return float(value) if value is not None else None
            
            # String conversions
            elif target_type in ['varchar', 'text', 'char']:
                return str(value) if value is not None else None
            
            # Boolean conversions
            elif target_type in ['boolean', 'bool']:
                if isinstance(value, str):
                    return value.lower() in ['true', '1', 'yes', 'on']
                return bool(value)
            
            # Date/Time conversions
            elif target_type in ['datetime', 'timestamp']:
                if isinstance(value, str):
                    # Try to parse common datetime formats
                    for fmt in ['%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%d']:
                        try:
                            return datetime.strptime(value, fmt)
                        except ValueError:
                            continue
                    return None
                elif isinstance(value, (datetime, date)):
                    return value
                return None
            
            elif target_type == 'date':
                if isinstance(value, str):
                    try:
                        return datetime.strptime(value, '%Y-%m-%d').date()
                    except ValueError:
                        return None
                elif isinstance(value, datetime):
                    return value.date()
                elif isinstance(value, date):
                    return value
                return None
            
            # Default: return as-is
            else:
                return value
                
        except (ValueError, TypeError) as e:
            logger.warning(f"Failed to convert value {value} to type {target_type}: {str(e)}")
            return None
    
    @staticmethod
    def clean_machine_id(machine_id: Any) -> Optional[str]:
        """Clean and standardize machine ID"""
        if machine_id is None:
            return None
        
        # Convert to string and strip whitespace
        clean_id = str(machine_id).strip()
        
        # Remove common prefixes/suffixes
        clean_id = clean_id.replace('Machine', '').replace('machine', '')
        clean_id = clean_id.strip()
        
        # Return None if empty after cleaning
        return clean_id if clean_id else None
    
    @staticmethod
    def parse_time_duration(duration_str: str) -> Optional[int]:
        """Parse time duration string to seconds"""
        if not duration_str or duration_str.strip() == '':
            return None
        
        try:
            # If it's already a number (seconds)
            if duration_str.isdigit():
                return int(duration_str)
            
            # Parse HH:MM:SS format
            if ':' in duration_str:
                parts = duration_str.split(':')
                if len(parts) == 3:
                    hours, minutes, seconds = map(int, parts)
                    return hours * 3600 + minutes * 60 + seconds
                elif len(parts) == 2:
                    minutes, seconds = map(int, parts)
                    return minutes * 60 + seconds
            
            # Try to parse as float (decimal seconds)
            return int(float(duration_str))
            
        except (ValueError, TypeError):
            logger.warning(f"Failed to parse duration: {duration_str}")
            return None
    
    @staticmethod
    def normalize_job_state(state: Any) -> Optional[str]:
        """Normalize job state values"""
        if state is None:
            return None
        
        state_str = str(state).strip().upper()
        
        # Map common variations
        state_mapping = {
            'OPEN': 'OPENED',
            'CLOSE': 'CLOSED',
            'COMPLETE': 'CLOSED',
            'COMPLETED': 'CLOSED',
            'FINISH': 'CLOSED',
            'FINISHED': 'CLOSED',
            'START': 'OPENED',
            'STARTED': 'OPENED',
            'RUNNING': 'OPENED',
            'ACTIVE': 'OPENED'
        }
        
        return state_mapping.get(state_str, state_str)
    
    @staticmethod
    def extract_downtime_categories(job_data: Dict[str, Any]) -> Dict[str, int]:
        """Extract downtime categories from job data"""
        downtime_fields = {
            'setup_time': ['setuptime', 'setup_time', 'SetupTime'],
            'waiting_setup_time': ['waitingsetuptime', 'waiting_setup_time', 'WaitingSetupTime'],
            'not_feeding_time': ['notfeedingtime', 'not_feeding_time', 'NotFeedingTime'],
            'adjustment_time': ['adjustmenttime', 'adjustment_time', 'AdjustmentTime'],
            'dressing_time': ['dressingtime', 'dressing_time', 'DressingTime'],
            'tooling_time': ['toolingtime', 'tooling_time', 'ToolingTime'],
            'engineering_time': ['engineeringtime', 'engineering_time', 'EngineeringTime'],
            'maintenance_time': ['maintenancetime', 'maintenance_time', 'MaintenanceTime'],
            'buy_in_time': ['buyintime', 'buy_in_time', 'BuyInTime'],
            'break_shift_change_time': ['breakshiftchangetime', 'break_shift_change_time', 'BreakShiftChangeTime'],
            'idle_time': ['idletime', 'idle_time', 'IdleTime'],
            'running_time': ['runningtime', 'running_time', 'RunningTime', 'runtime', 'run_time']
        }
        
        extracted = {}
        
        for category, field_names in downtime_fields.items():
            value = None
            for field_name in field_names:
                if field_name in job_data and job_data[field_name] is not None:
                    value = job_data[field_name]
                    break
            
            # Convert to seconds if found
            if value is not None:
                if isinstance(value, str):
                    extracted[category] = DataConverter.parse_time_duration(value) or 0
                else:
                    extracted[category] = int(value) if value else 0
            else:
                extracted[category] = 0
        
        return extracted
    
    @staticmethod
    def calculate_job_metrics(job_data: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate derived metrics from job data"""
        metrics = {}
        
        # Extract downtime categories
        downtime = DataConverter.extract_downtime_categories(job_data)
        
        # Calculate total downtime
        total_downtime = sum(downtime.values()) - downtime.get('running_time', 0)
        metrics['total_downtime'] = max(0, total_downtime)
        
        # Calculate job duration if start/end times available
        start_time = job_data.get('StartTime') or job_data.get('start_time')
        end_time = job_data.get('EndTime') or job_data.get('end_time')
        
        if start_time and end_time:
            if isinstance(start_time, str):
                start_time = DataConverter.convert_value(start_time, 'datetime')
            if isinstance(end_time, str):
                end_time = DataConverter.convert_value(end_time, 'datetime')
            
            if start_time and end_time:
                duration = (end_time - start_time).total_seconds()
                metrics['job_duration'] = int(duration)
                
                # Calculate efficiency if we have running time
                running_time = downtime.get('running_time', 0)
                if duration > 0:
                    metrics['efficiency_percentage'] = (running_time / duration) * 100
                else:
                    metrics['efficiency_percentage'] = 0
        
        # Add downtime breakdown
        metrics.update(downtime)
        
        return metrics