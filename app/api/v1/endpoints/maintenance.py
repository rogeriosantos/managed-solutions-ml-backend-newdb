"""
Predictive Maintenance API endpoints
"""
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text, func, and_, or_
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta

from app.core.database import get_postgres_db
from app.models.analytics import JobRecord

router = APIRouter()

@router.get("/maintenance/summary")
async def get_maintenance_summary(
    postgres_db: AsyncSession = Depends(get_postgres_db)
):
    """Get overall maintenance summary statistics"""
    try:
        # Get maintenance statistics
        result = await postgres_db.execute(text("""
            SELECT 
                COUNT(*) as total_jobs,
                COUNT(CASE WHEN maintenance_time > 0 THEN 1 END) as jobs_with_maintenance,
                SUM(maintenance_time) as total_maintenance_time,
                AVG(maintenance_time) as avg_maintenance_time,
                AVG(CASE WHEN job_duration > 0 THEN running_time::float / job_duration ELSE 0 END) as avg_efficiency,
                COUNT(DISTINCT machine_id) as total_machines
            FROM job_records
            WHERE created_at >= NOW() - INTERVAL '30 days'
        """))
        
        stats = result.fetchone()
        
        if not stats:
            return {"status": "error", "message": "No data found"}
        
        maintenance_rate = (stats.jobs_with_maintenance / stats.total_jobs * 100) if stats.total_jobs > 0 else 0
        
        return {
            "status": "success",
            "summary": {
                "total_jobs": stats.total_jobs,
                "jobs_with_maintenance": stats.jobs_with_maintenance,
                "maintenance_rate_percent": round(maintenance_rate, 2),
                "total_maintenance_hours": round(stats.total_maintenance_time / 3600, 2),
                "avg_maintenance_minutes": round(stats.avg_maintenance_time / 60, 2),
                "avg_efficiency_percent": round(stats.avg_efficiency * 100, 2),
                "total_machines": stats.total_machines,
                "period": "Last 30 days"
            }
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting maintenance summary: {str(e)}")

@router.get("/maintenance/by-machine")
async def get_maintenance_by_machine(
    limit: int = Query(20, description="Number of machines to return"),
    postgres_db: AsyncSession = Depends(get_postgres_db)
):
    """Get maintenance statistics by machine"""
    try:
        result = await postgres_db.execute(text("""
            SELECT 
                machine_id,
                COUNT(*) as total_jobs,
                COUNT(CASE WHEN maintenance_time > 0 THEN 1 END) as maintenance_jobs,
                SUM(maintenance_time) as total_maintenance_time,
                AVG(maintenance_time) as avg_maintenance_time,
                AVG(CASE WHEN job_duration > 0 THEN running_time::float / job_duration ELSE 0 END) as efficiency,
                SUM(parts_produced) as total_parts,
                MAX(start_time) as last_job_time
            FROM job_records
            WHERE created_at >= NOW() - INTERVAL '30 days'
            GROUP BY machine_id
            ORDER BY total_maintenance_time DESC
            LIMIT :limit
        """), {"limit": limit})
        
        machines = []
        for row in result:
            maintenance_rate = (row.maintenance_jobs / row.total_jobs * 100) if row.total_jobs > 0 else 0
            
            machines.append({
                "machine_id": row.machine_id,
                "total_jobs": row.total_jobs,
                "maintenance_jobs": row.maintenance_jobs,
                "maintenance_rate_percent": round(maintenance_rate, 2),
                "total_maintenance_hours": round(row.total_maintenance_time / 3600, 2),
                "avg_maintenance_minutes": round(row.avg_maintenance_time / 60, 2),
                "efficiency_percent": round(row.efficiency * 100, 2),
                "total_parts_produced": row.total_parts,
                "last_job_time": row.last_job_time.isoformat() if row.last_job_time else None
            })
        
        return {
            "status": "success",
            "machines": machines,
            "count": len(machines)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting machine maintenance data: {str(e)}")

@router.get("/maintenance/trends")
async def get_maintenance_trends(
    machine_id: Optional[str] = Query(None, description="Filter by specific machine"),
    days: int = Query(30, description="Number of days to analyze"),
    postgres_db: AsyncSession = Depends(get_postgres_db)
):
    """Get maintenance trends over time"""
    try:
        machine_filter = "AND machine_id = :machine_id" if machine_id else ""
        params = {"days": days}
        if machine_id:
            params["machine_id"] = machine_id
        
        result = await postgres_db.execute(text(f"""
            SELECT 
                DATE(start_time) as job_date,
                COUNT(*) as jobs_count,
                COUNT(CASE WHEN maintenance_time > 0 THEN 1 END) as maintenance_jobs,
                SUM(maintenance_time) as daily_maintenance_time,
                AVG(CASE WHEN job_duration > 0 THEN running_time::float / job_duration ELSE 0 END) as daily_efficiency
            FROM job_records
            WHERE start_time >= NOW() - INTERVAL ':days days'
            {machine_filter}
            GROUP BY DATE(start_time)
            ORDER BY job_date DESC
        """.replace(':days', str(days))), params)
        
        trends = []
        for row in result:
            maintenance_rate = (row.maintenance_jobs / row.jobs_count * 100) if row.jobs_count > 0 else 0
            
            trends.append({
                "date": row.job_date.isoformat(),
                "jobs_count": row.jobs_count,
                "maintenance_jobs": row.maintenance_jobs,
                "maintenance_rate_percent": round(maintenance_rate, 2),
                "maintenance_hours": round(row.daily_maintenance_time / 3600, 2),
                "efficiency_percent": round(row.daily_efficiency * 100, 2)
            })
        
        return {
            "status": "success",
            "trends": trends,
            "machine_id": machine_id,
            "period_days": days
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting maintenance trends: {str(e)}")

@router.get("/maintenance/alerts")
async def get_maintenance_alerts(
    postgres_db: AsyncSession = Depends(get_postgres_db)
):
    """Get maintenance alerts for machines that may need attention"""
    try:
        # Define thresholds
        high_maintenance_threshold = 3600  # 1 hour
        low_efficiency_threshold = 0.7     # 70%
        
        result = await postgres_db.execute(text("""
            WITH recent_stats AS (
                SELECT 
                    machine_id,
                    COUNT(*) as recent_jobs,
                    AVG(maintenance_time) as avg_maintenance,
                    AVG(CASE WHEN job_duration > 0 THEN running_time::float / job_duration ELSE 0 END) as avg_efficiency,
                    SUM(maintenance_time) as total_maintenance,
                    MAX(start_time) as last_job
                FROM job_records
                WHERE start_time >= NOW() - INTERVAL '7 days'
                GROUP BY machine_id
                HAVING COUNT(*) >= 3  -- At least 3 jobs in last week
            )
            SELECT 
                machine_id,
                recent_jobs,
                avg_maintenance,
                avg_efficiency,
                total_maintenance,
                last_job,
                CASE 
                    WHEN avg_maintenance > :high_maintenance AND avg_efficiency < :low_efficiency THEN 'CRITICAL'
                    WHEN avg_maintenance > :high_maintenance THEN 'HIGH_MAINTENANCE'
                    WHEN avg_efficiency < :low_efficiency THEN 'LOW_EFFICIENCY'
                    ELSE 'NORMAL'
                END as alert_level
            FROM recent_stats
            WHERE avg_maintenance > :high_maintenance OR avg_efficiency < :low_efficiency
            ORDER BY 
                CASE 
                    WHEN avg_maintenance > :high_maintenance AND avg_efficiency < :low_efficiency THEN 1
                    WHEN avg_maintenance > :high_maintenance THEN 2
                    WHEN avg_efficiency < :low_efficiency THEN 3
                    ELSE 4
                END,
                avg_maintenance DESC
        """), {
            "high_maintenance": high_maintenance_threshold,
            "low_efficiency": low_efficiency_threshold
        })
        
        alerts = []
        for row in result:
            alert_reasons = []
            if row.avg_maintenance > high_maintenance_threshold:
                alert_reasons.append(f"High maintenance time ({row.avg_maintenance/60:.1f} min avg)")
            if row.avg_efficiency < low_efficiency_threshold:
                alert_reasons.append(f"Low efficiency ({row.avg_efficiency*100:.1f}%)")
            
            alerts.append({
                "machine_id": row.machine_id,
                "alert_level": row.alert_level,
                "reasons": alert_reasons,
                "recent_jobs": row.recent_jobs,
                "avg_maintenance_minutes": round(row.avg_maintenance / 60, 1),
                "efficiency_percent": round(row.avg_efficiency * 100, 1),
                "total_maintenance_hours": round(row.total_maintenance / 3600, 2),
                "last_job_time": row.last_job.isoformat() if row.last_job else None
            })
        
        return {
            "status": "success",
            "alerts": alerts,
            "alert_count": len(alerts),
            "thresholds": {
                "high_maintenance_minutes": high_maintenance_threshold / 60,
                "low_efficiency_percent": low_efficiency_threshold * 100
            }
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting maintenance alerts: {str(e)}")

@router.get("/maintenance/machine/{machine_id}")
async def get_machine_maintenance_detail(
    machine_id: str,
    days: int = Query(30, description="Number of days to analyze"),
    postgres_db: AsyncSession = Depends(get_postgres_db)
):
    """Get detailed maintenance information for a specific machine"""
    try:
        # Get machine statistics
        result = await postgres_db.execute(text("""
            SELECT 
                COUNT(*) as total_jobs,
                COUNT(CASE WHEN maintenance_time > 0 THEN 1 END) as maintenance_jobs,
                SUM(maintenance_time) as total_maintenance_time,
                AVG(maintenance_time) as avg_maintenance_time,
                MIN(maintenance_time) as min_maintenance_time,
                MAX(maintenance_time) as max_maintenance_time,
                AVG(CASE WHEN job_duration > 0 THEN running_time::float / job_duration ELSE 0 END) as avg_efficiency,
                SUM(parts_produced) as total_parts,
                AVG(setup_time) as avg_setup_time,
                AVG(idle_time) as avg_idle_time,
                MIN(start_time) as first_job,
                MAX(start_time) as last_job
            FROM job_records
            WHERE machine_id = :machine_id 
            AND start_time >= NOW() - INTERVAL ':days days'
        """.replace(':days', str(days))), {"machine_id": machine_id, "days": days})
        
        stats = result.fetchone()
        
        if not stats or stats.total_jobs == 0:
            raise HTTPException(status_code=404, detail=f"No data found for machine {machine_id}")
        
        # Get recent maintenance events
        maintenance_events = await postgres_db.execute(text("""
            SELECT 
                start_time,
                job_number,
                maintenance_time,
                setup_time,
                idle_time,
                parts_produced,
                CASE WHEN job_duration > 0 THEN running_time::float / job_duration ELSE 0 END as efficiency
            FROM job_records
            WHERE machine_id = :machine_id 
            AND maintenance_time > 0
            AND start_time >= NOW() - INTERVAL ':days days'
            ORDER BY start_time DESC
            LIMIT 10
        """.replace(':days', str(days))), {"machine_id": machine_id})
        
        events = []
        for event in maintenance_events:
            events.append({
                "date": event.start_time.isoformat(),
                "job_number": event.job_number,
                "maintenance_minutes": round(event.maintenance_time / 60, 1),
                "setup_minutes": round(event.setup_time / 60, 1),
                "idle_minutes": round(event.idle_time / 60, 1),
                "parts_produced": event.parts_produced,
                "efficiency_percent": round(event.efficiency * 100, 1)
            })
        
        maintenance_rate = (stats.maintenance_jobs / stats.total_jobs * 100) if stats.total_jobs > 0 else 0
        
        return {
            "status": "success",
            "machine_id": machine_id,
            "period_days": days,
            "statistics": {
                "total_jobs": stats.total_jobs,
                "maintenance_jobs": stats.maintenance_jobs,
                "maintenance_rate_percent": round(maintenance_rate, 2),
                "total_maintenance_hours": round(stats.total_maintenance_time / 3600, 2),
                "avg_maintenance_minutes": round(stats.avg_maintenance_time / 60, 2),
                "max_maintenance_minutes": round(stats.max_maintenance_time / 60, 2),
                "avg_efficiency_percent": round(stats.avg_efficiency * 100, 2),
                "total_parts_produced": stats.total_parts,
                "avg_setup_minutes": round(stats.avg_setup_time / 60, 2),
                "avg_idle_minutes": round(stats.avg_idle_time / 60, 2),
                "first_job": stats.first_job.isoformat() if stats.first_job else None,
                "last_job": stats.last_job.isoformat() if stats.last_job else None
            },
            "recent_maintenance_events": events
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting machine details: {str(e)}")