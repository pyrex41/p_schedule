#!/usr/bin/env python3
"""
Health Monitor for Email Scheduler

This module provides health checks, metrics, and monitoring capabilities
for the email scheduling system. It tracks system performance, database
health, and overall scheduler status.
"""

import sqlite3
import json
import logging
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
import time

logger = logging.getLogger(__name__)

# ============================================================================
# HEALTH CHECK DATA STRUCTURES
# ============================================================================

@dataclass
class HealthStatus:
    """Overall health status of the scheduler system"""
    status: str  # "healthy", "degraded", "unhealthy"
    timestamp: str
    database_connected: bool
    last_successful_run: Optional[str]
    pending_schedules: int
    error_rate_24h: float
    issues: List[str]
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        return asdict(self)

@dataclass
class SystemMetrics:
    """Detailed system metrics for monitoring"""
    timestamp: str
    
    # Scheduler performance metrics
    total_contacts: int
    processed_contacts_24h: int
    scheduled_emails_24h: int
    skipped_emails_24h: int
    failed_operations_24h: int
    
    # Load balancing metrics
    avg_daily_email_volume: float
    max_daily_email_volume: int
    load_balancing_applied_count: int
    effective_date_smoothing_count: int
    
    # Database performance metrics
    db_size_mb: float
    avg_query_time_ms: float
    active_connections: int
    index_efficiency: float
    
    # Follow-up metrics
    followups_scheduled_24h: int
    behavior_analysis_success_rate: float
    
    # State compliance metrics
    exclusion_window_hits_24h: int
    state_compliance_rate: float
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        return asdict(self)

# ============================================================================
# HEALTH MONITOR CLASS
# ============================================================================

class HealthMonitor:
    """Health monitoring and metrics collection for email scheduler"""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.start_time = datetime.now()
    
    def get_health_status(self) -> HealthStatus:
        """Get current health status of the system"""
        issues = []
        
        # Check database connectivity
        db_connected = self.check_db_connection()
        if not db_connected:
            issues.append("Database connection failed")
        
        # Check last successful run
        last_run = self.get_last_successful_run()
        if last_run:
            run_age = datetime.now() - datetime.fromisoformat(last_run)
            if run_age > timedelta(days=2):
                issues.append(f"Last successful run was {run_age.days} days ago")
        else:
            issues.append("No successful runs found")
        
        # Check pending schedules backlog
        pending_count = self.count_pending_schedules()
        if pending_count > 10000:  # Threshold for concern
            issues.append(f"Large pending schedule backlog: {pending_count}")
        
        # Calculate error rate
        error_rate = self.calculate_error_rate_24h()
        if error_rate > 0.05:  # 5% error rate threshold
            issues.append(f"High error rate: {error_rate:.2%}")
        
        # Determine overall status
        if not db_connected:
            status = "unhealthy"
        elif len(issues) > 2:
            status = "degraded"
        elif len(issues) > 0:
            status = "degraded"
        else:
            status = "healthy"
        
        return HealthStatus(
            status=status,
            timestamp=datetime.now().isoformat(),
            database_connected=db_connected,
            last_successful_run=last_run,
            pending_schedules=pending_count,
            error_rate_24h=error_rate,
            issues=issues
        )
    
    def get_metrics(self) -> SystemMetrics:
        """Get detailed system metrics"""
        now = datetime.now()
        
        return SystemMetrics(
            timestamp=now.isoformat(),
            
            # Scheduler performance
            total_contacts=self._get_total_contacts(),
            processed_contacts_24h=self._get_processed_contacts_24h(),
            scheduled_emails_24h=self._get_scheduled_emails_24h(),
            skipped_emails_24h=self._get_skipped_emails_24h(),
            failed_operations_24h=self._get_failed_operations_24h(),
            
            # Load balancing
            avg_daily_email_volume=self._get_avg_daily_volume(),
            max_daily_email_volume=self._get_max_daily_volume(),
            load_balancing_applied_count=self._get_load_balancing_count_24h(),
            effective_date_smoothing_count=self._get_smoothing_count_24h(),
            
            # Database performance
            db_size_mb=self._get_db_size_mb(),
            avg_query_time_ms=self._calculate_avg_query_time(),
            active_connections=self._get_active_connections(),
            index_efficiency=self._calculate_index_efficiency(),
            
            # Follow-up metrics
            followups_scheduled_24h=self._get_followups_24h(),
            behavior_analysis_success_rate=self._get_behavior_analysis_rate(),
            
            # State compliance
            exclusion_window_hits_24h=self._get_exclusion_hits_24h(),
            state_compliance_rate=self._calculate_compliance_rate()
        )
    
    def check_db_connection(self) -> bool:
        """Check if database connection is working"""
        try:
            with sqlite3.connect(self.db_path, timeout=5) as conn:
                cursor = conn.execute("SELECT 1")
                cursor.fetchone()
                return True
        except Exception as e:
            logger.error(f"Database connection check failed: {e}")
            return False
    
    def get_last_successful_run(self) -> Optional[str]:
        """Get timestamp of last successful scheduler run"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute("""
                    SELECT completed_at FROM scheduler_checkpoints
                    WHERE status = 'completed'
                    ORDER BY completed_at DESC
                    LIMIT 1
                """)
                row = cursor.fetchone()
                return row[0] if row else None
        except Exception as e:
            logger.error(f"Error checking last successful run: {e}")
            return None
    
    def count_pending_schedules(self) -> int:
        """Count pending email schedules"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute("""
                    SELECT COUNT(*) FROM email_schedules
                    WHERE status = 'pre-scheduled'
                    AND scheduled_send_date >= date('now')
                """)
                return cursor.fetchone()[0]
        except Exception as e:
            logger.error(f"Error counting pending schedules: {e}")
            return 0
    
    def calculate_error_rate_24h(self) -> float:
        """Calculate error rate over last 24 hours"""
        try:
            cutoff = datetime.now() - timedelta(days=1)
            
            with sqlite3.connect(self.db_path) as conn:
                # Count total operations
                cursor = conn.execute("""
                    SELECT COUNT(*) FROM scheduler_checkpoints
                    WHERE run_timestamp >= ?
                """, (cutoff.isoformat(),))
                total_ops = cursor.fetchone()[0]
                
                if total_ops == 0:
                    return 0.0
                
                # Count failed operations
                cursor = conn.execute("""
                    SELECT COUNT(*) FROM scheduler_checkpoints
                    WHERE run_timestamp >= ? AND status = 'failed'
                """, (cutoff.isoformat(),))
                failed_ops = cursor.fetchone()[0]
                
                return failed_ops / total_ops
        except Exception as e:
            logger.error(f"Error calculating error rate: {e}")
            return 0.0
    
    def _get_total_contacts(self) -> int:
        """Get total number of contacts"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute("SELECT COUNT(*) FROM contacts")
                return cursor.fetchone()[0]
        except Exception:
            return 0
    
    def _get_processed_contacts_24h(self) -> int:
        """Get contacts processed in last 24 hours"""
        try:
            cutoff = datetime.now() - timedelta(days=1)
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute("""
                    SELECT COALESCE(SUM(contacts_processed), 0) 
                    FROM scheduler_checkpoints
                    WHERE run_timestamp >= ?
                """, (cutoff.isoformat(),))
                return cursor.fetchone()[0] or 0
        except Exception:
            return 0
    
    def _get_scheduled_emails_24h(self) -> int:
        """Get emails scheduled in last 24 hours"""
        try:
            cutoff = datetime.now() - timedelta(days=1)
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute("""
                    SELECT COUNT(*) FROM email_schedules
                    WHERE created_at >= ? AND status = 'pre-scheduled'
                """, (cutoff.isoformat(),))
                return cursor.fetchone()[0]
        except Exception:
            return 0
    
    def _get_skipped_emails_24h(self) -> int:
        """Get emails skipped in last 24 hours"""
        try:
            cutoff = datetime.now() - timedelta(days=1)
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute("""
                    SELECT COUNT(*) FROM email_schedules
                    WHERE created_at >= ? AND status = 'skipped'
                """, (cutoff.isoformat(),))
                return cursor.fetchone()[0]
        except Exception:
            return 0
    
    def _get_failed_operations_24h(self) -> int:
        """Get failed operations in last 24 hours"""
        try:
            cutoff = datetime.now() - timedelta(days=1)
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute("""
                    SELECT COUNT(*) FROM scheduler_checkpoints
                    WHERE run_timestamp >= ? AND status = 'failed'
                """, (cutoff.isoformat(),))
                return cursor.fetchone()[0]
        except Exception:
            return 0
    
    def _get_avg_daily_volume(self) -> float:
        """Get average daily email volume over last 7 days"""
        try:
            cutoff = datetime.now() - timedelta(days=7)
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute("""
                    SELECT AVG(daily_count) FROM (
                        SELECT DATE(scheduled_send_date) as send_date, COUNT(*) as daily_count
                        FROM email_schedules
                        WHERE scheduled_send_date >= ?
                        AND status IN ('pre-scheduled', 'scheduled', 'sent')
                        GROUP BY DATE(scheduled_send_date)
                    )
                """, (cutoff.date().isoformat(),))
                result = cursor.fetchone()[0]
                return result or 0.0
        except Exception:
            return 0.0
    
    def _get_max_daily_volume(self) -> int:
        """Get maximum daily email volume over last 7 days"""
        try:
            cutoff = datetime.now() - timedelta(days=7)
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute("""
                    SELECT MAX(daily_count) FROM (
                        SELECT DATE(scheduled_send_date) as send_date, COUNT(*) as daily_count
                        FROM email_schedules
                        WHERE scheduled_send_date >= ?
                        AND status IN ('pre-scheduled', 'scheduled', 'sent')
                        GROUP BY DATE(scheduled_send_date)
                    )
                """, (cutoff.date().isoformat(),))
                result = cursor.fetchone()[0]
                return result or 0
        except Exception:
            return 0
    
    def _get_load_balancing_count_24h(self) -> int:
        """Get count of load balancing applications in last 24 hours"""
        # This would need to be tracked in metadata when load balancing is applied
        # For now, return 0 as placeholder
        return 0
    
    def _get_smoothing_count_24h(self) -> int:
        """Get count of effective date smoothing applications in last 24 hours"""
        # This would need to be tracked in metadata when smoothing is applied
        # For now, return 0 as placeholder
        return 0
    
    def _get_db_size_mb(self) -> float:
        """Get database size in MB"""
        try:
            import os
            size_bytes = os.path.getsize(self.db_path)
            return size_bytes / (1024 * 1024)
        except Exception:
            return 0.0
    
    def _calculate_avg_query_time(self) -> float:
        """Calculate average query time (placeholder implementation)"""
        # In a real implementation, this would track query execution times
        # For now, perform a simple benchmark query
        try:
            start_time = time.time()
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute("SELECT COUNT(*) FROM contacts")
                cursor.fetchone()
            end_time = time.time()
            return (end_time - start_time) * 1000  # Convert to milliseconds
        except Exception:
            return 0.0
    
    def _get_active_connections(self) -> int:
        """Get number of active database connections"""
        # SQLite doesn't have persistent connections like other databases
        # This is more relevant for PostgreSQL/MySQL
        return 1 if self.check_db_connection() else 0
    
    def _calculate_index_efficiency(self) -> float:
        """Calculate index usage efficiency"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                # Check if critical indexes exist
                cursor = conn.execute("""
                    SELECT name FROM sqlite_master 
                    WHERE type = 'index' 
                    AND name LIKE 'idx_%'
                """)
                indexes = cursor.fetchall()
                
                # Expected critical indexes
                expected_indexes = [
                    'idx_contacts_state_birthday',
                    'idx_contacts_state_effective',
                    'idx_schedules_lookup',
                    'idx_schedules_contact_period'
                ]
                
                existing_count = len([idx for idx in indexes if idx[0] in expected_indexes])
                return existing_count / len(expected_indexes)
        except Exception:
            return 0.0
    
    def _get_followups_24h(self) -> int:
        """Get follow-ups scheduled in last 24 hours"""
        try:
            cutoff = datetime.now() - timedelta(days=1)
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute("""
                    SELECT COUNT(*) FROM email_schedules
                    WHERE created_at >= ? 
                    AND email_type LIKE 'followup_%'
                """, (cutoff.isoformat(),))
                return cursor.fetchone()[0]
        except Exception:
            return 0
    
    def _get_behavior_analysis_rate(self) -> float:
        """Get success rate of behavior analysis for follow-ups"""
        try:
            cutoff = datetime.now() - timedelta(days=1)
            with sqlite3.connect(self.db_path) as conn:
                # Count total follow-up processing attempts
                cursor = conn.execute("""
                    SELECT COUNT(*) FROM followup_processing_log
                    WHERE processed_at >= ?
                """, (cutoff.isoformat(),))
                total = cursor.fetchone()[0]
                
                if total == 0:
                    return 1.0
                
                # Count successful analyses (those with behavior data)
                cursor = conn.execute("""
                    SELECT COUNT(*) FROM followup_processing_log
                    WHERE processed_at >= ? 
                    AND behavior_analysis IS NOT NULL
                    AND behavior_analysis != ''
                """, (cutoff.isoformat(),))
                successful = cursor.fetchone()[0]
                
                return successful / total
        except Exception:
            return 0.0
    
    def _get_exclusion_hits_24h(self) -> int:
        """Get count of exclusion window hits in last 24 hours"""
        try:
            cutoff = datetime.now() - timedelta(days=1)
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute("""
                    SELECT COUNT(*) FROM email_schedules
                    WHERE created_at >= ? 
                    AND status = 'skipped'
                    AND skip_reason LIKE '%exclusion%'
                """, (cutoff.isoformat(),))
                return cursor.fetchone()[0]
        except Exception:
            return 0
    
    def _calculate_compliance_rate(self) -> float:
        """Calculate state compliance rate"""
        try:
            cutoff = datetime.now() - timedelta(days=7)
            with sqlite3.connect(self.db_path) as conn:
                # Count total scheduling decisions
                cursor = conn.execute("""
                    SELECT COUNT(*) FROM email_schedules
                    WHERE created_at >= ?
                """, (cutoff.isoformat(),))
                total = cursor.fetchone()[0]
                
                if total == 0:
                    return 1.0
                
                # Count compliant decisions (either scheduled or properly skipped)
                cursor = conn.execute("""
                    SELECT COUNT(*) FROM email_schedules
                    WHERE created_at >= ?
                    AND (
                        status = 'pre-scheduled' 
                        OR (status = 'skipped' AND skip_reason IS NOT NULL)
                    )
                """, (cutoff.isoformat(),))
                compliant = cursor.fetchone()[0]
                
                return compliant / total
        except Exception:
            return 0.0
    
    def get_system_summary(self) -> Dict[str, Any]:
        """Get a comprehensive system summary"""
        health = self.get_health_status()
        metrics = self.get_metrics()
        
        return {
            "health_status": health.to_dict(),
            "metrics": metrics.to_dict(),
            "system_info": {
                "uptime_seconds": (datetime.now() - self.start_time).total_seconds(),
                "database_path": self.db_path,
                "monitoring_timestamp": datetime.now().isoformat()
            }
        }

# ============================================================================
# COMMAND LINE INTERFACE
# ============================================================================

def main():
    """Main entry point for health monitoring"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Email Scheduler Health Monitor')
    parser.add_argument('--db', required=True, help='SQLite database path')
    parser.add_argument('--health', action='store_true', help='Show health status')
    parser.add_argument('--metrics', action='store_true', help='Show detailed metrics')
    parser.add_argument('--summary', action='store_true', help='Show complete system summary')
    parser.add_argument('--json', action='store_true', help='Output in JSON format')
    
    args = parser.parse_args()
    
    monitor = HealthMonitor(args.db)
    
    if args.summary or (not args.health and not args.metrics):
        data = monitor.get_system_summary()
    elif args.health:
        data = monitor.get_health_status().to_dict()
    elif args.metrics:
        data = monitor.get_metrics().to_dict()
    
    if args.json:
        print(json.dumps(data, indent=2))
    else:
        # Pretty print for human consumption
        if args.health or (not args.metrics and not args.summary):
            health = data if args.health else data['health_status']
            print(f"System Status: {health['status'].upper()}")
            print(f"Database Connected: {health['database_connected']}")
            print(f"Last Successful Run: {health['last_successful_run'] or 'Never'}")
            print(f"Pending Schedules: {health['pending_schedules']}")
            print(f"Error Rate (24h): {health['error_rate_24h']:.2%}")
            if health['issues']:
                print("Issues:")
                for issue in health['issues']:
                    print(f"  - {issue}")
        
        if args.metrics or args.summary:
            metrics = data if args.metrics else data['metrics']
            print(f"\nSystem Metrics (as of {metrics['timestamp']}):")
            print(f"Total Contacts: {metrics['total_contacts']:,}")
            print(f"Processed (24h): {metrics['processed_contacts_24h']:,}")
            print(f"Scheduled (24h): {metrics['scheduled_emails_24h']:,}")
            print(f"Skipped (24h): {metrics['skipped_emails_24h']:,}")
            print(f"Avg Daily Volume: {metrics['avg_daily_email_volume']:.1f}")
            print(f"Database Size: {metrics['db_size_mb']:.1f} MB")
            print(f"Index Efficiency: {metrics['index_efficiency']:.1%}")

if __name__ == '__main__':
    main()