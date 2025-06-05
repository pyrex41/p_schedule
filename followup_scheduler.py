#!/usr/bin/env python3
"""
Follow-up Email Scheduler

This module handles scheduling of follow-up emails based on user behavior
and engagement with initial emails. It implements the business logic for
determining the appropriate follow-up type based on user interactions.
"""

import sqlite3
import json
import logging
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
from scheduler import (
    EmailType, EmailStatus, SchedulingConfig, StateRulesEngine, 
    Contact, DatabaseManager
)

logger = logging.getLogger(__name__)

# ============================================================================
# FOLLOW-UP BEHAVIOR ANALYSIS
# ============================================================================

@dataclass
class FollowupBehavior:
    """Data structure for tracking user behavior and follow-up decisions"""
    contact_id: int
    initial_email_id: int
    initial_email_type: str
    has_clicked: bool = False
    has_answered_hq: bool = False
    has_medical_conditions: bool = False
    followup_type: str = "followup_1_cold"
    decision_reason: str = "no_engagement"
    metadata: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}

# ============================================================================
# FOLLOW-UP EMAIL SCHEDULER
# ============================================================================

class FollowupEmailScheduler:
    """Handles scheduling of follow-up emails based on user behavior"""
    
    def __init__(self, config: SchedulingConfig, state_rules: StateRulesEngine, db_path: str):
        self.config = config
        self.state_rules = state_rules
        self.db_path = db_path
        self.db_manager = DatabaseManager(db_path)
        
        # Ensure required tables exist
        self._ensure_followup_tables()
    
    def _ensure_followup_tables(self):
        """Ensure required tables for follow-up tracking exist"""
        with sqlite3.connect(self.db_path) as conn:
            # Create tracking_clicks table if it doesn't exist
            conn.execute("""
                CREATE TABLE IF NOT EXISTS tracking_clicks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    contact_id INTEGER NOT NULL,
                    email_schedule_id INTEGER,
                    clicked_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    url TEXT,
                    ip_address TEXT,
                    user_agent TEXT,
                    FOREIGN KEY (contact_id) REFERENCES contacts(id),
                    FOREIGN KEY (email_schedule_id) REFERENCES email_schedules(id)
                )
            """)
            
            # Create contact_events table if it doesn't exist
            conn.execute("""
                CREATE TABLE IF NOT EXISTS contact_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    contact_id INTEGER NOT NULL,
                    event_type TEXT NOT NULL,
                    event_data TEXT,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (contact_id) REFERENCES contacts(id)
                )
            """)
            
            # Create follow-up processing log
            conn.execute("""
                CREATE TABLE IF NOT EXISTS followup_processing_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    scheduler_run_id TEXT NOT NULL,
                    initial_email_id INTEGER NOT NULL,
                    contact_id INTEGER NOT NULL,
                    followup_type TEXT NOT NULL,
                    behavior_analysis TEXT,
                    processed_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (initial_email_id) REFERENCES email_schedules(id),
                    FOREIGN KEY (contact_id) REFERENCES contacts(id),
                    UNIQUE(initial_email_id, contact_id)
                )
            """)
    
    def schedule_followup_emails(self, lookback_days: int = 35) -> int:
        """Main entry point for follow-up scheduling"""
        logger.info(f"Starting follow-up email scheduling (lookback: {lookback_days} days)")
        
        # Generate unique run ID for this follow-up scheduling session
        import uuid
        scheduler_run_id = f"followup_{uuid.uuid4()}"
        
        # Get eligible initial emails
        eligible_emails = self._get_eligible_initial_emails(lookback_days)
        logger.info(f"Found {len(eligible_emails)} eligible initial emails for follow-up")
        
        if not eligible_emails:
            return 0
        
        # Process follow-ups in batches
        scheduled_count = 0
        batch_size = 1000
        
        for i in range(0, len(eligible_emails), batch_size):
            batch = eligible_emails[i:i + batch_size]
            logger.info(f"Processing follow-up batch {i//batch_size + 1}: {len(batch)} emails")
            
            followup_schedules = []
            
            for email_data in batch:
                try:
                    # Analyze behavior and determine follow-up type
                    behavior = self._analyze_contact_behavior(email_data)
                    
                    # Create follow-up schedule
                    followup_schedule = self._create_followup_schedule(
                        email_data, behavior, scheduler_run_id
                    )
                    
                    if followup_schedule:
                        followup_schedules.append(followup_schedule)
                        
                        # Log the processing
                        self._log_followup_processing(email_data, behavior, scheduler_run_id)
                
                except Exception as e:
                    logger.error(f"Error processing follow-up for email {email_data['id']}: {e}")
                    continue
            
            # Insert follow-up schedules
            if followup_schedules:
                self.db_manager.batch_insert_schedules_transactional(followup_schedules)
                scheduled_count += len(followup_schedules)
        
        logger.info(f"Follow-up scheduling complete: {scheduled_count} follow-ups scheduled")
        return scheduled_count
    
    def _get_eligible_initial_emails(self, lookback_days: int) -> List[Dict[str, Any]]:
        """Get initial emails eligible for follow-up scheduling"""
        lookback_date = date.today() - timedelta(days=lookback_days)
        
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            
            # Query for sent/delivered initial emails
            cursor = conn.execute("""
                SELECT es.id, es.contact_id, es.email_type, es.scheduled_send_date,
                       es.campaign_instance_id, es.priority, es.event_year, 
                       es.event_month, es.event_day, c.state, c.birth_date, c.effective_date
                FROM email_schedules es
                JOIN contacts c ON es.contact_id = c.id
                WHERE es.status IN ('sent', 'delivered')
                AND es.scheduled_send_date >= ?
                AND (
                    -- Anniversary-based emails
                    es.email_type IN ('birthday', 'effective_date', 'aep', 'post_window')
                    OR 
                    -- Campaign-based emails with followups enabled
                    (es.email_type LIKE 'campaign_%' AND es.campaign_instance_id IN (
                        SELECT ci.id FROM campaign_instances ci
                        JOIN campaign_types ct ON ci.campaign_type = ct.name
                        WHERE ct.enable_followups = TRUE
                    ))
                )
                AND es.contact_id NOT IN (
                    -- Exclude contacts with existing follow-ups
                    SELECT DISTINCT contact_id 
                    FROM email_schedules 
                    WHERE email_type LIKE 'followup_%'
                    AND status IN ('pre-scheduled', 'scheduled', 'sent', 'delivered')
                )
                ORDER BY es.scheduled_send_date DESC
            """, (lookback_date.isoformat(),))
            
            return [dict(row) for row in cursor.fetchall()]
    
    def _analyze_contact_behavior(self, email_data: Dict[str, Any]) -> FollowupBehavior:
        """Analyze contact behavior to determine appropriate follow-up type"""
        contact_id = email_data['contact_id']
        email_id = email_data['id']
        
        behavior = FollowupBehavior(
            contact_id=contact_id,
            initial_email_id=email_id,
            initial_email_type=email_data['email_type']
        )
        
        with sqlite3.connect(self.db_path) as conn:
            # Check for clicks
            click_cursor = conn.execute("""
                SELECT COUNT(*) FROM tracking_clicks 
                WHERE contact_id = ? AND email_schedule_id = ?
            """, (contact_id, email_id))
            
            click_count = click_cursor.fetchone()[0]
            behavior.has_clicked = click_count > 0
            
            if behavior.has_clicked:
                behavior.metadata['click_count'] = click_count
            
            # Check for health question responses
            hq_cursor = conn.execute("""
                SELECT event_data FROM contact_events 
                WHERE contact_id = ? 
                AND event_type = 'eligibility_answered'
                AND created_at >= ?
                ORDER BY created_at DESC
                LIMIT 1
            """, (contact_id, email_data['scheduled_send_date']))
            
            hq_row = hq_cursor.fetchone()
            if hq_row:
                behavior.has_answered_hq = True
                
                # Parse health question data
                try:
                    event_data = json.loads(hq_row[0]) if hq_row[0] else {}
                    
                    # Check for medical conditions
                    has_conditions = (
                        event_data.get('has_medical_conditions', False) or
                        event_data.get('main_questions_yes_count', 0) > 0
                    )
                    behavior.has_medical_conditions = has_conditions
                    behavior.metadata['hq_data'] = event_data
                    
                except (json.JSONDecodeError, KeyError) as e:
                    logger.warning(f"Error parsing health question data for contact {contact_id}: {e}")
        
        # Determine follow-up type based on behavior hierarchy
        behavior.followup_type, behavior.decision_reason = self._determine_followup_type(behavior)
        
        return behavior
    
    def _determine_followup_type(self, behavior: FollowupBehavior) -> Tuple[str, str]:
        """Determine the appropriate follow-up type based on user behavior"""
        
        # Priority order: Medical conditions > Healthy > Clicked > Cold
        if behavior.has_answered_hq:
            if behavior.has_medical_conditions:
                return "followup_4_hq_with_yes", "answered_hq_with_conditions"
            else:
                return "followup_3_hq_no_yes", "answered_hq_no_conditions"
        elif behavior.has_clicked:
            return "followup_2_clicked_no_hq", "clicked_no_health_questions"
        else:
            return "followup_1_cold", "no_engagement"
    
    def _create_followup_schedule(self, email_data: Dict[str, Any], 
                                behavior: FollowupBehavior, scheduler_run_id: str) -> Optional[Dict[str, Any]]:
        """Create a follow-up email schedule based on behavior analysis"""
        
        # Calculate follow-up send date
        initial_send_date = datetime.strptime(email_data['scheduled_send_date'], '%Y-%m-%d').date()
        followup_send_date = initial_send_date + timedelta(days=self.config.followup_timing.days_before_event * -1)  # Convert to days after
        
        # If follow-up date is in the past, schedule for tomorrow
        if followup_send_date <= date.today():
            followup_send_date = date.today() + timedelta(days=1)
        
        # Get contact for state checking
        contact = self._get_contact_by_id(behavior.contact_id)
        if not contact:
            logger.warning(f"Contact {behavior.contact_id} not found for follow-up")
            return None
        
        # Check exclusion rules (follow-ups always respect exclusion windows)
        is_excluded, skip_reason = self.state_rules.is_date_excluded(
            contact.state, followup_send_date, contact.birthday, contact.effective_date
        )
        
        # Determine priority (inherit from parent campaign or use default)
        priority = email_data.get('priority', 10)
        
        # Prepare metadata with behavior analysis
        metadata = {
            'initial_email_id': behavior.initial_email_id,
            'initial_email_type': behavior.initial_email_type,
            'followup_behavior': {
                'has_clicked': behavior.has_clicked,
                'has_answered_hq': behavior.has_answered_hq,
                'has_medical_conditions': behavior.has_medical_conditions,
                'decision_reason': behavior.decision_reason
            },
            **behavior.metadata
        }
        
        # Add campaign information if applicable
        campaign_instance_id = None
        if email_data.get('campaign_instance_id'):
            campaign_instance_id = email_data['campaign_instance_id']
            metadata['campaign_name'] = self._get_campaign_name(campaign_instance_id)
        
        return {
            'contact_id': behavior.contact_id,
            'email_type': behavior.followup_type,
            'scheduled_send_date': followup_send_date.isoformat(),
            'scheduled_send_time': self.config.send_time,
            'status': EmailStatus.SKIPPED.value if is_excluded else EmailStatus.PRE_SCHEDULED.value,
            'skip_reason': skip_reason,
            'priority': priority,
            'campaign_instance_id': campaign_instance_id,
            'scheduler_run_id': scheduler_run_id,
            'event_year': email_data.get('event_year'),
            'event_month': email_data.get('event_month'),
            'event_day': email_data.get('event_day'),
            'metadata': json.dumps(metadata)
        }
    
    def _get_contact_by_id(self, contact_id: int) -> Optional[Contact]:
        """Get contact data by ID"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("""
                SELECT id, first_name, last_name, email, zip_code, state, 
                       birth_date, effective_date, phone_number
                FROM contacts 
                WHERE id = ?
            """, (contact_id,))
            
            row = cursor.fetchone()
            if row:
                return Contact.from_db_row(dict(row))
            return None
    
    def _get_campaign_name(self, campaign_instance_id: int) -> Optional[str]:
        """Get campaign name for metadata tracking"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("""
                SELECT ci.instance_name, ct.name as campaign_type
                FROM campaign_instances ci
                JOIN campaign_types ct ON ci.campaign_type = ct.name
                WHERE ci.id = ?
            """, (campaign_instance_id,))
            
            row = cursor.fetchone()
            if row:
                return f"{row[1]}_{row[0]}"  # campaign_type_instance_name
            return None
    
    def _log_followup_processing(self, email_data: Dict[str, Any], 
                               behavior: FollowupBehavior, scheduler_run_id: str):
        """Log follow-up processing for audit purposes"""
        with sqlite3.connect(self.db_path) as conn:
            behavior_summary = {
                'followup_type': behavior.followup_type,
                'decision_reason': behavior.decision_reason,
                'has_clicked': behavior.has_clicked,
                'has_answered_hq': behavior.has_answered_hq,
                'has_medical_conditions': behavior.has_medical_conditions
            }
            
            conn.execute("""
                INSERT OR IGNORE INTO followup_processing_log
                (scheduler_run_id, initial_email_id, contact_id, followup_type, behavior_analysis)
                VALUES (?, ?, ?, ?, ?)
            """, (
                scheduler_run_id,
                behavior.initial_email_id,
                behavior.contact_id,
                behavior.followup_type,
                json.dumps(behavior_summary)
            ))

# ============================================================================
# COMMAND LINE INTERFACE
# ============================================================================

def main():
    """Main entry point for follow-up scheduler"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Follow-up Email Scheduler')
    parser.add_argument('--db', required=True, help='SQLite database path')
    parser.add_argument('--config', help='Configuration YAML path')
    parser.add_argument('--lookback-days', type=int, default=35, help='Days to look back for initial emails')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    
    args = parser.parse_args()
    
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Import configuration
    from scheduler import SchedulingConfig, StateRulesEngine
    
    config = SchedulingConfig.from_yaml(args.config) if args.config else SchedulingConfig()
    state_rules = StateRulesEngine()
    
    # Initialize and run follow-up scheduler
    followup_scheduler = FollowupEmailScheduler(config, state_rules, args.db)
    scheduled_count = followup_scheduler.schedule_followup_emails(args.lookback_days)
    
    print(f"Follow-up scheduling complete: {scheduled_count} follow-ups scheduled")

if __name__ == '__main__':
    main()