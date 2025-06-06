#!/usr/bin/env python3
"""
Email Scheduling System - Domain-Specific Business Logic Implementation

This scheduler implements the comprehensive email scheduling business logic
for multi-state compliance with anniversary-based and campaign-based emails.
"""

import sqlite3
import json
import hashlib
import logging
from datetime import datetime, date, timedelta
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple, Any, Union
from enum import Enum
import uuid
import yaml
from pathlib import Path
import time

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ============================================================================
# DOMAIN-SPECIFIC LANGUAGE (DSL) FOR EMAIL SCHEDULING RULES
# ============================================================================

class EmailType(Enum):
    """Email types supported by the scheduler"""
    BIRTHDAY = "birthday"
    EFFECTIVE_DATE = "effective_date"
    AEP = "aep"
    POST_WINDOW = "post_window"
    CAMPAIGN = "campaign"
    FOLLOWUP_1_COLD = "followup_1_cold"
    FOLLOWUP_2_CLICKED_NO_HQ = "followup_2_clicked_no_hq"
    FOLLOWUP_3_HQ_NO_YES = "followup_3_hq_no_yes"
    FOLLOWUP_4_HQ_WITH_YES = "followup_4_hq_with_yes"

class EmailStatus(Enum):
    """Email schedule status values"""
    PRE_SCHEDULED = "pre-scheduled"
    SKIPPED = "skipped"
    SCHEDULED = "scheduled"
    PROCESSING = "processing"
    SENT = "sent"
    DELIVERED = "delivered"
    FAILED = "failed"

class ExclusionRuleType(Enum):
    """Types of state exclusion rules"""
    BIRTHDAY_WINDOW = "birthday_window"
    EFFECTIVE_DATE_WINDOW = "effective_date_window"
    YEAR_ROUND = "year_round"

@dataclass
class TimingRule:
    """DSL for defining when emails should be sent"""
    days_before_event: int = 0  # Positive = before, negative = after, 0 = on date
    send_time: str = "08:30:00"  # Default send time in CT
    
    def calculate_send_date(self, event_date: date) -> date:
        """Calculate actual send date based on timing rule"""
        return event_date - timedelta(days=self.days_before_event)

@dataclass
class ExclusionWindow:
    """DSL for defining state-specific exclusion windows"""
    rule_type: ExclusionRuleType
    window_before_days: int = 0
    window_after_days: int = 0
    use_month_start: bool = False  # For Nevada birthday rules
    age_76_plus_exclusion: bool = False  # Future use
    pre_window_extension_days: int = 60  # Standard pre-window exclusion
    
    def get_exclusion_period(self, anchor_date: date, birth_date: Optional[date] = None) -> Tuple[date, date]:
        """Calculate the full exclusion period including pre-window extension"""
        if self.rule_type == ExclusionRuleType.YEAR_ROUND:
            # Year-round exclusion
            return date(1900, 1, 1), date(2100, 12, 31)
        
        # Handle Nevada special case
        if self.use_month_start and anchor_date:
            anchor_date = anchor_date.replace(day=1)
        
        # Calculate base window
        start_date = anchor_date - timedelta(days=self.window_before_days)
        end_date = anchor_date + timedelta(days=self.window_after_days)
        
        # Apply pre-window extension
        extended_start = start_date - timedelta(days=self.pre_window_extension_days)
        
        return extended_start, end_date

@dataclass
class StateRule:
    """DSL for comprehensive state-specific email rules"""
    state_code: str
    exclusion_windows: List[ExclusionWindow] = field(default_factory=list)
    
    def is_date_excluded(self, check_date: date, contact_birthday: Optional[date] = None, 
                        contact_effective_date: Optional[date] = None) -> Tuple[bool, Optional[str]]:
        """Check if a date falls within any exclusion window for this state"""
        current_year = datetime.now().year
        
        for window in self.exclusion_windows:
            if window.rule_type == ExclusionRuleType.YEAR_ROUND:
                return True, f"Year-round exclusion in {self.state_code}"
            
            elif window.rule_type == ExclusionRuleType.BIRTHDAY_WINDOW and contact_birthday:
                # Calculate this year's birthday anniversary
                try:
                    this_year_birthday = contact_birthday.replace(year=current_year)
                except ValueError:  # Feb 29 in non-leap year
                    this_year_birthday = contact_birthday.replace(year=current_year, month=2, day=28)
                
                start_date, end_date = window.get_exclusion_period(this_year_birthday, contact_birthday)
                if start_date <= check_date <= end_date:
                    return True, f"Birthday exclusion window in {self.state_code}"
            
            elif window.rule_type == ExclusionRuleType.EFFECTIVE_DATE_WINDOW and contact_effective_date:
                # Calculate this year's effective date anniversary
                try:
                    this_year_effective = contact_effective_date.replace(year=current_year)
                except ValueError:  # Feb 29 in non-leap year
                    this_year_effective = contact_effective_date.replace(year=current_year, month=2, day=28)
                
                start_date, end_date = window.get_exclusion_period(this_year_effective)
                if start_date <= check_date <= end_date:
                    return True, f"Effective date exclusion window in {self.state_code}"
        
        return False, None

@dataclass
class CampaignType:
    """DSL for defining reusable campaign behavior patterns"""
    name: str
    respect_exclusion_windows: bool = True
    enable_followups: bool = True
    timing_rule: TimingRule = field(default_factory=TimingRule)
    target_all_contacts: bool = False
    priority: int = 10  # Lower numbers = higher priority
    active: bool = True

@dataclass
class CampaignInstance:
    """DSL for specific campaign executions with templates and timing"""
    id: Optional[int]
    campaign_type: str
    instance_name: str
    email_template: Optional[str] = None
    sms_template: Optional[str] = None
    active_start_date: Optional[date] = None
    active_end_date: Optional[date] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

@dataclass
class ContactCampaign:
    """DSL for linking contacts to specific campaign instances"""
    contact_id: int
    campaign_instance_id: int
    trigger_date: date
    status: str = "pending"
    metadata: Dict[str, Any] = field(default_factory=dict)

@dataclass
class LoadBalancingConfig:
    """DSL for load balancing and smoothing configuration"""
    daily_send_percentage_cap: float = 0.07  # 7% of org contacts per day
    ed_daily_soft_limit: int = 15  # Soft cap for effective date emails
    ed_smoothing_window_days: int = 5  # ±2 days for ED smoothing
    catch_up_spread_days: int = 7  # Window for catch-up distribution
    overage_threshold: float = 1.2  # 120% triggers redistribution
    max_emails_per_contact_per_period: int = 2  # Max emails per period
    period_days: int = 14  # Period for frequency checking

@dataclass
class SchedulingConfig:
    """DSL for overall scheduling system configuration"""
    send_time: str = "08:30:00"
    batch_size: int = 10000
    birthday_email_timing: TimingRule = field(default_factory=lambda: TimingRule(days_before_event=14))
    effective_date_timing: TimingRule = field(default_factory=lambda: TimingRule(days_before_event=30))
    aep_dates: List[Dict[str, int]] = field(default_factory=lambda: [{"month": 9, "day": 15}])
    followup_timing: TimingRule = field(default_factory=lambda: TimingRule(days_before_event=-2))  # 2 days after
    load_balancing: LoadBalancingConfig = field(default_factory=LoadBalancingConfig)
    
    @classmethod
    def from_yaml(cls, yaml_path: str) -> 'SchedulingConfig':
        """Load configuration from YAML file"""
        if Path(yaml_path).exists():
            with open(yaml_path, 'r') as f:
                data = yaml.safe_load(f)
                
                # Extract and transform YAML structure to match dataclass
                config = cls()
                
                # Timing constants
                if 'timing_constants' in data:
                    timing = data['timing_constants']
                    config.send_time = timing.get('send_time', config.send_time)
                    config.birthday_email_timing = TimingRule(
                        days_before_event=timing.get('birthday_email_days_before', 14)
                    )
                    config.effective_date_timing = TimingRule(
                        days_before_event=timing.get('effective_date_days_before', 30)
                    )
                    config.followup_timing = TimingRule(
                        days_before_event=-timing.get('followup_days_after', 2)
                    )
                
                # Processing configuration
                if 'processing' in data:
                    processing = data['processing']
                    config.batch_size = processing.get('batch_size', config.batch_size)
                
                # AEP configuration
                if 'aep_config' in data:
                    aep = data['aep_config']
                    config.aep_dates = aep.get('default_dates', config.aep_dates)
                
                # Load balancing configuration
                if 'load_balancing' in data:
                    lb = data['load_balancing']
                    config.load_balancing = LoadBalancingConfig(
                        daily_send_percentage_cap=lb.get('daily_send_percentage_cap', 0.07),
                        ed_daily_soft_limit=lb.get('ed_daily_soft_limit', 15),
                        ed_smoothing_window_days=lb.get('ed_smoothing_window_days', 5),
                        catch_up_spread_days=lb.get('catch_up_spread_days', 7),
                        overage_threshold=lb.get('overage_threshold', 1.2),
                        max_emails_per_contact_per_period=lb.get('max_emails_per_contact_per_period', 2),
                        period_days=lb.get('period_days', 14)
                    )
                
                return config
        return cls()

# ============================================================================
# STATE RULES ENGINE - DECLARATIVE STATE-SPECIFIC RULES
# ============================================================================

class StateRulesEngine:
    """Engine for managing state-specific exclusion rules using DSL"""
    
    def __init__(self):
        self.state_rules: Dict[str, StateRule] = {}
        self._load_default_rules()
    
    def _load_default_rules(self):
        """Load default state rules as defined in business logic"""
        
        # Year-round exclusion states
        year_round_states = ["CT", "MA", "NY", "WA"]
        for state in year_round_states:
            self.state_rules[state] = StateRule(
                state_code=state,
                exclusion_windows=[ExclusionWindow(rule_type=ExclusionRuleType.YEAR_ROUND)]
            )
        
        # Birthday window states
        birthday_rules = {
            "CA": ExclusionWindow(ExclusionRuleType.BIRTHDAY_WINDOW, 30, 60),
            "ID": ExclusionWindow(ExclusionRuleType.BIRTHDAY_WINDOW, 0, 63),
            "KY": ExclusionWindow(ExclusionRuleType.BIRTHDAY_WINDOW, 0, 60),
            "MD": ExclusionWindow(ExclusionRuleType.BIRTHDAY_WINDOW, 0, 30),
            "NV": ExclusionWindow(ExclusionRuleType.BIRTHDAY_WINDOW, 0, 60, use_month_start=True),
            "OK": ExclusionWindow(ExclusionRuleType.BIRTHDAY_WINDOW, 0, 60),
            "OR": ExclusionWindow(ExclusionRuleType.BIRTHDAY_WINDOW, 0, 31),
            "VA": ExclusionWindow(ExclusionRuleType.BIRTHDAY_WINDOW, 0, 30),
        }
        
        for state, rule in birthday_rules.items():
            self.state_rules[state] = StateRule(state_code=state, exclusion_windows=[rule])
        
        # Effective date window states
        self.state_rules["MO"] = StateRule(
            state_code="MO",
            exclusion_windows=[ExclusionWindow(ExclusionRuleType.EFFECTIVE_DATE_WINDOW, 30, 33)]
        )
    
    def get_state_rule(self, state_code: str) -> Optional[StateRule]:
        """Get state rule for a given state code"""
        if not state_code:
            return None
        return self.state_rules.get(state_code.upper())
    
    def is_date_excluded(self, state_code: str, check_date: date, 
                        contact_birthday: Optional[date] = None,
                        contact_effective_date: Optional[date] = None) -> Tuple[bool, Optional[str]]:
        """Check if a date is excluded for a contact in a specific state"""
        if not state_code:
            return False, "No state specified"
        
        rule = self.get_state_rule(state_code)
        if not rule:
            return False, None  # No rules = no exclusion
        
        return rule.is_date_excluded(check_date, contact_birthday, contact_effective_date)

# ============================================================================
# CONTACT PROCESSOR - HANDLES CONTACT DATA AND VALIDATION
# ============================================================================

@dataclass
class Contact:
    """Domain model for contact data"""
    id: int
    email: str
    zip_code: str
    state: str
    birthday: Optional[date] = None
    effective_date: Optional[date] = None
    first_name: str = ""
    last_name: str = ""
    phone_number: str = ""
    
    @classmethod
    def from_db_row(cls, row: Dict[str, Any]) -> Optional['Contact']:
        """Create contact from database row with validation"""
        try:
            # Parse dates
            birthday = None
            if row.get('birth_date'):
                birthday = datetime.strptime(row['birth_date'], '%Y-%m-%d').date()
            
            effective_date = None
            if row.get('effective_date'):
                effective_date = datetime.strptime(row['effective_date'], '%Y-%m-%d').date()
            
            return cls(
                id=row['id'],
                email=row['email'],
                zip_code=row.get('zip_code', ''),
                state=row.get('state', ''),
                birthday=birthday,
                effective_date=effective_date,
                first_name=row.get('first_name', ''),
                last_name=row.get('last_name', ''),
                phone_number=row.get('phone_number', '')
            )
        except (ValueError, KeyError) as e:
            logger.warning(f"Invalid contact data for ID {row.get('id')}: {e}")
            return None
    
    def is_valid(self) -> Tuple[bool, List[str]]:
        """Validate contact data for scheduling"""
        errors = []
        
        if not self.email:
            errors.append("Missing email address")
        
        if not self.zip_code:
            errors.append("Missing ZIP code")
        
        if not self.state:
            errors.append("Missing state")
        
        return len(errors) == 0, errors

# ============================================================================
# DATABASE MANAGER - HANDLES ALL DATABASE OPERATIONS
# ============================================================================

class DatabaseManager:
    """Manages all database operations for the scheduler"""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._ensure_schema()
    
    def _ensure_schema(self):
        """Ensure all required tables and columns exist"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("PRAGMA foreign_keys = ON")
            
            # Add missing columns to email_schedules if they don't exist
            cursor = conn.cursor()
            
            # Check existing columns
            cursor.execute("PRAGMA table_info(email_schedules)")
            existing_cols = {row[1] for row in cursor.fetchall()}
            
            required_columns = {
                'priority': 'INTEGER DEFAULT 10',
                'campaign_instance_id': 'INTEGER',
                'email_template': 'TEXT',
                'sms_template': 'TEXT',
                'scheduler_run_id': 'TEXT',
                'actual_send_datetime': 'TEXT',
                'metadata': 'TEXT'
            }
            
            for col_name, col_def in required_columns.items():
                if col_name not in existing_cols:
                    try:
                        conn.execute(f"ALTER TABLE email_schedules ADD COLUMN {col_name} {col_def}")
                        logger.info(f"Added column {col_name} to email_schedules")
                    except sqlite3.OperationalError as e:
                        logger.warning(f"Could not add column {col_name}: {e}")
            
            # Create campaign system tables
            self._create_campaign_tables(conn)
            
            # Create additional tracking tables
            self._create_tracking_tables(conn)
            
            # Create performance indexes
            self._create_performance_indexes(conn)
    
    def _create_campaign_tables(self, conn: sqlite3.Connection):
        """Create campaign system tables"""
        
        # Campaign types table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS campaign_types (
                name TEXT PRIMARY KEY,
                respect_exclusion_windows BOOLEAN DEFAULT TRUE,
                enable_followups BOOLEAN DEFAULT TRUE,
                days_before_event INTEGER DEFAULT 0,
                target_all_contacts BOOLEAN DEFAULT FALSE,
                priority INTEGER DEFAULT 10,
                active BOOLEAN DEFAULT TRUE,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Campaign instances table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS campaign_instances (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                campaign_type TEXT NOT NULL,
                instance_name TEXT NOT NULL,
                email_template TEXT,
                sms_template TEXT,
                active_start_date DATE,
                active_end_date DATE,
                metadata TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(campaign_type, instance_name),
                FOREIGN KEY (campaign_type) REFERENCES campaign_types(name)
            )
        """)
        
        # Contact campaigns table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS contact_campaigns (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                contact_id INTEGER NOT NULL,
                campaign_instance_id INTEGER NOT NULL,
                trigger_date DATE,
                status TEXT DEFAULT 'pending',
                metadata TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(contact_id, campaign_instance_id, trigger_date),
                FOREIGN KEY (campaign_instance_id) REFERENCES campaign_instances(id),
                FOREIGN KEY (contact_id) REFERENCES contacts(id)
            )
        """)
        
        # Campaign change log
        conn.execute("""
            CREATE TABLE IF NOT EXISTS campaign_change_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                campaign_instance_id INTEGER NOT NULL,
                field_changed TEXT NOT NULL,
                old_value TEXT,
                new_value TEXT,
                changed_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                changed_by TEXT,
                requires_rescheduling BOOLEAN DEFAULT TRUE,
                FOREIGN KEY (campaign_instance_id) REFERENCES campaign_instances(id)
            )
        """)
    
    def _create_tracking_tables(self, conn: sqlite3.Connection):
        """Create additional tracking and audit tables"""
        
        # Scheduler checkpoints for audit and recovery
        conn.execute("""
            CREATE TABLE IF NOT EXISTS scheduler_checkpoints (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_timestamp DATETIME NOT NULL,
                scheduler_run_id TEXT UNIQUE NOT NULL,
                contacts_checksum TEXT NOT NULL,
                schedules_before_checksum TEXT,
                schedules_after_checksum TEXT,
                contacts_processed INTEGER,
                emails_scheduled INTEGER,
                emails_skipped INTEGER,
                status TEXT NOT NULL,
                error_message TEXT,
                completed_at DATETIME
            )
        """)
        
        # Configuration versions for tracking config changes
        conn.execute("""
            CREATE TABLE IF NOT EXISTS config_versions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                config_type TEXT NOT NULL,
                config_data TEXT NOT NULL,
                valid_from DATETIME NOT NULL,
                valid_to DATETIME,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                created_by TEXT
            )
        """)
    
    def _create_performance_indexes(self, conn: sqlite3.Connection):
        """Create performance indexes for the scheduler"""
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_contacts_state_birthday ON contacts(state, birth_date)",
            "CREATE INDEX IF NOT EXISTS idx_contacts_state_effective ON contacts(state, effective_date)",
            "CREATE INDEX IF NOT EXISTS idx_campaigns_active ON campaign_instances(active_start_date, active_end_date)",
            "CREATE INDEX IF NOT EXISTS idx_schedules_lookup ON email_schedules(contact_id, email_type, scheduled_send_date)",
            "CREATE INDEX IF NOT EXISTS idx_schedules_contact_period ON email_schedules(contact_id, scheduled_send_date, status)",
            "CREATE INDEX IF NOT EXISTS idx_schedules_status_date ON email_schedules(status, scheduled_send_date)",
            "CREATE INDEX IF NOT EXISTS idx_schedules_run_id ON email_schedules(scheduler_run_id)",
            "CREATE INDEX IF NOT EXISTS idx_contact_campaigns_contact ON contact_campaigns(contact_id, status)",
            "CREATE INDEX IF NOT EXISTS idx_contact_campaigns_instance ON contact_campaigns(campaign_instance_id, status)",
            "CREATE INDEX IF NOT EXISTS idx_checkpoints_run_id ON scheduler_checkpoints(scheduler_run_id)",
            "CREATE INDEX IF NOT EXISTS idx_checkpoints_timestamp ON scheduler_checkpoints(run_timestamp)",
            # Add the new unique index for conflict resolution
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_email_schedules_unique_event ON email_schedules(contact_id, email_type, event_year)"
        ]
        
        for index_sql in indexes:
            try:
                conn.execute(index_sql)
                logger.debug(f"Created index: {index_sql.split('ON')[0].split('EXISTS')[1].strip()}")
            except sqlite3.OperationalError as e:
                logger.warning(f"Could not create index: {e}")
    
    def get_contacts_batch(self, offset: int = 0, limit: int = 10000) -> List[Contact]:
        """Get a batch of contacts for processing"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("""
                SELECT id, first_name, last_name, email, zip_code, state, 
                       birth_date, effective_date, phone_number
                FROM contacts 
                WHERE email IS NOT NULL AND email != ''
                ORDER BY id
                LIMIT ? OFFSET ?
            """, (limit, offset))
            
            contacts = []
            for row in cursor.fetchall():
                contact = Contact.from_db_row(dict(row))
                if contact and contact.is_valid()[0]:
                    contacts.append(contact)
                elif contact:
                    logger.warning(f"Invalid contact {contact.id}: {contact.is_valid()[1]}")
            
            return contacts
    
    def get_total_contact_count(self) -> int:
        """Get total number of valid contacts"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("""
                SELECT COUNT(*) FROM contacts 
                WHERE email IS NOT NULL AND email != ''
            """)
            return cursor.fetchone()[0]
    
    def clear_scheduled_emails(self, contact_ids: List[int], scheduler_run_id: str):
        """Clear pre-scheduled and skipped emails for contacts being reprocessed"""
        if not contact_ids:
            return
        
        placeholders = ','.join('?' * len(contact_ids))
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(f"""
                DELETE FROM email_schedules 
                WHERE contact_id IN ({placeholders})
                AND status IN ('pre-scheduled', 'skipped')
            """, contact_ids)
            
            logger.info(f"Cleared existing schedules for {len(contact_ids)} contacts")
    
    def get_active_campaign_instances(self, current_date: date) -> List[CampaignInstance]:
        """Get all active campaign instances for the current date"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("""
                SELECT ci.*, ct.respect_exclusion_windows, ct.enable_followups,
                       ct.days_before_event, ct.priority
                FROM campaign_instances ci
                JOIN campaign_types ct ON ci.campaign_type = ct.name
                WHERE ct.active = TRUE
                AND (ci.active_start_date IS NULL OR ci.active_start_date <= ?)
                AND (ci.active_end_date IS NULL OR ci.active_end_date >= ?)
            """, (current_date.isoformat(), current_date.isoformat()))
            
            instances = []
            for row in cursor.fetchall():
                row_dict = dict(row)
                instance = CampaignInstance(
                    id=row_dict['id'],
                    campaign_type=row_dict['campaign_type'],
                    instance_name=row_dict['instance_name'],
                    email_template=row_dict['email_template'],
                    sms_template=row_dict['sms_template'],
                    active_start_date=datetime.strptime(row_dict['active_start_date'], '%Y-%m-%d').date() if row_dict['active_start_date'] else None,
                    active_end_date=datetime.strptime(row_dict['active_end_date'], '%Y-%m-%d').date() if row_dict['active_end_date'] else None,
                    metadata=json.loads(row_dict['metadata']) if row_dict['metadata'] else {}
                )
                instances.append(instance)
            
            return instances
    
    def get_contact_campaigns(self, campaign_instance_ids: List[int]) -> List[ContactCampaign]:
        """Get contact campaign targeting data"""
        if not campaign_instance_ids:
            return []
        
        placeholders = ','.join('?' * len(campaign_instance_ids))
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(f"""
                SELECT contact_id, campaign_instance_id, trigger_date, status, metadata
                FROM contact_campaigns
                WHERE campaign_instance_id IN ({placeholders})
                AND status = 'pending'
            """, campaign_instance_ids)
            
            campaigns = []
            for row in cursor.fetchall():
                row_dict = dict(row)
                campaign = ContactCampaign(
                    contact_id=row_dict['contact_id'],
                    campaign_instance_id=row_dict['campaign_instance_id'],
                    trigger_date=datetime.strptime(row_dict['trigger_date'], '%Y-%m-%d').date(),
                    status=row_dict['status'],
                    metadata=json.loads(row_dict['metadata']) if row_dict['metadata'] else {}
                )
                campaigns.append(campaign)
            
            return campaigns
    
    def execute_with_retry(self, operation, max_attempts=3, backoff_base=2):
        """Execute database operation with retry and exponential backoff"""
        for attempt in range(max_attempts):
            try:
                return operation()
            except sqlite3.OperationalError as e:
                if attempt == max_attempts - 1:
                    logger.error(f"Database operation failed after {max_attempts} attempts: {e}")
                    raise
                sleep_time = backoff_base ** attempt
                logger.warning(f"Database retry {attempt + 1}/{max_attempts} after {sleep_time}s: {e}")
                time.sleep(sleep_time)
            except Exception as e:
                logger.error(f"Non-recoverable database error: {e}")
                raise
    
    def create_checkpoint(self, scheduler_run_id: str, contact_count: int) -> int:
        """Create a scheduler checkpoint for audit and recovery"""
        def _create():
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute("""
                    INSERT INTO scheduler_checkpoints 
                    (run_timestamp, scheduler_run_id, contacts_checksum, status, contacts_processed)
                    VALUES (?, ?, ?, ?, ?)
                """, (
                    datetime.now().isoformat(),
                    scheduler_run_id,
                    hashlib.md5(f"{contact_count}_{datetime.now()}".encode()).hexdigest(),
                    'started',
                    0
                ))
                return cursor.lastrowid
        
        return self.execute_with_retry(_create)
    
    def update_checkpoint(self, checkpoint_id: int, status: str, **kwargs):
        """Update checkpoint with completion status and metrics"""
        def _update():
            with sqlite3.connect(self.db_path) as conn:
                # Build dynamic update query
                set_clauses = ['status = ?']
                params = [status]
                
                for key, value in kwargs.items():
                    set_clauses.append(f"{key} = ?")
                    params.append(value)
                
                if status in ['completed', 'failed']:
                    set_clauses.append('completed_at = ?')
                    params.append(datetime.now().isoformat())
                
                params.append(checkpoint_id)
                
                query = f"UPDATE scheduler_checkpoints SET {', '.join(set_clauses)} WHERE id = ?"
                conn.execute(query, params)
        
        self.execute_with_retry(_update)
    
    def batch_insert_schedules_transactional(self, schedules: List[Dict[str, Any]]):
        """
        Batch insert/update email schedules with full transaction management using
        a robust ON CONFLICT DO UPDATE strategy.
        """
        if not schedules:
            return

        def _insert_update():
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                script_parts = ["BEGIN TRANSACTION;"]
                
                for s in schedules:
                    # Safely escape string values for SQL
                    def format_sql_literal(value):
                        if value is None: return "NULL"
                        if isinstance(value, (int, float, bool)): return str(value)
                        escaped = str(value).replace("'", "''")
                        return f"'{escaped}'"

                    sql = f"""
                        INSERT INTO email_schedules (
                            contact_id, email_type, event_year, event_month, event_day,
                            scheduled_send_date, scheduled_send_time, status, skip_reason, 
                            priority, campaign_instance_id, email_template, sms_template, 
                            scheduler_run_id, metadata
                        )
                        VALUES (
                            {s['contact_id']},
                            {format_sql_literal(s['email_type'])},
                            {format_sql_literal(s.get('event_year'))},
                            {format_sql_literal(s.get('event_month'))},
                            {format_sql_literal(s.get('event_day'))},
                            {format_sql_literal(s['scheduled_send_date'])},
                            {format_sql_literal(s['scheduled_send_time'])},
                            {format_sql_literal(s['status'])},
                            {format_sql_literal(s.get('skip_reason'))},
                            {s.get('priority', 10)},
                            {format_sql_literal(s.get('campaign_instance_id'))},
                            {format_sql_literal(s.get('email_template'))},
                            {format_sql_literal(s.get('sms_template'))},
                            {format_sql_literal(s['scheduler_run_id'])},
                            {format_sql_literal(s.get('metadata'))}
                        )
                        ON CONFLICT(contact_id, email_type, event_year) DO UPDATE SET
                            scheduled_send_date = excluded.scheduled_send_date,
                            scheduled_send_time = excluded.scheduled_send_time,
                            status              = excluded.status,
                            skip_reason         = excluded.skip_reason,
                            priority            = excluded.priority,
                            metadata            = excluded.metadata,
                            event_month         = excluded.event_month,
                            event_day           = excluded.event_day
                        WHERE email_schedules.status NOT IN ('sent', 'delivered', 'processing', 'accepted');
                    """
                    script_parts.append(sql)

                script_parts.append("COMMIT;")
                batch_script = "\n".join(script_parts)
                
                try:
                    cursor.executescript(batch_script)
                    logger.info(f"Transactionally inserted/updated {len(schedules)} email schedules")
                except Exception as e:
                    conn.rollback()
                    logger.error(f"Transaction rolled back due to error: {e}")
                    raise

        self.execute_with_retry(_insert_update)

    def get_contacts_in_scheduling_window(self, lookahead_days: int, lookback_days: int) -> List[Contact]:
        """
        Efficiently fetches contacts from the database who might need scheduled emails
        in the current active window using a single optimized query.
        """
        today = date.today()
        active_window_end = today + timedelta(days=lookahead_days)
        lookback_window_start = today - timedelta(days=lookback_days)
        
        # Format dates for SQL
        today_str = today.strftime("%m-%d")
        future_end_str = active_window_end.strftime("%m-%d")
        past_start_str = lookback_window_start.strftime("%m-%d")

        # This query is complex but highly performant. It finds contacts with an anniversary
        # (birthday or effective date) within the lookahead/lookback window.
        # It handles year rollovers correctly.
        sql = f"""
            SELECT id, email, zip_code, state, birth_date, effective_date, first_name, last_name, phone_number
            FROM contacts
            WHERE
                (
                    (strftime('%m-%d', birth_date) BETWEEN '{past_start_str}' AND '12-31') OR
                    (strftime('%m-%d', birth_date) BETWEEN '01-01' AND '{future_end_str}')
                )
                OR
                (
                    (strftime('%m-%d', effective_date) BETWEEN '{past_start_str}' AND '12-31') OR
                    (strftime('%m-%d', effective_date) BETWEEN '01-01' AND '{future_end_str}')
                )
        """
        
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(sql)
            contacts = [Contact.from_db_row(dict(row)) for row in cursor.fetchall() if row]
            logger.info(f"Found {len(contacts)} contacts with potential anniversary events in the scheduling window.")
            return contacts

# ============================================================================
# ANNIVERSARY EMAIL SCHEDULER - HANDLES BIRTHDAY, EFFECTIVE DATE, AEP
# ============================================================================

class AnniversaryEmailScheduler:
    """Handles scheduling of anniversary-based emails using DSL rules"""
    
    def __init__(self, config: SchedulingConfig, state_rules: StateRulesEngine):
        self.config = config
        self.state_rules = state_rules
    
    def schedule_all_anniversary_emails(self, contacts: List[Contact], scheduler_run_id: str) -> List[Dict[str, Any]]:
        """
        Generates all potential anniversary-based schedules for a pre-filtered list of contacts.
        """
        all_schedules = []
        for contact in contacts:
            schedules = self._generate_schedules_for_contact(contact, scheduler_run_id)
            all_schedules.extend(schedules)
        logger.info(f"Generated {len(all_schedules)} potential anniversary schedules.")
        return all_schedules

    def _generate_schedules_for_contact(self, contact: Contact, scheduler_run_id: str) -> List[Dict[str, Any]]:
        """Generate all anniversary-based emails for a single contact."""
        schedules = []
        current_year = date.today().year
        
        # Years to check: current and next year for forward planning
        years_to_check = [current_year, current_year + 1]
        
        for year in years_to_check:
            # Birthday emails
            if contact.birthday:
                birthday_schedule = self._schedule_birthday_email(contact, year, scheduler_run_id)
                if birthday_schedule:
                    schedules.append(birthday_schedule)
            
            # Effective date emails
            if contact.effective_date:
                ed_schedule = self._schedule_effective_date_email(contact, year, scheduler_run_id)
                if ed_schedule:
                    schedules.append(ed_schedule)
            
            # AEP emails
            aep_schedule = self._schedule_aep_email(contact, year, scheduler_run_id)
            if aep_schedule:
                schedules.append(aep_schedule)
        
        # Post-window emails (if any emails were skipped)
        skipped_schedules = [s for s in schedules if s['status'] == EmailStatus.SKIPPED.value]
        if skipped_schedules:
            post_window_schedule = self._schedule_post_window_email(contact, scheduler_run_id)
            if post_window_schedule:
                schedules.append(post_window_schedule)
        
        return schedules
    
    def _schedule_birthday_email(self, contact: Contact, year: int, scheduler_run_id: str) -> Optional[Dict[str, Any]]:
        """Schedule birthday email using DSL timing rules"""
        if not contact.birthday:
            return None
        
        # Calculate this year's birthday anniversary
        try:
            anniversary_date = contact.birthday.replace(year=year)
        except ValueError:  # Feb 29 in non-leap year
            anniversary_date = contact.birthday.replace(year=year, month=2, day=28)
        
        # If this year's anniversary has passed, use next year
        if anniversary_date < date.today():
            try:
                anniversary_date = contact.birthday.replace(year=year + 1)
            except ValueError:
                anniversary_date = contact.birthday.replace(year=year + 1, month=2, day=28)
        
        # Calculate send date using timing rule
        send_date = self.config.birthday_email_timing.calculate_send_date(anniversary_date)
        
        # Check exclusion rules
        is_excluded, skip_reason = self.state_rules.is_date_excluded(
            contact.state, send_date, contact.birthday, contact.effective_date
        )
        
        return {
            'contact_id': contact.id,
            'email_type': EmailType.BIRTHDAY.value,
            'scheduled_send_date': send_date.isoformat(),
            'scheduled_send_time': self.config.send_time,
            'status': EmailStatus.SKIPPED.value if is_excluded else EmailStatus.PRE_SCHEDULED.value,
            'skip_reason': skip_reason,
            'priority': 5,  # High priority for anniversary emails
            'scheduler_run_id': scheduler_run_id,
            'event_year': anniversary_date.year,
            'event_month': anniversary_date.month,
            'event_day': anniversary_date.day
        }
    
    def _schedule_effective_date_email(self, contact: Contact, year: int, scheduler_run_id: str) -> Optional[Dict[str, Any]]:
        """Schedule effective date email using DSL timing rules"""
        if not contact.effective_date:
            return None
        
        # Calculate this year's effective date anniversary
        try:
            anniversary_date = contact.effective_date.replace(year=year)
        except ValueError:
            anniversary_date = contact.effective_date.replace(year=year, month=2, day=28)
        
        # If this year's anniversary has passed, use next year
        if anniversary_date < date.today():
            try:
                anniversary_date = contact.effective_date.replace(year=year + 1)
            except ValueError:
                anniversary_date = contact.effective_date.replace(year=year + 1, month=2, day=28)
        
        # Calculate send date using timing rule
        send_date = self.config.effective_date_timing.calculate_send_date(anniversary_date)
        
        # Check exclusion rules
        is_excluded, skip_reason = self.state_rules.is_date_excluded(
            contact.state, send_date, contact.birthday, contact.effective_date
        )
        
        return {
            'contact_id': contact.id,
            'email_type': EmailType.EFFECTIVE_DATE.value,
            'scheduled_send_date': send_date.isoformat(),
            'scheduled_send_time': self.config.send_time,
            'status': EmailStatus.SKIPPED.value if is_excluded else EmailStatus.PRE_SCHEDULED.value,
            'skip_reason': skip_reason,
            'priority': 5,  # High priority for anniversary emails
            'scheduler_run_id': scheduler_run_id,
            'event_year': anniversary_date.year,
            'event_month': anniversary_date.month,
            'event_day': anniversary_date.day
        }
    
    def _schedule_aep_email(self, contact: Contact, year: int, scheduler_run_id: str) -> Optional[Dict[str, Any]]:
        """Schedule AEP (Annual Enrollment Period) email"""
        # Use first AEP date from config (typically Sept 15)
        aep_config = self.config.aep_dates[0]
        aep_date = date(year, aep_config['month'], aep_config['day'])
        
        # If this year's AEP has passed, use next year
        if aep_date < date.today():
            aep_date = date(year + 1, aep_config['month'], aep_config['day'])
        
        # AEP emails are sent on the date (no days_before calculation)
        send_date = aep_date
        
        # Check exclusion rules
        is_excluded, skip_reason = self.state_rules.is_date_excluded(
            contact.state, send_date, contact.birthday, contact.effective_date
        )
        
        return {
            'contact_id': contact.id,
            'email_type': EmailType.AEP.value,
            'scheduled_send_date': send_date.isoformat(),
            'scheduled_send_time': self.config.send_time,
            'status': EmailStatus.SKIPPED.value if is_excluded else EmailStatus.PRE_SCHEDULED.value,
            'skip_reason': skip_reason,
            'priority': 3,  # Medium priority
            'scheduler_run_id': scheduler_run_id,
            'event_year': aep_date.year,
            'event_month': aep_date.month,
            'event_day': aep_date.day
        }
    
    def _schedule_post_window_email(self, contact: Contact, scheduler_run_id: str) -> Optional[Dict[str, Any]]:
        """Schedule post-exclusion window email"""
        # Find the next date when exclusion window ends
        # This is a simplified implementation - in practice would need to calculate
        # the exact end date of the current exclusion window
        
        # For now, schedule 30 days from today as a catch-up
        send_date = date.today() + timedelta(days=30)
        
        return {
            'contact_id': contact.id,
            'email_type': EmailType.POST_WINDOW.value,
            'scheduled_send_date': send_date.isoformat(),
            'scheduled_send_time': self.config.send_time,
            'status': EmailStatus.PRE_SCHEDULED.value,
            'priority': 8,  # Lower priority for catch-up emails
            'scheduler_run_id': scheduler_run_id
        }

# ============================================================================
# LOAD BALANCING AND SMOOTHING ENGINE
# ============================================================================

class LoadBalancer:
    """Load balancing and smoothing engine for email distribution"""
    
    def __init__(self, config: LoadBalancingConfig):
        self.config = config
    
    def apply_load_balancing(self, schedules: List[Dict[str, Any]], total_contacts: int) -> List[Dict[str, Any]]:
        """Apply load balancing and smoothing to email schedules"""
        if not schedules:
            return schedules
        
        logger.info(f"Applying load balancing to {len(schedules)} schedules")
        
        # 1. Apply effective date smoothing first
        smoothed_schedules = self.smooth_effective_date_emails(schedules)
        
        # 2. Apply global daily cap enforcement
        balanced_schedules = self.enforce_daily_caps(smoothed_schedules, total_contacts)
        
        # 3. Distribute catch-up emails
        final_schedules = self.distribute_catch_up_emails(balanced_schedules)
        
        logger.info(f"Load balancing complete: {len(final_schedules)} schedules")
        return final_schedules
    
    def smooth_effective_date_emails(self, schedules: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Smooth effective date email clustering using deterministic jitter"""
        logger.info("Applying effective date smoothing")
        
        # Group schedules by date and email type
        schedules_by_date = {}
        for schedule in schedules:
            send_date = schedule['scheduled_send_date']
            if send_date not in schedules_by_date:
                schedules_by_date[send_date] = []
            schedules_by_date[send_date].append(schedule)
        
        smoothed_schedules = []
        today = date.today()
        
        for send_date_str, date_schedules in schedules_by_date.items():
            send_date = datetime.strptime(send_date_str, '%Y-%m-%d').date()
            
            # Count effective date emails for this date
            ed_emails = [s for s in date_schedules if s['email_type'] == EmailType.EFFECTIVE_DATE.value]
            ed_count = len(ed_emails)
            
            # Apply smoothing if ED limit exceeded
            if ed_count > self.config.ed_daily_soft_limit:
                logger.info(f"Smoothing {ed_count} effective date emails on {send_date_str}")
                
                # Apply jitter to ED emails
                for schedule in ed_emails:
                    jittered_date = self._calculate_jittered_date(
                        schedule, send_date, self.config.ed_smoothing_window_days
                    )
                    
                    # Ensure date is not in the past
                    if jittered_date < today:
                        jittered_date = today + timedelta(days=1)
                    
                    schedule['scheduled_send_date'] = jittered_date.isoformat()
                    schedule['load_balancing_applied'] = True
                    schedule['original_send_date'] = send_date_str
                
                # Add non-ED emails unchanged
                non_ed_emails = [s for s in date_schedules if s['email_type'] != EmailType.EFFECTIVE_DATE.value]
                smoothed_schedules.extend(non_ed_emails)
                smoothed_schedules.extend(ed_emails)
            else:
                # No smoothing needed
                smoothed_schedules.extend(date_schedules)
        
        return smoothed_schedules
    
    def enforce_daily_caps(self, schedules: List[Dict[str, Any]], total_contacts: int) -> List[Dict[str, Any]]:
        """Enforce global daily email caps with redistribution"""
        logger.info("Enforcing daily caps")
        
        # Calculate daily cap
        daily_cap = int(total_contacts * self.config.daily_send_percentage_cap)
        overage_threshold = int(daily_cap * self.config.overage_threshold)
        
        logger.info(f"Daily cap: {daily_cap}, Overage threshold: {overage_threshold}")
        
        # Group schedules by date
        schedules_by_date = {}
        for schedule in schedules:
            send_date = schedule['scheduled_send_date']
            if send_date not in schedules_by_date:
                schedules_by_date[send_date] = []
            schedules_by_date[send_date].append(schedule)
        
        # Sort dates for processing
        sorted_dates = sorted(schedules_by_date.keys())
        balanced_schedules = []
        
        for send_date_str in sorted_dates:
            date_schedules = schedules_by_date[send_date_str]
            
            if len(date_schedules) > overage_threshold:
                logger.info(f"Redistributing {len(date_schedules)} emails from {send_date_str}")
                
                # Keep emails up to daily cap
                keep_schedules = date_schedules[:daily_cap]
                overflow_schedules = date_schedules[daily_cap:]
                
                # Redistribute overflow to next available days
                redistributed = self._redistribute_overflow(
                    overflow_schedules, send_date_str, sorted_dates, schedules_by_date, daily_cap
                )
                
                balanced_schedules.extend(keep_schedules)
                balanced_schedules.extend(redistributed)
            else:
                balanced_schedules.extend(date_schedules)
        
        return balanced_schedules
    
    def distribute_catch_up_emails(self, schedules: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Distribute catch-up emails across window to prevent clustering"""
        logger.info("Distributing catch-up emails")
        
        today = date.today()
        catch_up_schedules = []
        regular_schedules = []
        
        # Separate catch-up emails (those with past send dates)
        for schedule in schedules:
            send_date = datetime.strptime(schedule['scheduled_send_date'], '%Y-%m-%d').date()
            
            # Check if this is a catch-up email (past due but event still future)
            if send_date < today:
                event_date = None
                if schedule.get('event_year'):
                    event_date = date(
                        schedule['event_year'],
                        schedule['event_month'],
                        schedule['event_day']
                    )
                
                # Only reschedule if event is still in the future
                if event_date and event_date > today:
                    catch_up_schedules.append(schedule)
                else:
                    # Event has passed, keep original schedule (will likely be skipped)
                    regular_schedules.append(schedule)
            else:
                regular_schedules.append(schedule)
        
        # Distribute catch-up emails
        if catch_up_schedules:
            logger.info(f"Distributing {len(catch_up_schedules)} catch-up emails")
            
            for schedule in catch_up_schedules:
                distributed_date = self._calculate_catch_up_date(schedule)
                schedule['scheduled_send_date'] = distributed_date.isoformat()
                schedule['catch_up_applied'] = True
                schedule['original_send_date'] = schedule.get('original_send_date', schedule['scheduled_send_date'])
        
        return regular_schedules + catch_up_schedules
    
    def _calculate_jittered_date(self, schedule: Dict[str, Any], original_date: date, window_days: int) -> date:
        """Calculate jittered date using deterministic hash"""
        # Create deterministic hash input
        hash_input = f"{schedule['contact_id']}_{schedule['email_type']}_{schedule.get('event_year', '')}"
        hash_value = int(hashlib.md5(hash_input.encode()).hexdigest(), 16)
        
        # Calculate jitter within window
        total_window = window_days * 2 + 1  # ±window_days
        jitter_offset = (hash_value % total_window) - window_days
        
        jittered_date = original_date + timedelta(days=jitter_offset)
        return jittered_date
    
    def _calculate_catch_up_date(self, schedule: Dict[str, Any]) -> date:
        """Calculate catch-up date using deterministic distribution"""
        hash_input = f"{schedule['contact_id']}_{schedule['email_type']}_catchup"
        hash_value = int(hashlib.md5(hash_input.encode()).hexdigest(), 16)
        
        # Distribute across catch-up window starting tomorrow
        day_offset = (hash_value % self.config.catch_up_spread_days) + 1
        catch_up_date = date.today() + timedelta(days=day_offset)
        
        return catch_up_date
    
    def _redistribute_overflow(self, overflow_schedules: List[Dict[str, Any]], 
                             original_date: str, sorted_dates: List[str],
                             schedules_by_date: Dict[str, List], daily_cap: int) -> List[Dict[str, Any]]:
        """Redistribute overflow emails to subsequent days"""
        redistributed = []
        current_date = datetime.strptime(original_date, '%Y-%m-%d').date()
        
        for schedule in overflow_schedules:
            # Find next available day with capacity
            next_date = current_date + timedelta(days=1)
            
            while True:
                next_date_str = next_date.isoformat()
                
                # Count existing schedules for this date
                existing_count = len(schedules_by_date.get(next_date_str, []))
                
                if existing_count < daily_cap:
                    # Found available capacity
                    schedule['scheduled_send_date'] = next_date_str
                    schedule['redistributed_from'] = original_date
                    
                    # Update the tracking dictionary
                    if next_date_str not in schedules_by_date:
                        schedules_by_date[next_date_str] = []
                    schedules_by_date[next_date_str].append(schedule)
                    
                    redistributed.append(schedule)
                    break
                else:
                    # Try next day
                    next_date += timedelta(days=1)
        
        return redistributed

# ============================================================================
# MAIN SCHEDULER ORCHESTRATOR
# ============================================================================

class EmailScheduler:
    """Main scheduler orchestrator that coordinates all components"""
    
    def __init__(self, db_path: str, config_path: Optional[str] = None):
        self.db_manager = DatabaseManager(db_path)
        self.config = SchedulingConfig.from_yaml(config_path) if config_path else SchedulingConfig()
        self.state_rules = StateRulesEngine()
        self.anniversary_scheduler = AnniversaryEmailScheduler(self.config, self.state_rules)
        self.load_balancer = LoadBalancer(self.config.load_balancing)
        
        # Import and initialize campaign scheduler
        try:
            from campaign_scheduler import CampaignEmailScheduler
            self.campaign_scheduler = CampaignEmailScheduler(self.config, self.state_rules, db_path)
        except ImportError:
            logger.warning("Campaign scheduler not available - campaign emails will be skipped")
            self.campaign_scheduler = None
        
        self.scheduler_run_id = str(uuid.uuid4())
        
        logger.info(f"Email Scheduler initialized with run ID: {self.scheduler_run_id}")
    
    def apply_frequency_limits(self, schedules: List[Dict[str, Any]], contact_id: int) -> List[Dict[str, Any]]:
        """Apply email frequency limits to prevent overwhelming contacts"""
        if not schedules:
            return schedules
        
        # Query recent emails for this contact (excluding follow-ups from count)
        lookback_date = date.today() - timedelta(days=self.config.load_balancing.period_days)
        
        with sqlite3.connect(self.db_manager.db_path) as conn:
            cursor = conn.execute("""
                SELECT COUNT(*) FROM email_schedules 
                WHERE contact_id = ? 
                AND scheduled_send_date >= ?
                AND status IN ('pre-scheduled', 'scheduled', 'sent', 'delivered')
                AND email_type NOT LIKE 'followup_%'
            """, (contact_id, lookback_date.isoformat()))
            
            current_email_count = cursor.fetchone()[0]
        
        # Calculate available slots
        max_emails = self.config.load_balancing.max_emails_per_contact_per_period
        available_slots = max_emails - current_email_count
        
        if available_slots <= 0:
            # Mark all as skipped
            for schedule in schedules:
                if not schedule['email_type'].startswith('followup_'):
                    schedule['status'] = EmailStatus.SKIPPED.value
                    schedule['skip_reason'] = 'frequency_limit_exceeded'
            return schedules
        
        if len(schedules) <= available_slots:
            # All schedules can be sent
            return schedules
        
        # Need to prioritize - sort by priority (lower number = higher priority)
        sorted_schedules = sorted(schedules, key=lambda x: x.get('priority', 10))
        
        # Keep high priority emails, skip the rest
        kept_schedules = []
        skipped_count = 0
        
        for i, schedule in enumerate(sorted_schedules):
            if schedule['email_type'].startswith('followup_'):
                # Always allow follow-ups
                kept_schedules.append(schedule)
            elif i < available_slots:
                # Within limit
                kept_schedules.append(schedule)
            else:
                # Over limit - skip
                schedule['status'] = EmailStatus.SKIPPED.value
                schedule['skip_reason'] = 'frequency_limit_exceeded'
                kept_schedules.append(schedule)
                skipped_count += 1
        
        if skipped_count > 0:
            logger.info(f"Frequency limit applied to contact {contact_id}: {skipped_count} emails skipped")
        
        return kept_schedules

    def run_full_schedule(self):
        """Run complete scheduling process for all contacts using a high-performance, query-driven approach."""
        logger.info("Starting full email scheduling process")
        
        total_contacts = self.db_manager.get_total_contact_count()
        scheduled_count = 0
        skipped_count = 0
        
        # Create checkpoint for audit and recovery
        checkpoint_id = self.db_manager.create_checkpoint(self.scheduler_run_id, total_contacts)
        
        try:
            # 1. Fetch relevant contacts for anniversary emails
            # This is the core performance improvement: query-driven pre-filtering.
            relevant_contacts = self.db_manager.get_contacts_in_scheduling_window(
                lookahead_days=30, lookback_days=7
            )

            # 2. Generate all potential anniversary schedules from the small, relevant contact list.
            anniversary_schedules = self.anniversary_scheduler.schedule_all_anniversary_emails(
                relevant_contacts, self.scheduler_run_id
            )

            # 3. Generate all campaign schedules (this is now also a batch operation).
            campaign_schedules = []
            if self.campaign_scheduler:
                campaign_schedules = self.campaign_scheduler.schedule_all_campaign_emails(
                    self.scheduler_run_id
                )
            
            # 4. Combine all potential schedules.
            all_schedules = anniversary_schedules + campaign_schedules
            
            # 5. Apply load balancing to the entire batch of schedules.
            balanced_schedules = self.load_balancer.apply_load_balancing(all_schedules, total_contacts)
            
            # 6. Update final counts
            for s in balanced_schedules:
                if s['status'] == EmailStatus.SKIPPED.value:
                    skipped_count += 1
                else:
                    scheduled_count += 1
            
            # 7. Insert schedules into database with robust transaction management.
            if balanced_schedules:
                self.db_manager.batch_insert_schedules_transactional(balanced_schedules)
            
            # Final checkpoint update
            self.db_manager.update_checkpoint(
                checkpoint_id,
                'completed',
                contacts_processed=total_contacts, # This is now a conceptual number
                emails_scheduled=scheduled_count,
                emails_skipped=skipped_count
            )
            
            logger.info(f"""
            Scheduling complete:
            - Relevant contacts considered: {len(relevant_contacts)}
            - Total emails scheduled: {scheduled_count}
            - Total emails skipped: {skipped_count}
            - Run ID: {self.scheduler_run_id}
            """)
            
        except Exception as e:
            # Update checkpoint with error
            self.db_manager.update_checkpoint(
                checkpoint_id,
                'failed',
                error_message=str(e),
                emails_scheduled=scheduled_count,
                emails_skipped=skipped_count
            )
            logger.error(f"Scheduling failed: {e}")
            raise
    
    def schedule_for_contact(self, contact_id: int) -> List[Dict[str, Any]]:
        """Schedule emails for a specific contact (useful for testing)"""
        contacts = self.db_manager.get_contacts_batch(offset=0, limit=1)
        # This is a simplified version - would need to modify get_contacts_batch to accept contact_id filter
        # For now, this is a placeholder
        return []

# ============================================================================
# COMMAND LINE INTERFACE
# ============================================================================

def main():
    """Main entry point for the scheduler"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Email Scheduling System')
    parser.add_argument('--db', required=True, help='SQLite database path')
    parser.add_argument('--config', help='Configuration YAML path')
    parser.add_argument('--run-full', action='store_true', help='Run full scheduling for all contacts')
    parser.add_argument('--contact-id', type=int, help='Schedule for specific contact ID')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    
    args = parser.parse_args()
    
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    
    scheduler = EmailScheduler(args.db, args.config)
    
    if args.run_full:
        scheduler.run_full_schedule()
    elif args.contact_id:
        schedules = scheduler.schedule_for_contact(args.contact_id)
        print(f"Scheduled {len(schedules)} emails for contact {args.contact_id}")
    else:
        print("Please specify --run-full or --contact-id")

if __name__ == '__main__':
    main()