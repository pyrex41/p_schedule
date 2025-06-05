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
        return self.state_rules.get(state_code.upper())
    
    def is_date_excluded(self, state_code: str, check_date: date, 
                        contact_birthday: Optional[date] = None,
                        contact_effective_date: Optional[date] = None) -> Tuple[bool, Optional[str]]:
        """Check if a date is excluded for a contact in a specific state"""
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
                'actual_send_datetime': 'TEXT'
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
    
    def batch_insert_schedules(self, schedules: List[Dict[str, Any]]):
        """Batch insert email schedules"""
        if not schedules:
            return
        
        with sqlite3.connect(self.db_path) as conn:
            conn.executemany("""
                INSERT OR IGNORE INTO email_schedules 
                (contact_id, email_type, scheduled_send_date, scheduled_send_time, 
                 status, skip_reason, priority, campaign_instance_id, email_template, 
                 sms_template, scheduler_run_id, event_year, event_month, event_day)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                (s['contact_id'], s['email_type'], s['scheduled_send_date'], 
                 s['scheduled_send_time'], s['status'], s.get('skip_reason'),
                 s.get('priority', 10), s.get('campaign_instance_id'),
                 s.get('email_template'), s.get('sms_template'), 
                 s['scheduler_run_id'], s.get('event_year'), s.get('event_month'), 
                 s.get('event_day'))
                for s in schedules
            ])
            
            logger.info(f"Inserted {len(schedules)} email schedules")

# ============================================================================
# ANNIVERSARY EMAIL SCHEDULER - HANDLES BIRTHDAY, EFFECTIVE DATE, AEP
# ============================================================================

class AnniversaryEmailScheduler:
    """Handles scheduling of anniversary-based emails using DSL rules"""
    
    def __init__(self, config: SchedulingConfig, state_rules: StateRulesEngine):
        self.config = config
        self.state_rules = state_rules
    
    def schedule_anniversary_emails(self, contact: Contact, scheduler_run_id: str) -> List[Dict[str, Any]]:
        """Schedule all anniversary-based emails for a contact"""
        schedules = []
        current_year = datetime.now().year
        
        # Birthday emails
        if contact.birthday:
            birthday_schedule = self._schedule_birthday_email(contact, current_year, scheduler_run_id)
            if birthday_schedule:
                schedules.append(birthday_schedule)
        
        # Effective date emails
        if contact.effective_date:
            ed_schedule = self._schedule_effective_date_email(contact, current_year, scheduler_run_id)
            if ed_schedule:
                schedules.append(ed_schedule)
        
        # AEP emails
        aep_schedule = self._schedule_aep_email(contact, current_year, scheduler_run_id)
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
# MAIN SCHEDULER ORCHESTRATOR
# ============================================================================

class EmailScheduler:
    """Main scheduler orchestrator that coordinates all components"""
    
    def __init__(self, db_path: str, config_path: Optional[str] = None):
        self.db_manager = DatabaseManager(db_path)
        self.config = SchedulingConfig.from_yaml(config_path) if config_path else SchedulingConfig()
        self.state_rules = StateRulesEngine()
        self.anniversary_scheduler = AnniversaryEmailScheduler(self.config, self.state_rules)
        
        # Import and initialize campaign scheduler
        try:
            from campaign_scheduler import CampaignEmailScheduler
            self.campaign_scheduler = CampaignEmailScheduler(self.config, self.state_rules, db_path)
        except ImportError:
            logger.warning("Campaign scheduler not available - campaign emails will be skipped")
            self.campaign_scheduler = None
        
        self.scheduler_run_id = str(uuid.uuid4())
        
        logger.info(f"Email Scheduler initialized with run ID: {self.scheduler_run_id}")
    
    def run_full_schedule(self):
        """Run complete scheduling process for all contacts"""
        logger.info("Starting full email scheduling process")
        
        total_contacts = self.db_manager.get_total_contact_count()
        processed_count = 0
        scheduled_count = 0
        skipped_count = 0
        
        # Process contacts in batches
        offset = 0
        while offset < total_contacts:
            batch_contacts = self.db_manager.get_contacts_batch(offset, self.config.batch_size)
            if not batch_contacts:
                break
            
            logger.info(f"Processing batch: {offset} - {offset + len(batch_contacts)} of {total_contacts}")
            
            # Clear existing schedules for this batch
            contact_ids = [c.id for c in batch_contacts]
            self.db_manager.clear_scheduled_emails(contact_ids, self.scheduler_run_id)
            
            # Schedule emails for this batch
            all_schedules = []
            for contact in batch_contacts:
                try:
                    # Schedule anniversary-based emails
                    anniversary_schedules = self.anniversary_scheduler.schedule_anniversary_emails(
                        contact, self.scheduler_run_id
                    )
                    all_schedules.extend(anniversary_schedules)
                    
                    # Schedule campaign-based emails
                    if self.campaign_scheduler:
                        campaign_schedules = self.campaign_scheduler.schedule_campaign_emails(
                            contact, self.scheduler_run_id
                        )
                        all_schedules.extend(campaign_schedules)
                    
                    processed_count += 1
                    
                except Exception as e:
                    logger.error(f"Error processing contact {contact.id}: {e}")
                    continue
            
            # Apply load balancing and smoothing
            # TODO: Implement load balancing logic
            
            # Count scheduled vs skipped
            batch_scheduled = len([s for s in all_schedules if s['status'] == EmailStatus.PRE_SCHEDULED.value])
            batch_skipped = len([s for s in all_schedules if s['status'] == EmailStatus.SKIPPED.value])
            
            scheduled_count += batch_scheduled
            skipped_count += batch_skipped
            
            # Insert schedules into database
            self.db_manager.batch_insert_schedules(all_schedules)
            
            offset += len(batch_contacts)
        
        logger.info(f"""
        Scheduling complete:
        - Contacts processed: {processed_count}
        - Emails scheduled: {scheduled_count}
        - Emails skipped: {skipped_count}
        - Run ID: {self.scheduler_run_id}
        """)
    
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