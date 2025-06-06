"""
Active Window Event Scheduling with Simplified Pre-Filtering

This module implements an advanced email scheduling system designed to:
1. Efficiently pre-filter contacts based on relevant date ranges
2. Identify specific event instances requiring scheduling
3. Apply intelligent send date calculations with catch-up and smoothing logic
4. Validate against state rules and exclusion windows
5. Perform atomic batch database operations

Key features:
- Efficiency: Minimizes processing by intelligently pre-filtering contacts
- Accuracy: Ensures timely communications relative to trigger events
- Load Balancing: Smooths sending peaks for clustered events (like month-start effective dates)
- Catch-Up: Handles contacts whose ideal send date has passed but event is still in future
- Stability: Avoids unnecessarily rescheduling communications already in progress
"""

import asyncio
import logging
from datetime import date, datetime, timedelta
from typing import List, Dict, Any, Tuple, Optional, Set, DefaultDict
from collections import defaultdict
import hashlib
from math import ceil
from src.db_utils import get_org_db_conn, get_direct_connection
from src.contact_rules_engine import ContactRuleEngine, EmailType, EmailSchedule
from src.email_scheduler_common import ScheduleContactDetail
from src.org_utils import parse_date_flexible
import yaml
import os
from pathlib import Path
import json
from functools import lru_cache
from threading import Lock
from tqdm import tqdm

logger = logging.getLogger(__name__)

# Type alias for database connections - allows flexibility in implementation
Connection = Any  # Compatible with both SQLite and Turso connections

def get_org_conn(org_id: int):
    """Get organization database connection."""
    org_db_url = os.getenv("ORG_TURSO_DB_URL")
    org_auth_token = os.getenv("ORG_TURSO_AUTH_TOKEN")
    if org_db_url and org_auth_token:
        return get_direct_connection(org_db_url, org_auth_token)
    else:
        return get_org_db_conn(org_id)

# Event data structure - using TypedDict for better type hints
class EventData:
    """Structured event data to replace tuples"""
    def __init__(self, 
                 contact_dict: Dict[str, Any],
                 event_type: str,
                 event_year: int,
                 event_month: int,
                 event_day: int,
                 ideal_send_date: date,
                 target_send_date: Optional[date] = None,
                 final_send_date: Optional[date] = None,
                 status: str = 'pending',
                 skip_reason: Optional[str] = None,
                 catchup_note: Optional[str] = None,
                 is_pre_marked_skip: bool = False):
        self.contact_dict = contact_dict
        self.event_type = event_type
        self.event_year = event_year
        self.event_month = event_month
        self.event_day = event_day
        self.ideal_send_date = ideal_send_date
        self.target_send_date = target_send_date or ideal_send_date
        self.final_send_date = final_send_date or self.target_send_date
        self.status = status
        self.skip_reason = skip_reason
        self.catchup_note = catchup_note
        self.is_pre_marked_skip = is_pre_marked_skip
    
    @property
    def contact_id(self) -> int:
        return self.contact_dict.get('id')
    
    @property
    def event_date(self) -> date:
        return date(self.event_year, self.event_month, self.event_day)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for logging/serialization"""
        return {
            'contact_id': self.contact_id,
            'event_type': self.event_type,
            'event_year': self.event_year,
            'event_month': self.event_month,
            'event_day': self.event_day,
            'ideal_send_date': self.ideal_send_date.isoformat(),
            'target_send_date': self.target_send_date.isoformat(),
            'final_send_date': self.final_send_date.isoformat(),
            'status': self.status,
            'skip_reason': self.skip_reason,
            'catchup_note': self.catchup_note,
            'is_pre_marked_skip': self.is_pre_marked_skip
        }
    
    def mark_as_skipped(self, reason: str):
        """Mark this event as skipped with a reason"""
        self.status = 'skipped'
        self.skip_reason = reason
    
    def mark_as_scheduled(self, final_date: Optional[date] = None):
        """Mark this event as scheduled"""
        self.status = 'pre-scheduled'
        if final_date:
            self.final_send_date = final_date
    
    def is_catchup(self, today: date) -> bool:
        """Check if this is a catch-up email"""
        return self.ideal_send_date < today and self.target_send_date >= today
    
    def __repr__(self):
        return f"EventData(contact_id={self.contact_id}, type={self.event_type}, status={self.status})"

# In-memory cache for frequently accessed data
class SchedulerCache:
    def __init__(self):
        self._cache_lock = Lock()
        self._eligibility_cache = {}  # contact_id -> (timestamp, result)
        self._recent_emails_cache = {}  # contact_id -> [(email_type, date), ...]
        self._contact_cache = {}  # contact_id -> contact_data
        self._cache_ttl = timedelta(hours=1)  # Cache TTL
        self._last_cleanup = datetime.now()
        
    def _cleanup_expired(self):
        """Remove expired cache entries"""
        now = datetime.now()
        if now - self._last_cleanup < timedelta(minutes=5):
            return
            
        with self._cache_lock:
            cutoff = now - self._cache_ttl
            
            # Cleanup eligibility cache
            self._eligibility_cache = {
                k: v for k, v in self._eligibility_cache.items()
                if v[0] > cutoff
            }
            
            # Cleanup recent emails cache
            self._recent_emails_cache = {
                k: v for k, v in self._recent_emails_cache.items()
                if any(date > cutoff for _, date in v)
            }
            
            self._last_cleanup = now
    
    def get_eligibility(self, contact_id: int) -> Optional[Tuple[bool, str]]:
        """Get cached eligibility result for a contact"""
        with self._cache_lock:
            self._cleanup_expired()
            if contact_id in self._eligibility_cache:
                timestamp, result = self._eligibility_cache[contact_id]
                if datetime.now() - timestamp < self._cache_ttl:
                    return result
                del self._eligibility_cache[contact_id]
        return None
    
    def set_eligibility(self, contact_id: int, result: Tuple[bool, str]):
        """Cache eligibility result for a contact"""
        with self._cache_lock:
            self._eligibility_cache[contact_id] = (datetime.now(), result)
    
    def get_recent_emails(self, contact_id: int) -> Optional[List[Tuple[str, datetime]]]:
        """Get cached recent emails for a contact"""
        with self._cache_lock:
            self._cleanup_expired()
            if contact_id in self._recent_emails_cache:
                emails = self._recent_emails_cache[contact_id]
                if any(date > datetime.now() - self._cache_ttl for _, date in emails):
                    return emails
                del self._recent_emails_cache[contact_id]
        return None
    
    def set_recent_emails(self, contact_id: int, emails: List[Tuple[str, datetime]]):
        """Cache recent emails for a contact"""
        with self._cache_lock:
            self._recent_emails_cache[contact_id] = emails
    
    def get_contact(self, contact_id: int) -> Optional[Dict[str, Any]]:
        """Get cached contact data"""
        with self._cache_lock:
            self._cleanup_expired()
            if contact_id in self._contact_cache:
                timestamp, data = self._contact_cache[contact_id]
                if datetime.now() - timestamp < self._cache_ttl:
                    return data
                del self._contact_cache[contact_id]
        return None
    
    def set_contact(self, contact_id: int, data: Dict[str, Any]):
        """Cache contact data"""
        with self._cache_lock:
            self._contact_cache[contact_id] = (datetime.now(), data)
    
    def clear(self):
        """Clear all caches"""
        with self._cache_lock:
            self._eligibility_cache.clear()
            self._recent_emails_cache.clear()
            self._contact_cache.clear()

# Global cache instance
scheduler_cache = SchedulerCache()

def check_contact_recent_emails_and_eligibility(
    org_id: int, 
    contact_id: int, 
    days_to_check: int = None
) -> Tuple[bool, Optional[str]]:
    """
    Check if a contact has:
    1. Received any email within the specified days window
    2. Answered eligibility questions after that email was sent
    
    Uses in-memory cache for better performance.
    """
    if days_to_check is None:
        days_to_check = RECENT_EMAIL_CHECK_DAYS
    
    # Check cache first
    cached_result = scheduler_cache.get_eligibility(contact_id)
    if cached_result is not None:
        return cached_result
    
    try:
        # Get cursor
        db_conn = get_org_conn(org_id)
        cursor = db_conn.cursor()
        
        # Check if the contact_events table exists
        try:
            cursor.execute("SELECT COUNT(*) FROM contact_events")
            event_count = cursor.fetchone()[0]
            logger.debug(f"contact_events table exists with {event_count} records")
        except Exception as e:
            logger.warning(f"contact_events table does not exist in database: {e}")
            return False, None
            
        # First check if this contact has any eligibility events
        try:
            cursor.execute("""
                SELECT metadata, created_at FROM contact_events
                WHERE contact_id = ? 
                AND event_type = 'eligibility_answered'
                ORDER BY created_at DESC
                LIMIT 1
            """, (contact_id,))
            
            eligibility_event = cursor.fetchone()
            
            if eligibility_event:
                # Extract data in a way that works with both dict-like and tuple rows
                if hasattr(eligibility_event, 'keys'):
                    metadata_str = eligibility_event['metadata'] 
                    skip_timestamp = eligibility_event['created_at']
                else:
                    metadata_str, skip_timestamp = eligibility_event
                
                # Format the timestamp
                skip_date_str = _format_timestamp(skip_timestamp)
                
                # Parse metadata for details if available
                has_medical_conditions = _parse_eligibility_metadata(metadata_str)
                
                # Create a detailed reason
                skip_reason = _create_eligibility_skip_reason(has_medical_conditions, skip_date_str)
                
                # Cache the result
                result = (True, skip_reason)
                scheduler_cache.set_eligibility(contact_id, result)
                
                logger.info(f"Contact {contact_id} has eligibility_answered event. Will skip with reason: {skip_reason}")
                return result
                
        except Exception as e:
            logger.warning(f"Error checking contact_events for contact {contact_id}: {e}")
        
        # If no direct eligibility events found, check for emails + eligibility combo
        # Calculate date for looking back days_to_check
        cutoff_date = (datetime.now() - timedelta(days=days_to_check))
        days_ago = cutoff_date.isoformat()
        
        # Check for sent emails
        try:
            cursor.execute("""
                SELECT id, email_type, status, scheduled_send_date, created_at
                FROM email_schedules
                WHERE contact_id = ? 
                AND (created_at > ? OR scheduled_send_date > ?)
                AND status IN ('sent', 'delivered', 'accepted')
                ORDER BY created_at DESC
                LIMIT 1
            """, (contact_id, days_ago, days_ago))
            
            recent_email = cursor.fetchone()
            
            if not recent_email:
                # No recent emails found, no need to check eligibility
                result = (False, None)
                scheduler_cache.set_eligibility(contact_id, result)
                return result
                
            # Extract email details
            if hasattr(recent_email, 'keys'):
                email_id = recent_email['id']
                email_type = recent_email['email_type']
                email_sent_date = recent_email.get('scheduled_send_date') or recent_email.get('created_at')
            else:
                email_id, email_type, status, scheduled_send_date, created_at = recent_email
                email_sent_date = scheduled_send_date or created_at
            
            # Parse the sent date string to datetime
            if isinstance(email_sent_date, str):
                try:
                    email_sent_datetime = datetime.fromisoformat(email_sent_date.replace('Z', '+00:00'))
                except ValueError:
                    email_sent_datetime = datetime.now() - timedelta(days=days_to_check/2)
            else:
                email_sent_datetime = email_sent_date
                
            # Convert to ISO format for database query
            email_sent_iso = email_sent_datetime.isoformat()
            
            # Check for eligibility_answered events after the email was sent
            cursor.execute("""
                SELECT metadata, created_at FROM contact_events
                WHERE contact_id = ? 
                AND event_type = 'eligibility_answered'
                AND created_at > ?
                ORDER BY created_at DESC
                LIMIT 1
            """, (contact_id, email_sent_iso))
            
            eligibility_event = cursor.fetchone()
            
            if not eligibility_event:
                # No eligibility answers after the recent email, so don't skip
                result = (False, None)
                scheduler_cache.set_eligibility(contact_id, result)
                return result
                
            # We found eligibility answers after the recent email, so we should skip
            if hasattr(eligibility_event, 'keys'):
                metadata_str = eligibility_event['metadata'] 
                skip_timestamp = eligibility_event['created_at']
            else:
                metadata_str, skip_timestamp = eligibility_event
            
            # Format timestamp
            skip_date_str = _format_timestamp(skip_timestamp)
            
            # Parse metadata
            has_medical_conditions = _parse_eligibility_metadata(metadata_str)
            
            # Create a detailed reason specifically for post-email eligibility responses
            if has_medical_conditions is not None:
                condition_str = "with" if has_medical_conditions else "without"
                skip_reason = f"Contact answered eligibility questions {condition_str} medical conditions on {skip_date_str} after recent {email_type} email"
            else:
                skip_reason = f"Contact answered eligibility questions on {skip_date_str} after recent {email_type} email"
            
            result = (True, skip_reason)
            scheduler_cache.set_eligibility(contact_id, result)
            return result
            
        except Exception as e:
            logger.warning(f"Error checking recent emails and eligibility for contact {contact_id}: {e}")
            return False, None
            
    except Exception as e:
        logger.warning(f"Error in check_contact_recent_emails_and_eligibility for contact {contact_id}: {e}")
        return False, None

# Load configuration using the new centralized config loader
import sys
sys.path.append(str(Path(__file__).parent.parent))
from config_loader import (
    load_all_configs, get_timing_constant, get_load_balancing_config,
    get_birthday_offset, get_effective_date_offset, get_lookahead_days,
    get_lookback_days, get_daily_send_cap_percentage, get_standard_send_time,
    get_config_section
)

# Load all configuration
config = load_all_configs()

# Configuration constants - now loaded from YAML
BIRTHDAY_OFFSET = get_birthday_offset()
EFFECTIVE_DATE_OFFSET = get_effective_date_offset()

# Advanced scheduling configuration - now from config
LOOKAHEAD_DAYS = get_lookahead_days()
LOOKBACK_DAYS = get_lookback_days()
CATCH_UP_SPREAD_DAYS = get_timing_constant("catch_up_spread_days", 7)
ED_SMOOTHING_WINDOW_DAYS = get_timing_constant("ed_smoothing_window_days", 5)
ED_DAILY_SOFT_LIMIT = get_timing_constant("ed_daily_soft_limit", 15)
STANDARD_SEND_TIME = get_standard_send_time()
DAILY_SEND_PERCENTAGE_CAP = get_daily_send_cap_percentage()

# AEP configuration
AEP_PRE_SCHEDULING_LEAD_DAYS = get_timing_constant("aep_pre_scheduling_lead_days", 30)
AEP_INCLUDE_ALL_CONTACTS = get_timing_constant("aep_include_all_contacts", False)

# Recent email and eligibility check configuration
RECENT_EMAIL_CHECK_DAYS = get_timing_constant("recent_email_check_days", 90)
ELIGIBILITY_CHECK_ENABLED = get_timing_constant("eligibility_check_enabled", True)

# Legacy config support - maintain backward compatibility
yaml_config = config  # For any remaining legacy references
aep_campaigns_config = config.get("aep_campaigns_config", {"aep_campaigns": {}, "aep_config": {"pre_scheduling_lead_days": 30, "include_all_contacts": False}})
campaign_eligibility_config = config.get("campaign_eligibility_config", {})

# Default: Apply to birthday and effective date emails, and optionally specific campaigns
APPLY_ELIGIBILITY_CHECK_TO = {
    "BIRTHDAY": True,
    "EFFECTIVE_DATE": True,
    # AEP campaigns can be configured individually in the yaml config
}

# Email frequency control configuration 
email_frequency_config = config.get("email_frequency_config", {})
RESPECT_90_DAY_WINDOW = email_frequency_config.get("respect_90_day_window", True)
SHORT_WINDOW_ADJUSTMENT_DAYS = email_frequency_config.get("short_window_adjustment_days", 5)
LONG_WINDOW_THRESHOLD_DAYS = email_frequency_config.get("long_window_threshold_days", 30)
CAMPAIGN_FREQUENCY_OVERRIDES = email_frequency_config.get("campaign_overrides", {})


def _is_campaign_active_for_scheduling(
    campaign_start_date: date, 
    campaign_end_date: date, 
    today: date, 
    pre_scheduling_lead_days: int = AEP_PRE_SCHEDULING_LEAD_DAYS
) -> bool:
    """
    Determine if a campaign is active for scheduling based on today's date.
    
    Args:
        campaign_start_date: Campaign start date
        campaign_end_date: Campaign end date
        today: Today's date
        pre_scheduling_lead_days: Days before campaign start to begin scheduling
        
    Returns:
        True if the campaign is active for scheduling, False otherwise
    """
    # Campaign is active if today is within the pre-scheduling window or actual campaign window
    pre_scheduling_start = campaign_start_date - timedelta(days=pre_scheduling_lead_days)
    return pre_scheduling_start <= today <= campaign_end_date


def _is_any_aep_campaign_active(
    today: date, 
    aep_campaigns: Dict[str, Dict[str, Any]] = aep_campaigns_config.get("aep_campaigns", {})
) -> bool:
    """
    Check if any AEP campaign is currently active for scheduling.
    
    Args:
        today: Today's date
        aep_campaigns: Dictionary of AEP campaign configurations
        
    Returns:
        True if any campaign is active for scheduling, False otherwise
    """
    for campaign_key, details in aep_campaigns.items():
        try:
            campaign_start_date = date.fromisoformat(details['send_window_start'])
            campaign_end_date = date.fromisoformat(details['send_window_end'])
            
            if _is_campaign_active_for_scheduling(campaign_start_date, campaign_end_date, today):
                return True
        except (KeyError, ValueError) as e:
            logger.warning(f"Invalid campaign configuration for {campaign_key}: {e}")
    
    return False


async def get_contacts_in_scheduling_window(
    db_conn: Connection, 
    today: date, 
    lookahead_days: int,
    lookback_days: int = LOOKBACK_DAYS,
    include_all_for_aep: bool = AEP_INCLUDE_ALL_CONTACTS
) -> List[Dict[str, Any]]:
    """
    Efficiently fetches contacts from the database who might need scheduled emails
    in the current active window using a single optimized query.
    Uses in-memory cache for better performance.
    """
    active_window_end = today + timedelta(days=lookahead_days)
    lookback_window_start = today - timedelta(days=lookback_days)
    
    # Check if any AEP campaign is active for scheduling
    is_aep_scheduling_active = _is_any_aep_campaign_active(today)
    
    # If AEP is active and we want to include all contacts, then get them all
    if is_aep_scheduling_active and include_all_for_aep:
        logger.info("AEP campaign active: fetching ALL contacts regardless of birth/effective dates")
        sql = """
        SELECT id, email, zip_code, birth_date, effective_date, phone_number,
               first_name, last_name
        FROM contacts
        """
    else:
        # Format dates for SQL
        today_str = today.strftime("%Y-%m-%d")
        future_end_str = active_window_end.strftime("%Y-%m-%d") 
        past_start_str = lookback_window_start.strftime("%Y-%m-%d")
        
        # Optimized single query using window functions and proper indexing
        sql = f"""
        WITH RECURSIVE dates(date) AS (
            SELECT date('{past_start_str}')
            UNION ALL
            SELECT date(date, '+1 day')
            FROM dates
            WHERE date < '{future_end_str}'
        ),
        contact_dates AS (
            SELECT 
                c.id, c.email, c.zip_code, c.birth_date, c.effective_date, 
                c.phone_number, c.first_name, c.last_name,
                -- Calculate anniversary dates using ISO format date functions
                date(strftime('%Y', '{today_str}') || '-' || substr(birth_date, 6, 2) || '-' || substr(birth_date, 9, 2)) as this_year_birthday,
                date(strftime('%Y', '{today_str}') + 1 || '-' || substr(birth_date, 6, 2) || '-' || substr(birth_date, 9, 2)) as next_year_birthday,
                date(strftime('%Y', '{today_str}') || '-' || substr(effective_date, 6, 2) || '-' || substr(effective_date, 9, 2)) as this_year_effective_date,
                date(strftime('%Y', '{today_str}') + 1 || '-' || substr(effective_date, 6, 2) || '-' || substr(effective_date, 9, 2)) as next_year_effective_date
            FROM contacts c
            WHERE birth_date IS NOT NULL OR effective_date IS NOT NULL
        ),
        -- Get contacts who already have sent emails for their events using a window function
        sent_emails AS (
            SELECT DISTINCT contact_id, email_type, event_year
            FROM email_schedules
            WHERE status IN ('sent', 'processing')
        ),
        -- Calculate event dates and check for conflicts in a single pass
        event_dates AS (
            SELECT 
                cd.*,
                CASE 
                    WHEN this_year_birthday BETWEEN '{past_start_str}' AND '{future_end_str}' THEN this_year_birthday
                    WHEN next_year_birthday BETWEEN '{past_start_str}' AND '{future_end_str}' THEN next_year_birthday
                    ELSE NULL
                END as birthday_event_date,
                CASE 
                    WHEN this_year_effective_date BETWEEN '{past_start_str}' AND '{future_end_str}' THEN this_year_effective_date
                    WHEN next_year_effective_date BETWEEN '{past_start_str}' AND '{future_end_str}' THEN next_year_effective_date
                    ELSE NULL
                END as effective_date_event
            FROM contact_dates cd
        )
        SELECT 
            id, email, zip_code, birth_date, effective_date, 
            phone_number, first_name, last_name,
            this_year_birthday, next_year_birthday,
            this_year_effective_date, next_year_effective_date
        FROM event_dates ed
        WHERE 
            (birthday_event_date IS NOT NULL OR effective_date_event IS NOT NULL)
            -- Exclude contacts who already have sent emails for their events
            AND NOT EXISTS (
                SELECT 1 FROM sent_emails se
                WHERE se.contact_id = ed.id
                AND (
                    (se.email_type = 'BIRTHDAY' AND 
                     (se.event_year = strftime('%Y', birthday_event_date)))
                    OR
                    (se.email_type = 'EFFECTIVE_DATE' AND 
                     (se.event_year = strftime('%Y', effective_date_event)))
                )
            )
        """
    
    cursor = db_conn.cursor()
    
    # Get total number of contacts in the organization for daily cap calculation
    cursor.execute("SELECT COUNT(*) FROM contacts")
    row = cursor.fetchone()
    total_contacts = row[0] if row else 0   
    logger.info(f"Organization has {total_contacts} total contacts")
    
    # Execute the main query
    cursor.execute(sql)
    rows = cursor.fetchall()
    
    # Use explicit column names from our SQL query
    column_names = ['id', 'email', 'zip_code', 'birth_date', 'effective_date', 'phone_number', 
                   'first_name', 'last_name', 'this_year_birthday', 'next_year_birthday',
                   'this_year_effective_date', 'next_year_effective_date']
    contacts = []
    
    # Count contacts by event type
    birthday_contacts = set()
    effective_date_contacts = set()
    
    for row in rows:
        # Handle both tuple-like rows (SQLite/Turso) and dict-like rows (future compatibility)
        if hasattr(row, 'keys'):
            # Row is dict-like
            contact_dict = dict(row)
        else:
            # Row is tuple-like (which is the case with Turso)
            contact_dict = dict(zip(column_names, row))
        
        # Cache the contact data
        contact_id = contact_dict['id']
        scheduler_cache.set_contact(contact_id, contact_dict)
        
        # Track which type of event triggered this contact's inclusion
        if contact_dict.get('birth_date'):
            if (contact_dict.get('this_year_birthday') and 
                past_start_str <= contact_dict['this_year_birthday'] <= future_end_str):
                birthday_contacts.add(contact_id)
            if (contact_dict.get('next_year_birthday') and 
                past_start_str <= contact_dict['next_year_birthday'] <= future_end_str):
                birthday_contacts.add(contact_id)
        
        if contact_dict.get('effective_date'):
            if (contact_dict.get('this_year_effective_date') and 
                past_start_str <= contact_dict['this_year_effective_date'] <= future_end_str):
                effective_date_contacts.add(contact_id)
            if (contact_dict.get('next_year_effective_date') and 
                past_start_str <= contact_dict['next_year_effective_date'] <= future_end_str):
                effective_date_contacts.add(contact_id)
        
        # Remove the calculated date fields before adding to contacts list
        for field in ['this_year_birthday', 'next_year_birthday', 
                     'this_year_effective_date', 'next_year_effective_date']:
            contact_dict.pop(field, None)
        
        contacts.append(contact_dict)
    
    # Log detailed statistics
    logger.info(f"Pre-filtered {len(contacts)} contacts with upcoming events in active window")
    logger.info(f"This is {len(contacts)/total_contacts*100:.1f}% of all contacts in the organization")
    logger.info(f"Contacts with birthdays in window: {len(birthday_contacts)} ({len(birthday_contacts)/total_contacts*100:.1f}%)")
    logger.info(f"Contacts with effective dates in window: {len(effective_date_contacts)} ({len(effective_date_contacts)/total_contacts*100:.1f}%)")
    logger.info(f"Contacts with both types in window: {len(birthday_contacts & effective_date_contacts)}")
    
    return contacts, total_contacts


async def identify_candidate_events(
    contacts: List[Dict[str, Any]], 
    today: date,
    lookahead_days: int,
    aep_campaigns: Dict[str, Dict[str, Any]] = aep_campaigns_config.get("aep_campaigns", {})
) -> List[EventData]:
    """
    For each pre-filtered contact, determine the precise annual event instances
    (Birthday, Effective Date, AEP) that are candidates for scheduling.
    
    Args:
        contacts: List of contact dictionaries
        today: Today's date (or test override)
        lookahead_days: Number of days to look ahead
        aep_campaigns: Dictionary of AEP campaign configurations
        
    Returns:
        List of EventData objects
    """
    active_window_end = today + timedelta(days=lookahead_days)
    candidate_events = []
    
    # Identify active AEP campaigns first
    active_aep_campaigns = {}
    for campaign_key, details in aep_campaigns.items():
        try:
            campaign_start_date = date.fromisoformat(details['send_window_start'])
            campaign_end_date = date.fromisoformat(details['send_window_end'])
            
            if _is_campaign_active_for_scheduling(campaign_start_date, campaign_end_date, today):
                active_aep_campaigns[campaign_key] = {
                    'start_date': campaign_start_date,
                    'end_date': campaign_end_date,
                    'description': details.get('description', '')
                }
        except (KeyError, ValueError) as e:
            logger.warning(f"Invalid campaign configuration for {campaign_key}: {e}")
    
    logger.info(f"Found {len(active_aep_campaigns)} active AEP campaigns for scheduling")
    
    for contact in contacts:
        contact_id = contact.get('id')
        email = contact.get('email')
        
        if not contact_id or not email:
            # More detailed logging to help diagnose issues
            if not contact_id:
                logger.warning(f"Contact missing id, keys available: {list(contact.keys())}")
            if not email:
                logger.warning(f"Contact missing email, keys available: {list(contact.keys())}")
            logger.warning(f"Contact: {contact}")
            continue
        
        # Parse dates safely
        birth_date_str = contact.get('birth_date')
        effective_date_str = contact.get('effective_date')
        
        birth_date = parse_date_flexible(birth_date_str) if birth_date_str else None
        effective_date = parse_date_flexible(effective_date_str) if effective_date_str else None
        
        # Process Birthday Events (this year and next)
        if birth_date:
            for year in [today.year, today.year + 1]:
                try:
                    # Handle Feb 29 birthdays in non-leap years
                    if birth_date.month == 2 and birth_date.day == 29:
                        is_leap_year = (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0)
                        if not is_leap_year:
                            event_date = date(year, 2, 28)  # Use Feb 28 in non-leap years
                        else:
                            event_date = date(year, 2, 29)
                    else:
                        event_date = date(year, birth_date.month, birth_date.day)
                    
                    # Calculate ideal send date (default: 14 days before birthday)
                    ideal_send_date = event_date - timedelta(days=BIRTHDAY_OFFSET)
                    
                    # Only include if ideal send date is within active window
                    if today <= ideal_send_date <= active_window_end:
                        candidate_events.append(EventData(
                            contact_dict=contact,
                            event_type="BIRTHDAY",
                            event_year=event_date.year,
                            event_month=event_date.month,
                            event_day=event_date.day,
                            ideal_send_date=ideal_send_date
                        ))
                    # Also include events whose ideal send date is in the past,
                    # but their actual event date is still in the future
                    elif ideal_send_date < today and event_date > today:
                        candidate_events.append(EventData(
                            contact_dict=contact,
                            event_type="BIRTHDAY",
                            event_year=event_date.year,
                            event_month=event_date.month,
                            event_day=event_date.day,
                            ideal_send_date=ideal_send_date  # Keep the ideal date, we'll adjust it later
                        ))
                except ValueError as e:
                    logger.error(f"Error calculating birthday event for contact {contact_id}: {e}")
        
        # Process Effective Date Events (this year and next)
        if effective_date:
            # Check if effective date is less than a year from today
            time_since_effective = (today - effective_date).days
            is_recent_enrollment = time_since_effective < 365
                
            for year in [today.year, today.year + 1]:
                try:
                    # Handle Feb 29 effective dates in non-leap years
                    if effective_date.month == 2 and effective_date.day == 29:
                        is_leap_year = (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0)
                        if not is_leap_year:
                            event_date = date(year, 2, 28)  # Use Feb 28 in non-leap years
                        else:
                            event_date = date(year, 2, 29)
                    else:
                        event_date = date(year, effective_date.month, effective_date.day)
                    
                    # Calculate ideal send date (default: 30 days before effective date anniversary)
                    ideal_send_date = event_date - timedelta(days=EFFECTIVE_DATE_OFFSET)
                    
                    # For recent enrollments: Add to candidate events with a flag for skipping
                    if is_recent_enrollment:
                        # Only add if the ideal send date would be in our scheduling window
                        if ((today <= ideal_send_date <= active_window_end) or 
                            (ideal_send_date < today and event_date > today)):
                            logger.info(f"Adding contact {contact_id} with effective date {effective_date} to be skipped (less than 1 year)")
                            # Using a special marker in the tuple to indicate this should be skipped
                            # We'll process this during validation to set the proper status
                            candidate_events.append(EventData(
                                contact_dict=contact,
                                event_type="EFFECTIVE_DATE",
                                event_year=event_date.year,
                                event_month=event_date.month,
                                event_day=event_date.day,
                                ideal_send_date=ideal_send_date,
                                is_pre_marked_skip=True,  # Extra flag indicating recent enrollment requiring skip
                                catchup_note=f"Less than one year since effective date ({time_since_effective} days)"
                            ))
                    # Normal processing for contacts with effective dates older than a year
                    else:
                        # Only include if ideal send date is within active window
                        if today <= ideal_send_date <= active_window_end:
                            candidate_events.append(EventData(
                                contact_dict=contact,
                                event_type="EFFECTIVE_DATE",
                                event_year=event_date.year,
                                event_month=event_date.month,
                                event_day=event_date.day,
                                ideal_send_date=ideal_send_date
                            ))
                        # Also include events whose ideal send date is in the past,
                        # but their actual event date is still in the future
                        elif ideal_send_date < today and event_date > today:
                            candidate_events.append(EventData(
                                contact_dict=contact,
                                event_type="EFFECTIVE_DATE",
                                event_year=event_date.year,
                                event_month=event_date.month,
                                event_day=event_date.day,
                                ideal_send_date=ideal_send_date  # Keep the ideal date, we'll adjust it later
                            ))
                except ValueError as e:
                    logger.error(f"Error calculating effective date event for contact {contact_id}: {e}")
        
        # Process AEP Events for all active campaigns
        for campaign_key, campaign_details in active_aep_campaigns.items():
            campaign_start_date = campaign_details['start_date']
            campaign_end_date = campaign_details['end_date']
            
            # Extract the year from the campaign key (e.g., AEP_PRE_ANNOUNCEMENT_2024 -> 2024)
            # Use campaign start year as fallback
            try:
                event_year = int(campaign_key.split('_')[-1])
            except (ValueError, IndexError):
                event_year = campaign_start_date.year
            
            # Use a hash of the contact_id and campaign_key to distribute 
            # the ideal send date across the campaign window
            hash_input = f"{contact_id}_{campaign_key}"
            hash_value = int(hashlib.md5(hash_input.encode()).hexdigest(), 16)
            
            # Calculate days between start and end dates (inclusive)
            campaign_days = (campaign_end_date - campaign_start_date).days + 1
            
            # Distribute across the campaign window
            days_offset = hash_value % campaign_days
            ideal_send_date = campaign_start_date + timedelta(days=days_offset)
            
            # Only include if the ideal send date is within our active window
            if today <= ideal_send_date <= active_window_end:
                candidate_events.append(EventData(
                    contact_dict=contact,
                    event_type=campaign_key,  # Use the full campaign key as the event type
                    event_year=event_year,
                    event_month=campaign_start_date.month,  # Use campaign start month for keying
                    event_day=campaign_start_date.day,    # Use campaign start day for keying
                    ideal_send_date=ideal_send_date
                ))
    
    logger.info(f"Identified {len(candidate_events)} candidate events for {len(contacts)} contacts")
    return candidate_events


async def calculate_target_send_dates(
    candidate_events: List[EventData],
    today: date,
    total_contacts: int,
    db_conn: Optional[Connection] = None,
    daily_send_percentage_cap: float = DAILY_SEND_PERCENTAGE_CAP,
    catch_up_spread_days: int = CATCH_UP_SPREAD_DAYS,
    ed_smoothing_window_days: int = ED_SMOOTHING_WINDOW_DAYS,
    ed_daily_soft_limit: int = ED_DAILY_SOFT_LIMIT
) -> List[EventData]:
    """
    Calculate final target send dates for candidate events using optimized SQL queries.
    """
    # Calculate max emails per day based on org size (soft cap)
    max_emails_per_day = ceil(total_contacts * daily_send_percentage_cap)
    logger.info(f"Soft daily cap: {max_emails_per_day} emails per day ({daily_send_percentage_cap*100:.1f}% of contacts)")
    
    # If we have a database connection and frequency checks are enabled, do a batch check
    frequency_skip_map = {}
    if db_conn and RESPECT_90_DAY_WINDOW:
        contact_events_for_sql = [] # Renamed to avoid confusion with outer scope if any
        logger.info(f"Building contact_events_for_sql. Input candidate_events count: {len(candidate_events)}")
        
        for i, current_event in enumerate(candidate_events):
            contact_id_initial = current_event.contact_id
            event_type_initial = current_event.event_type

            # Check for pre-marked skips (e.g., recent enrollment from identify_candidate_events)
            if current_event.is_pre_marked_skip:
                continue # Skip adding to contact_events_for_sql
            
            if contact_id_initial and not should_bypass_frequency_rules(event_type_initial):
                event_date = current_event.event_date
                contact_events_for_sql.append((contact_id_initial, event_type_initial, event_date, current_event.ideal_send_date))

        if contact_events_for_sql:
            try:
                cursor = db_conn.cursor()
                values_clause = ','.join(
                    f"({c[0]}, '{c[1]}', '{c[2].isoformat()}', '{c[3].isoformat()}')"
                    for c in contact_events_for_sql # Use the filtered list here
                )
                # ... (rest of the SQL query using contact_events_for_sql and its processing to populate frequency_skip_map)
                # The DEBUG_CTS_SQL_PROCESS logs are expected to be within this try block, after cursor.fetchall()
                script = f"""
                WITH contact_events(contact_id, event_type, event_date, ideal_send_date) AS (
                    VALUES {values_clause}
                ),
                recent_emails AS (
                    SELECT 
                        ce.contact_id,
                        ce.event_type,
                        ce.event_date,
                        ce.ideal_send_date,
                        es.scheduled_send_date as recent_email_date,
                        es.email_type as recent_email_type,
                        julianday(ce.event_date) - julianday(es.scheduled_send_date) as days_between
                    FROM contact_events ce
                    LEFT JOIN email_schedules es ON 
                        es.contact_id = ce.contact_id AND
                        es.scheduled_send_date BETWEEN 
                            date(ce.event_date, '-{RECENT_EMAIL_CHECK_DAYS} days') AND 
                            ce.event_date AND
                        es.status IN ('sent', 'delivered', 'accepted', 'processing', 'scheduled', 'pre-scheduled')
                    ORDER BY es.scheduled_send_date DESC
                )
                SELECT 
                    contact_id,
                    event_type,
                    event_date,
                    ideal_send_date,
                    recent_email_date,
                    recent_email_type,
                    days_between,
                    CASE 
                        WHEN recent_email_date IS NULL THEN 0
                        WHEN days_between > {LONG_WINDOW_THRESHOLD_DAYS} THEN 1
                        WHEN days_between <= {SHORT_WINDOW_ADJUSTMENT_DAYS} THEN 0
                        ELSE 1
                    END as should_skip,
                    CASE 
                        WHEN recent_email_date IS NULL THEN NULL
                        WHEN days_between <= {SHORT_WINDOW_ADJUSTMENT_DAYS} THEN NULL
                        ELSE date(event_date, '-{SHORT_WINDOW_ADJUSTMENT_DAYS} days')
                    END as adjusted_date
                FROM recent_emails;
                """
                
                cursor.execute(script)
                rows = cursor.fetchall()
                logger.info(f"Executing frequency check SQL with {len(contact_events_for_sql)} contacts")
                logger.info(f"SQL params - RECENT_EMAIL_CHECK_DAYS: {RECENT_EMAIL_CHECK_DAYS}, LONG_WINDOW_THRESHOLD_DAYS: {LONG_WINDOW_THRESHOLD_DAYS}, SHORT_WINDOW_ADJUSTMENT_DAYS: {SHORT_WINDOW_ADJUSTMENT_DAYS}")
                logger.info(f"Fetched {len(rows)} rows from frequency check query")
                
                if rows:
                    # Process results into a map
                    for row_data_from_sql in rows: # Use a distinct name
                        contact_id_from_sql, event_type_from_sql, _, _, recent_email_date_from_sql, recent_email_type_from_sql, days_between_from_sql, should_skip_from_sql, adjusted_date_from_sql = row_data_from_sql
                        
                        current_should_skip_bool = bool(should_skip_from_sql)
                        current_key = (contact_id_from_sql, event_type_from_sql)

                        if current_should_skip_bool:
                            skip_reason = f"Contact received {recent_email_type_from_sql} email on {recent_email_date_from_sql} within {RECENT_EMAIL_CHECK_DAYS}-day window (days between: {days_between_from_sql:.1f})"
                            frequency_skip_map[current_key] = (True, skip_reason)
                        elif adjusted_date_from_sql:
                            frequency_skip_map[current_key] = (False, adjusted_date_from_sql)
                
            except Exception as e:
                logger.error(f"Error in batch frequency check: {e}")
    
    # Track daily targets for effective date smoothing and caps
    daily_ed_targets = defaultdict(int)
    daily_total_targets = defaultdict(int)
    result_events = []
    
    # First pass: Count events by type and date
    for event in candidate_events:
        if event.is_pre_marked_skip:
            continue  # Skip pre-marked events
        
        # Check frequency skip map
        key = (event.contact_id, event.event_type)
        is_event_for_debug = (event.contact_id == 4924 and event.event_type == "BIRTHDAY")

        ideal_send_date = event.ideal_send_date
        if key in frequency_skip_map:
            is_flagged_to_skip, result_from_map = frequency_skip_map[key]
            if is_event_for_debug:
                logger.info(f"DEBUG_CTS_SECOND_PASS: Contact 4924 BIRTHDAY: Found in frequency_skip_map. is_flagged_to_skip: {is_flagged_to_skip}, result_from_map: '{result_from_map}'")

            if is_flagged_to_skip:
                continue  # Skip counting for skipped events
            elif isinstance(result_from_map, str):  # Adjusted date
                ideal_send_date = date.fromisoformat(result_from_map)
                if is_event_for_debug:
                    logger.info(f"DEBUG_CTS_SECOND_PASS: Contact 4924 BIRTHDAY: ideal_send_date adjusted to: {ideal_send_date}")
        elif is_event_for_debug:
            logger.info(f"DEBUG_CTS_SECOND_PASS: Contact 4924 BIRTHDAY: NOT found in frequency_skip_map.")

        # Count by type and total
        if event.event_type == "EFFECTIVE_DATE":
            daily_ed_targets[ideal_send_date] += 1
        daily_total_targets[ideal_send_date] += 1

    logger.info(f"DEBUG_CTS_SECOND_PASS: Daily ED targets: {daily_ed_targets}")
    logger.info(f"DEBUG_CTS_SECOND_PASS: Daily total targets: {daily_total_targets}")
    
    # Identify days that exceed the cap
    over_limit_days = {day for day, count in daily_total_targets.items() 
                      if count > max_emails_per_day * 1.2}  # Allow 20% overage
    
    # Second pass: Apply smoothing and caps
    for event in candidate_events:
        if event.is_pre_marked_skip:
            result_events.append(event) 
            if event.contact_id == 4924 and event.event_type == "BIRTHDAY":
                 logger.info(f"DEBUG_CTS_SECOND_PASS: Contact 4924 BIRTHDAY: Found pre-marked (recent enrollment) event: {event}")
            continue
        
        # Check frequency skip map
        key = (event.contact_id, event.event_type)
        is_event_for_debug = (event.contact_id == 4924 and event.event_type == "BIRTHDAY")

        ideal_send_date = event.ideal_send_date
        if key in frequency_skip_map:
            is_flagged_to_skip, result_from_map = frequency_skip_map[key]
            if is_event_for_debug:
                logger.info(f"DEBUG_CTS_SECOND_PASS: Contact 4924 BIRTHDAY: Found in frequency_skip_map. is_flagged_to_skip: {is_flagged_to_skip}, result_from_map: '{result_from_map}'")

            if is_flagged_to_skip:
                # Mark event as skipped
                event.mark_as_skipped(result_from_map)
                result_events.append(event)
                if is_event_for_debug:
                    logger.info(f"DEBUG_CTS_SECOND_PASS: Contact 4924 BIRTHDAY: Appended SKIPPED event to result_events: {event}")
                continue
            elif isinstance(result_from_map, str):  # Adjusted date
                ideal_send_date = date.fromisoformat(result_from_map)
                event.ideal_send_date = ideal_send_date  # Update the event's ideal send date
                if is_event_for_debug:
                    logger.info(f"DEBUG_CTS_SECOND_PASS: Contact 4924 BIRTHDAY: ideal_send_date adjusted to: {ideal_send_date}")
        elif is_event_for_debug:
            logger.info(f"DEBUG_CTS_SECOND_PASS: Contact 4924 BIRTHDAY: NOT found in frequency_skip_map.")

        # Default target is the ideal date
        target_send_date = ideal_send_date
        
        # Apply catch-up logic for past-due ideal dates
        if ideal_send_date < today and event.event_date >= today:
            hash_input = f"{event.contact_id}_{event.event_type}_{event.event_year}"
            hash_value = int(hashlib.md5(hash_input.encode()).hexdigest(), 16)
            days_offset = hash_value % catch_up_spread_days
            target_send_date = today + timedelta(days=days_offset)
        
        # Apply effective date smoothing
        elif event.event_type == "EFFECTIVE_DATE" and not ideal_send_date < today:
            daily_count = daily_ed_targets[ideal_send_date]
            ed_threshold = min(ed_daily_soft_limit, int(max_emails_per_day * 0.3))
            
            logger.debug(f"ED Smoothing - Contact {event.contact_id}: Daily count {daily_count} vs threshold {ed_threshold}")
            
            if daily_count > ed_threshold:
                hash_input = f"{event.contact_id}_{event.event_type}_{event.event_year}"
                hash_value = int(hashlib.md5(hash_input.encode()).hexdigest(), 16)
                half_window = ed_smoothing_window_days // 2
                jitter = (hash_value % ed_smoothing_window_days) - half_window
                potential_date = ideal_send_date + timedelta(days=jitter)
                
                logger.debug(f"ED Smoothing - Contact {event.contact_id}: Original date {ideal_send_date}, jitter {jitter}, potential date {potential_date}")
                
                if potential_date >= today:
                    target_send_date = potential_date
                    logger.debug(f"ED Smoothing - Contact {event.contact_id}: Using smoothed date {target_send_date}")
        
        # Apply global daily cap
        if target_send_date in over_limit_days:
            next_day = target_send_date + timedelta(days=1)
            # Ensure next_day is not already over limit (or less over limit)
            # and avoid pushing too far if multiple days are capped.
            current_day_overage = daily_total_targets.get(target_send_date, 0) - max_emails_per_day
            next_day_overage = daily_total_targets.get(next_day, 0) - max_emails_per_day
                
            if next_day_overage < current_day_overage and next_day_overage < (max_emails_per_day * 0.2): # Only move if next day is better and not excessively over
                    target_send_date = next_day
            daily_total_targets[target_send_date - timedelta(days=1)] -= 1 # Decrement count from original day
            daily_total_targets[target_send_date] += 1 # Increment count for new day
            if is_event_for_debug:
                logger.info(f"DEBUG_CTS_SECOND_PASS: Contact 4924 BIRTHDAY: Global daily cap moved target_send_date to: {target_send_date}")
        
        # Update the event with the calculated target send date
        event.target_send_date = target_send_date
        result_events.append(event)
        if is_event_for_debug:
            logger.info(f"DEBUG_CTS_SECOND_PASS: Contact 4924 BIRTHDAY: Appended NON-SKIPPED event to result_events: {event}")
    
    return result_events


async def resolve_birthday_effective_date_conflicts(
    events_with_target_dates: List[EventData],
    test_date_override: Optional[date] = None,
    conflict_window_days: int = 90,
    db_conn: Optional[Connection] = None
) -> List[EventData]:
    """
    Resolve conflicts between emails within a 90-day window.
    
    Conflict resolution rules:
    1. If neither is scheduled yet, prioritize birthday over other emails
    2. If one is already scheduled (pre-scheduled status), keep that one and skip the new one
    3. For non-birthday/non-effective date emails, check based on campaign configuration
    
    Args:
        events_with_target_dates: List of event tuples from calculate_target_send_dates
        test_date_override: Optional date to use for testing
        conflict_window_days: Number of days to consider for conflict window (default: 90)
        db_conn: Database connection for checking already scheduled emails
        
    Returns:
        List of EventData objects with conflicts resolved, marking some for skipping
    """
    today = test_date_override or date.today()
    result_events = []
    
    # Group events by contact_id
    contact_events = defaultdict(list)
    for event in events_with_target_dates:
        # Skip events that are already marked for skipping
        if event.is_pre_marked_skip:
            result_events.append(event)
            continue
            
        contact_id = event.contact_id
        contact_events[contact_id].append(event)
    
    # Check for already scheduled emails in the database
    already_scheduled = {}
    if db_conn:
        try:
            cursor = db_conn.cursor()
            
            # Get all contact IDs to check
            contact_ids = list(contact_events.keys())
            
            # Process in batches to avoid too large query parameters 
            # SQLite parameter limit is 999, so 500 is safe
            batch_size = 500
            for i in range(0, len(contact_ids), batch_size):
                batch = contact_ids[i:i+batch_size]
                
                # Convert to string for SQL query
                # Turso/SQLite doesn't support parameter arrays, so we construct the list
                contact_ids_str = ','.join(str(id) for id in batch)
                
                # Query for already scheduled emails (not skipped ones)
                query = f"""
                SELECT contact_id, email_type, event_year, scheduled_send_date
                FROM email_schedules
                WHERE contact_id IN ({contact_ids_str})
                AND status = 'pre-scheduled'
                """
                
                cursor.execute(query)
                rows = cursor.fetchall()
                
                for row in rows:
                    # Handle both dict-like and tuple-like results
                    if hasattr(row, 'keys'):
                        contact_id = row['contact_id']
                        email_type = row['email_type']
                        event_year = row['event_year']
                        scheduled_date_str = row['scheduled_send_date']
                    else:
                        contact_id, email_type, event_year, scheduled_date_str = row
                    
                    # Parse the scheduled date
                    if scheduled_date_str:
                        try:
                            scheduled_date = datetime.fromisoformat(scheduled_date_str.replace('Z', '+00:00')).date()
                        except ValueError:
                            scheduled_date = today  # Default to today if parsing fails
                    else:
                        scheduled_date = today
                    
                    # Store the already scheduled email info
                    if contact_id not in already_scheduled:
                        already_scheduled[contact_id] = []
                        
                    already_scheduled[contact_id].append({
                        'email_type': email_type,
                        'event_year': event_year,
                        'scheduled_date': scheduled_date
                    })
                
            logger.info(f"Found {sum(len(emails) for emails in already_scheduled.values())} already scheduled birthday/effective date emails")
            
        except Exception as e:
            logger.error(f"Error checking for already scheduled emails: {e}")
    
    # Process each contact's events to resolve conflicts
    conflicts_resolved = 0
    
    for contact_id, events in contact_events.items():
        # Group events by type
        birthday_events = [e for e in events if e.event_type == "BIRTHDAY"]
        effective_date_events = [e for e in events if e.event_type == "EFFECTIVE_DATE"]
        other_events = [e for e in events if e.event_type not in ("BIRTHDAY", "EFFECTIVE_DATE")]
        
        # Quick-path if there's only one event type (no conflicts possible)
        if len(events) == 1 or (not birthday_events and not effective_date_events and len(other_events) <= 1):
            # Add all events to result unchanged
            result_events.extend(events)
            continue
        
        # Check already scheduled emails for this contact
        scheduled_emails = already_scheduled.get(contact_id, [])
        scheduled_email_types = {e['email_type'] for e in scheduled_emails}
        
        # Collect all target dates by email type
        all_target_dates = {}
        for event in events:
            event_type = event.event_type
            target_date = event.target_send_date
            if event_type not in all_target_dates:
                all_target_dates[event_type] = []
            all_target_dates[event_type].append(target_date)
        
        # Find conflicts between all email types
        conflict_pairs = []
        checked_pairs = set()
        
        # Check for conflicts between all pairs of email types
        for type1, dates1 in all_target_dates.items():
            for type2, dates2 in all_target_dates.items():
                # Skip self-comparisons and already checked pairs
                if type1 == type2 or (type1, type2) in checked_pairs or (type2, type1) in checked_pairs:
                    continue
                    
                # Mark this pair as checked
                checked_pairs.add((type1, type2))
                
                # Check all date combinations for conflicts
                for date1 in dates1:
                    for date2 in dates2:
                        days_diff = abs((date1 - date2).days)
                        if days_diff <= conflict_window_days:
                            conflict_pairs.append((type1, type2))
                            break
                    if (type1, type2) in conflict_pairs:
                        break
        
        # If there are no conflicts, add all events unchanged
        if not conflict_pairs:
            result_events.extend(events)
            continue
            
        # Conflict resolution logic
        events_to_keep = set()
        events_to_skip = set()
        
        # Priority order: BIRTHDAY > EFFECTIVE_DATE > other email types
        # First, handle already scheduled emails - they take precedence
        if scheduled_email_types:
            for type1, type2 in conflict_pairs:
                if type1 in scheduled_email_types:
                    # Type1 is already scheduled, so skip Type2
                    events_to_keep.add(type1)
                    events_to_skip.add(type2)
                elif type2 in scheduled_email_types:
                    # Type2 is already scheduled, so skip Type1
                    events_to_keep.add(type2)
                    events_to_skip.add(type1)
        
        # Then, handle new conflicts with priority ordering
        for type1, type2 in conflict_pairs:
            # Skip if either type is already decided
            if type1 in events_to_keep or type1 in events_to_skip or type2 in events_to_keep or type2 in events_to_skip:
                continue
                
            # Apply priority rules
            if type1 == "BIRTHDAY" and type2 != "BIRTHDAY":
                events_to_keep.add(type1)
                events_to_skip.add(type2)
            elif type2 == "BIRTHDAY" and type1 != "BIRTHDAY":
                events_to_keep.add(type2)
                events_to_skip.add(type1)
            elif type1 == "EFFECTIVE_DATE" and type2 not in ("BIRTHDAY", "EFFECTIVE_DATE"):
                events_to_keep.add(type1)
                events_to_skip.add(type2)
            elif type2 == "EFFECTIVE_DATE" and type1 not in ("BIRTHDAY", "EFFECTIVE_DATE"):
                events_to_keep.add(type2)
                events_to_skip.add(type1)
            else:
                # For other types or same priority, keep the one with an earlier target date
                date1 = min(all_target_dates[type1])
                date2 = min(all_target_dates[type2])
                if date1 <= date2:
                    events_to_keep.add(type1)
                    events_to_skip.add(type2)
                else:
                    events_to_keep.add(type2)
                    events_to_skip.add(type1)
        
        # Process each event based on the resolution
        for event in events:
            event_type = event.event_type
            
            if event_type in events_to_skip:
                # Create skip reason based on what this conflicts with
                conflict_types = []
                for type1, type2 in conflict_pairs:
                    if event_type == type1 and type2 in events_to_keep:
                        conflict_types.append(type2)
                    elif event_type == type2 and type1 in events_to_keep:
                        conflict_types.append(type1)
                
                conflict_reason = f"Conflict with {', '.join(conflict_types)} email(s) within {conflict_window_days}-day window"
                
                # Mark event for skipping
                event.mark_as_skipped(conflict_reason)
                result_events.append(event)
                conflicts_resolved += 1
            else:
                # Keep this event unchanged
                result_events.append(event)
        
        # Log the resolution
        logger.info(f"Contact {contact_id}: Resolved conflicts - keeping {events_to_keep}, skipping {events_to_skip}")
    
    # Add any remaining events from contacts without both types of events
    logger.info(f"Resolved {conflicts_resolved} birthday/effective date conflicts within {conflict_window_days}-day window")
    
    return result_events

async def validate_with_contact_rule_engine(
    candidate_events_with_target_dates: List[EventData],
    test_date_override: Optional[date] = None,
    db_conn: Optional[Connection] = None
) -> List[EventData]:
    """
    Validate each candidate event using the ContactRuleEngine to respect
    state-specific rules and exclusion windows.
    
    Also applies the recent email + eligibility check filter if enabled.
    
    Args:
        candidate_events_with_target_dates: List of event tuples from calculate_target_send_dates
        test_date_override: Optional date to use for testing
        db_conn: Database connection for additional checks (optional)
        
    Returns:
        List of EventData objects with added validation results:
        (contact_dict, event_type, event_year, event_month, event_day, ideal_send_date, 
         target_send_date, final_status, final_reason, final_db_send_date)
    """
    # Initialize ContactRuleEngine
    rule_engine = ContactRuleEngine(test_date=test_date_override)
    validated_events = []
    
    # Map of contact_id -> (should_skip, reason) for the eligibility check
    # We'll prefill this with batch queries for better performance
    eligibility_skip_map = {}
    
    # Perform batch eligibility check if enabled
    if ELIGIBILITY_CHECK_ENABLED and db_conn:
        eligibility_skip_map = await _perform_batch_eligibility_check(
            candidate_events_with_target_dates, 
            db_conn
        )
    
    # Now process each event, using the pre-computed eligibility check results
    for event in candidate_events_with_target_dates:
        # Check if this is an event pre-marked for skipping (recent enrollment)
        if event.is_pre_marked_skip:
            # Skip rule engine validation and directly add as skipped
            logger.info(f"Adding pre-marked skipped event for contact {event.contact_id}: {event.event_type}, reason: {event.catchup_note}")
            event.mark_as_skipped(event.catchup_note or "Pre-marked for skipping")
            validated_events.append(event)
            continue
            
        contact_id = event.contact_id
        email = event.contact_dict.get('email')
        zip_code = event.contact_dict.get('zip_code')
        phone_number = event.contact_dict.get('phone_number')
        
        # Create a specific event instance for this exact anniversary 
        # (use the event's specific date, not the original birth/effective date)
        event_date = event.event_date
        
        # Check if we should apply the eligibility check filter
        should_apply_filter = False
        if ELIGIBILITY_CHECK_ENABLED:
            # Check if this event type is eligible for this filter
            if event.event_type in APPLY_ELIGIBILITY_CHECK_TO and APPLY_ELIGIBILITY_CHECK_TO[event.event_type]:
                should_apply_filter = True
            # For AEP campaigns, check the campaign-specific settings
            elif event.event_type.startswith("AEP_") and event.event_type in campaign_eligibility_config:
                should_apply_filter = campaign_eligibility_config.get(event.event_type, {}).get("apply_eligibility_check", False)
        
        # If filter should be applied and this contact might need skipping
        if should_apply_filter and contact_id in eligibility_skip_map:
            # Get the pre-computed eligibility check result
            should_skip_due_to_eligibility, eligibility_skip_reason = eligibility_skip_map[contact_id]
            
            if should_skip_due_to_eligibility:
                # Skip this event due to recent eligibility response
                logger.info(f"Skipping {event.event_type} email for contact {contact_id} due to recent eligibility activity: {eligibility_skip_reason}")
                event.mark_as_skipped(eligibility_skip_reason)
                validated_events.append(event)
                continue
        
        # For ContactRuleEngine validation, we need to create ScheduleContactDetail
        # with the specific event instance date as the birthday/effective_date
        # depending on the event type
        if event.event_type == "BIRTHDAY":
            # Note: We're using the specific event date as the birthday
            # This is different from the original contact's birthday
            contact_detail = ScheduleContactDetail(
                email=email,
                zip_code=zip_code,
                birthday=event_date,
                effective_date=parse_date_flexible(event.contact_dict.get('effective_date')) if event.contact_dict.get('effective_date') else None,
                phone_number=phone_number
            )
        elif event.event_type == "EFFECTIVE_DATE":
            # Note: We're using the specific event date as the effective_date
            # This is different from the original contact's effective_date
            contact_detail = ScheduleContactDetail(
                email=email,
                zip_code=zip_code,
                birthday=parse_date_flexible(event.contact_dict.get('birth_date')) if event.contact_dict.get('birth_date') else None,
                effective_date=event_date,
                phone_number=phone_number
            )
        elif event.event_type.startswith("AEP_"):
            # AEP emails don't rely on state exclusion windows, so bypass rule engine validation
            # and use the target send date directly, but ensure it's not in the past
            today_for_check = test_date_override or date.today()
            final_send_date = event.target_send_date if event.target_send_date >= today_for_check else today_for_check
            event.final_send_date = final_send_date
            event.mark_as_scheduled(final_send_date)
            event.catchup_note = f"AEP campaign: {event.event_type}"
            validated_events.append(event)
            continue
        else:
            # Unexpected event type
            logger.warning(f"Skipping unexpected event type: {event.event_type} for contact {contact_id}")
            continue
        
        # Load dates through the rule engine
        try:
            full_schedule_output = await rule_engine.load_dates(contact_detail)
            
            # Find the corresponding EmailSchedule item for our event type
            if event.event_type == "BIRTHDAY":
                matching_email = next((email for email in full_schedule_output.emails 
                                    if email.email_type == EmailType.BIRTHDAY), None)
            elif event.event_type == "EFFECTIVE_DATE":
                matching_email = next((email for email in full_schedule_output.emails 
                                    if email.email_type == EmailType.EFFECTIVE_DATE), None)
            else:
                matching_email = None
            
            post_window_email = next((email for email in full_schedule_output.emails 
                                    if email.email_type == EmailType.POST_WINDOW), None)
            
            if matching_email:
                # Determine final status
                final_status = 'skipped' if matching_email.skipped else 'pre-scheduled'
                final_reason = matching_email.reason
                
                # Use the target_send_date from the catch-up logic instead of the rule engine date
                # This ensures we never schedule emails in the past
                today_for_check = test_date_override or date.today()
                final_db_send_date = event.target_send_date if event.target_send_date >= today_for_check else matching_email.scheduled_date
                
                # Update the event with validation results
                event.status = final_status
                event.skip_reason = final_reason
                event.final_send_date = final_db_send_date
                validated_events.append(event)
                
                # If this event is skipped and there's a post-window email, add it as well
                if matching_email.skipped and post_window_email:
                    # Create a new post-window event with the original event's year as reference
                    # This is so we can properly track it as linked to the original event instance
                    post_window_date = post_window_email.scheduled_date
                    
                    # Ensure post-window date is never in the past
                    today_for_check = test_date_override or date.today()
                    if post_window_date < today_for_check:
                        post_window_date = today_for_check + timedelta(days=1)  # Schedule for tomorrow if it's in the past
                    
                    post_window_event = EventData(
                        contact_dict=event.contact_dict,
                        event_type=f"POST_WINDOW_{event.event_type}",  # e.g., "POST_WINDOW_BIRTHDAY"
                        event_year=event.event_year,  # Use the original event's year for unique constraint
                        event_month=post_window_date.month,
                        event_day=post_window_date.day,
                        ideal_send_date=post_window_date,  # Ideal = actual for post-window
                        target_send_date=post_window_date,  # Target = actual for post-window
                        final_send_date=post_window_date,
                        status='pre-scheduled',
                        catchup_note=final_reason
                    )
                    validated_events.append(post_window_event)
            else:
                logger.warning(f"No matching email for event type {event.event_type} in rule engine output for contact {contact_id}")
                
        except Exception as e:
            logger.error(f"Error validating contact {contact_id} with rule engine: {e}")
    
    return validated_events


async def _batch_cache_eligibility_results(db_conn: Connection, contact_ids: List[int]) -> None:
    """
    Pre-cache eligibility results for a batch of contacts.
    Uses a single optimized query to fetch all eligibility results at once.
    """
    if not contact_ids:
        return
        
    try:
        cursor = db_conn.cursor()
        
        # Calculate cutoff date for eligibility events
        cutoff_date = (datetime.now() - timedelta(days=RECENT_EMAIL_CHECK_DAYS))
        cutoff_date_iso = cutoff_date.isoformat()
        
        # Build a single efficient query using executescript
        script = f"""
        BEGIN TRANSACTION;
        
        WITH contact_ids(contact_id) AS (
            VALUES {','.join(f'({id})' for id in contact_ids)}
        )
        SELECT 
            ce.contact_id,
            ce.metadata,
            ce.created_at
        FROM contact_events ce
        INNER JOIN contact_ids ci ON ce.contact_id = ci.contact_id
        WHERE ce.event_type = 'eligibility_answered'
        AND ce.created_at > '{cutoff_date_iso}'
        ORDER BY ce.contact_id, ce.created_at DESC;
        
        COMMIT;
        """
        
        # Execute the script
        cursor.executescript(script)
        
        # Process results
        rows = cursor.fetchall()
        
        if not rows:
            logger.debug(f"No eligibility events found for batch of {len(contact_ids)} contacts")
            return
            
        # Group by contact_id and get the most recent event for each
        processed_contacts = set()
        for row in rows:
            try:
                # Extract data from row (compatible with both dict-like and tuple rows)
                if hasattr(row, 'keys'):
                    contact_id = row['contact_id']
                    metadata_str = row['metadata']
                    skip_timestamp = row['created_at']
                else:
                    contact_id, metadata_str, skip_timestamp = row
                    
                # Skip if we already processed this contact (we want the most recent)
                if contact_id in processed_contacts:
                    continue
                    
                processed_contacts.add(contact_id)
                
                # Format timestamp for human-readable display
                skip_date_str = _format_timestamp(skip_timestamp)
                
                # Parse metadata for medical condition status
                has_medical_conditions = _parse_eligibility_metadata(metadata_str)
                
                # Craft a detailed reason
                skip_reason = _create_eligibility_skip_reason(has_medical_conditions, skip_date_str)
                
                # Cache the result
                scheduler_cache.set_eligibility(contact_id, (True, skip_reason))
                
            except Exception as e:
                logger.warning(f"Error processing eligibility event row: {e}")
                continue
                
        # Log summary
        found_count = len(processed_contacts)
        logger.info(f"Pre-cached eligibility results for {found_count} contacts")
        
    except Exception as e:
        logger.error(f"Error in batch eligibility caching: {e}")

async def _batch_cache_recent_emails(db_conn: Connection, contact_ids: List[int]) -> None:
    """
    Pre-cache recent email data for a batch of contacts.
    Uses a single optimized query to fetch all recent emails at once.
    """
    if not contact_ids:
        return
        
    try:
        cursor = db_conn.cursor()
        cutoff = (datetime.now() - timedelta(days=RECENT_EMAIL_CHECK_DAYS)).isoformat()
        
        # Build a single efficient query using executescript
        script = f"""
        BEGIN TRANSACTION;
        
        WITH contact_ids(contact_id) AS (
            VALUES {','.join(f'({id})' for id in contact_ids)}
        )
        SELECT 
            es.contact_id,
            es.email_type,
            es.scheduled_send_date
        FROM email_schedules es
        INNER JOIN contact_ids ci ON es.contact_id = ci.contact_id
        WHERE es.scheduled_send_date > '{cutoff}'
        AND es.status IN ('sent', 'delivered', 'accepted')
        ORDER BY es.contact_id, es.scheduled_send_date DESC;
        
        COMMIT;
        """
        
        # Execute the script
        cursor.executescript(script)
        
        # Process results
        rows = cursor.fetchall()
        
        if not rows:
            logger.debug(f"No recent emails found for batch of {len(contact_ids)} contacts")
            return
            
        # Group by contact_id and cache the emails
        emails_by_contact = defaultdict(list)
        for row in rows:
            try:
                # Extract data from row (compatible with both dict-like and tuple rows)
                if hasattr(row, 'keys'):
                    contact_id = row['contact_id']
                    email_type = row['email_type']
                    date_str = row['scheduled_send_date']
                else:
                    contact_id, email_type, date_str = row
                
                # Parse the date
                if date_str:
                    try:
                        email_date = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                        emails_by_contact[contact_id].append((email_type, email_date))
                    except ValueError:
                        continue
                        
            except Exception as e:
                logger.warning(f"Error processing recent email row: {e}")
                continue
        
        # Cache the results
        for contact_id, emails in emails_by_contact.items():
            scheduler_cache.set_recent_emails(contact_id, emails)
            
        # Log summary
        found_count = len(emails_by_contact)
        logger.info(f"Pre-cached recent emails for {found_count} contacts")
        
    except Exception as e:
        logger.error(f"Error in batch recent emails caching: {e}")

async def _perform_batch_eligibility_check(
    candidate_events: List[EventData],
    db_conn: Connection
) -> Dict[int, Tuple[bool, str]]:
    """
    Perform a batch check for eligibility events for all contacts in the candidate events.
    Uses batch caching for better performance.
    """
    start_time = datetime.now()
    eligibility_skip_map = {}
    
    try:
        # First, extract all contact IDs that need checking
        contacts_to_check = []
        eligible_event_types = set()
        
        # Process each event to determine which contacts need checking
        for event in candidate_events:
            try:
                # Handle pre-marked events
                if event.is_pre_marked_skip:
                    continue
                    
                # For normal events, check if the event type should be filtered
                contact_id = event.contact_id
                event_type = event.event_type
                
                if not contact_id:
                    logger.warning(f"Found event without valid contact ID: {event}")
                    continue
                
                # Check if this event type is eligible for the filter
                should_apply_filter = False
                if event_type in APPLY_ELIGIBILITY_CHECK_TO and APPLY_ELIGIBILITY_CHECK_TO[event_type]:
                    should_apply_filter = True
                    eligible_event_types.add(event_type)
                elif event_type.startswith("AEP_") and event_type in campaign_eligibility_config:
                    should_apply_filter = campaign_eligibility_config.get(event_type, {}).get("apply_eligibility_check", False)
                    if should_apply_filter:
                        eligible_event_types.add(event_type)
                    
                if should_apply_filter:
                    contacts_to_check.append(contact_id)
            except Exception as e:
                logger.warning(f"Error processing event for eligibility check: {e}")
                continue
        
        # Skip batch processing if no contacts to check
        if not contacts_to_check:
            logger.info("No contacts require eligibility checking - skipping batch query")
            return {}
        
        # Log information about what we're checking
        unique_contacts_to_check = list(set(contacts_to_check))
        logger.info(f"Running batch eligibility check for {len(unique_contacts_to_check)} contacts")
        logger.info(f"Eligible event types: {', '.join(sorted(eligible_event_types))}")
        
        # Pre-cache eligibility results and recent emails
        await _batch_cache_eligibility_results(db_conn, unique_contacts_to_check)
        await _batch_cache_recent_emails(db_conn, unique_contacts_to_check)
        
        # Now check each contact using the cached data
        for contact_id in unique_contacts_to_check:
            # Check cache first
            cached_result = scheduler_cache.get_eligibility(contact_id)
            if cached_result is not None:
                eligibility_skip_map[contact_id] = cached_result
                continue
                        
            # If not in cache, check recent emails
            recent_emails = scheduler_cache.get_recent_emails(contact_id)
            if recent_emails:
                # Sort by date descending to get most recent
                recent_emails.sort(key=lambda x: x[1], reverse=True)
                most_recent_email_type, most_recent_date = recent_emails[0]
                
                # Format the date
                skip_date_str = _format_timestamp(most_recent_date)
                
                # Create skip reason
                skip_reason = f"Contact received {most_recent_email_type} email on {skip_date_str}"
                eligibility_skip_map[contact_id] = (True, skip_reason)
                    
        # Log summary of results
        found_count = len(eligibility_skip_map)
        logger.info(f"Found {found_count} contacts with eligibility events in the last {RECENT_EMAIL_CHECK_DAYS} days")
            
        # Calculate hit rate
        hit_rate = (found_count / len(unique_contacts_to_check)) * 100 if unique_contacts_to_check else 0
        logger.info(f"Eligibility check hit rate: {hit_rate:.1f}%")
                
    except Exception as e:
        logger.error(f"Unexpected error in batch eligibility check: {type(e).__name__}: {e}")
    
    # Log performance metrics
    elapsed_ms = (datetime.now() - start_time).total_seconds() * 1000
    contacts_per_second = (len(contacts_to_check) / elapsed_ms) * 1000 if elapsed_ms > 0 else 0
    logger.info(f"Batch eligibility check completed in {elapsed_ms:.2f}ms " 
               f"({contacts_per_second:.1f} contacts/second)")
    
    return eligibility_skip_map


def _format_timestamp(timestamp) -> str:
    """Format a timestamp into a human-readable date string."""
    if isinstance(timestamp, str):
        try:
            dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            return timestamp
    else:
        return timestamp.strftime("%Y-%m-%d") if hasattr(timestamp, 'strftime') else str(timestamp)


def _parse_eligibility_metadata(metadata_str) -> Optional[bool]:
    """Parse eligibility metadata to determine if user has medical conditions."""
    has_medical_conditions = None
    try:
        if metadata_str:
            # Handle both string and dictionary formats
            if isinstance(metadata_str, str):
                metadata = json.loads(metadata_str)
            else:
                metadata = metadata_str
                
            # First check direct flag
            has_medical_conditions = metadata.get('has_medical_conditions')
            
            # Check alternative representations
            if has_medical_conditions is None:
                # Fallback to counting yes answers if available
                if 'main_questions_yes_count' in metadata:
                    has_medical_conditions = metadata.get('main_questions_yes_count', 0) > 0
                # Check for eligibility_questions data structure
                elif 'eligibility_questions' in metadata:
                    questions = metadata.get('eligibility_questions', [])
                    # If any question has a 'yes' answer, the contact has medical conditions
                    has_medical_conditions = any(
                        q.get('answer', '').lower() == 'yes' for q in questions 
                        if isinstance(q, dict)
                    )
                # Check for direct question answers in metadata root
                elif any(k.startswith('question_') for k in metadata.keys()):
                    has_medical_conditions = any(
                        v.lower() == 'yes' for k, v in metadata.items()
                        if k.startswith('question_') and isinstance(v, str)
                    )
    except (json.JSONDecodeError, TypeError, AttributeError) as e:
        logger.debug(f"Error parsing eligibility metadata: {e}")
    
    return has_medical_conditions


def _create_eligibility_skip_reason(has_medical_conditions: Optional[bool], date_str: str) -> str:
    """Create a human-readable reason for skipping based on eligibility response."""
    if has_medical_conditions is not None:
        condition_str = "with" if has_medical_conditions else "without"
        return f"Contact answered eligibility questions {condition_str} medical conditions on {date_str}"
    else:
        return f"Contact answered eligibility questions on {date_str}"


def should_bypass_frequency_rules(email_type: str) -> bool:
    """
    Determine if an email type should bypass the 90-day frequency rules.
    
    Args:
        email_type: The type of email to check
        
    Returns:
        True if the email should bypass rules, False otherwise
    """
    # Check campaign-specific overrides first
    if email_type in CAMPAIGN_FREQUENCY_OVERRIDES:
        return CAMPAIGN_FREQUENCY_OVERRIDES[email_type].get("bypass_frequency_rules", False)
    
    # Check AEP campaigns
    if email_type.startswith("AEP_"):
        campaign_key = email_type
        aep_campaigns = aep_campaigns_config.get("aep_campaigns", {})
        
        if campaign_key in aep_campaigns:
            # Check if this AEP campaign has a specific override
            campaign_data = aep_campaigns[campaign_key]
            return campaign_data.get("bypass_frequency_rules", False)
    
    # Default behavior respects the 90-day window
    return False


async def check_recent_emails(
    db_conn: Connection,
    contact_id: int,
    event_date: date,
    window_days: int = 90,
    email_type: str = None,
    bypass_window: bool = False
) -> Tuple[bool, Optional[str], Optional[date]]:
    """
    Check if a contact has received any email within the specified window
    before the event date (not send date).
    
    Args:
        db_conn: Database connection
        contact_id: Contact ID to check
        event_date: The actual event date (birthday, effective date, etc.)
        window_days: Number of days to look back from the event date
        email_type: The type of email being considered for sending
        bypass_window: If True, skip the check (for campaigns that override frequency rules)
        
    Returns:
        Tuple of (should_skip, reason_if_skipping, most_recent_email_date)
        - should_skip: True if this contact should be skipped based on recent emails
        - reason_if_skipping: String explaining why (None if not skipping)
        - most_recent_email_date: Date of most recent email (None if no recent emails)
    """
    # If bypass is set, skip the entire check
    if bypass_window:
        logger.info(f"Bypassing recent email check for contact {contact_id}, email type {email_type}")
        return False, None, None
        
    # Calculate start of window (going backwards from event_date)
    window_start = event_date - timedelta(days=window_days)
    window_start_str = window_start.isoformat()
    event_date_str = event_date.isoformat()
    
    try:
        # Get cursor
        cursor = db_conn.cursor()
        
        # Query for any emails sent or scheduled to be sent in the window
        query = """
            SELECT id, email_type, status, scheduled_send_date 
            FROM email_schedules
            WHERE contact_id = ? 
            AND scheduled_send_date BETWEEN ? AND ?
            AND status IN ('sent', 'delivered', 'accepted', 'processing', 'scheduled', 'pre-scheduled')
            ORDER BY scheduled_send_date DESC
            LIMIT 1
        """
        
        cursor.execute(query, (contact_id, window_start_str, event_date_str))
        row = cursor.fetchone()
        
        if not row:
            # No recent emails found
            return False, None, None
            
        # Extract data from the row (compatible with both dict-like and tuple rows)
        if hasattr(row, 'keys'):
            recent_email_id = row['id']
            recent_email_type = row['email_type']
            recent_email_status = row['status']
            recent_email_date_str = row['scheduled_send_date']
        else:
            recent_email_id, recent_email_type, recent_email_status, recent_email_date_str = row
        
        # Parse the date
        if recent_email_date_str:
            try:
                recent_email_date = date.fromisoformat(recent_email_date_str.split('T')[0])
            except ValueError:
                # If parsing fails, use a safe default
                recent_email_date = date.today()
        else:
            recent_email_date = date.today()
            
        # Calculate days between recent email and event
        days_to_event = (event_date - recent_email_date).days
        
        # Determine if we should skip based on proximity to event
        if days_to_event > LONG_WINDOW_THRESHOLD_DAYS:
            # Event is far enough in the future - skip this email
            skip_reason = f"Contact received {recent_email_type} email on {recent_email_date} within 90-day window before {event_date}"
            return True, skip_reason, recent_email_date
        elif days_to_event <= SHORT_WINDOW_ADJUSTMENT_DAYS:
            # Event is very close - don't skip, send it anyway
            return False, None, recent_email_date
        else:
            # Event is approaching but not immediate - provide info for adjustment
            # The calling function can decide to adjust the date or skip
            adjustment_info = f"Contact received {recent_email_type} email on {recent_email_date}, event on {event_date}"
            return True, adjustment_info, recent_email_date
            
    except Exception as e:
        logger.warning(f"Error checking recent emails for contact {contact_id}: {e}")
        # In case of error, don't skip (fail open to ensure important emails aren't missed)
        return False, None, None


async def adjust_send_date_for_frequency(
    db_conn: Connection,
    contact_id: int,
    email_type: str,
    event_date: date,
    ideal_send_date: date,
    window_days: int = 90
) -> Tuple[date, bool, Optional[str]]:
    """
    Adjusts the send date for an email based on frequency rules and recent emails.
    
    Args:
        db_conn: Database connection
        contact_id: Contact ID
        email_type: Type of email to send
        event_date: Actual date of the event
        ideal_send_date: Originally calculated ideal send date
        window_days: Window for checking recent emails (default 90 days)
        
    Returns:
        Tuple of (adjusted_send_date, should_skip, skip_reason)
        - adjusted_send_date: New send date (may be same as ideal if no adjustment needed)
        - should_skip: True if this email should be skipped entirely
        - skip_reason: Reason for skipping, if applicable
    """
    # Check if this email type should bypass frequency rules
    bypass_rules = should_bypass_frequency_rules(email_type)
    
    if not RESPECT_90_DAY_WINDOW or bypass_rules:
        # Don't apply frequency rules
        return ideal_send_date, False, None
    
    # Check for recent emails
    should_skip, reason, recent_date = await check_recent_emails(
        db_conn=db_conn,
        contact_id=contact_id,
        event_date=event_date,
        window_days=window_days,
        email_type=email_type,
        bypass_window=bypass_rules
    )
    
    if not should_skip or not recent_date:
        # No recent emails or explicit decision not to skip
        return ideal_send_date, False, None
        
    # If we're here, we have a recent email within the window
    
    # Calculate days between recent email and event
    days_to_event = (event_date - recent_date).days
    
    if days_to_event > LONG_WINDOW_THRESHOLD_DAYS:
        # Event is far in the future, skip this email
        return ideal_send_date, True, reason
    elif days_to_event <= SHORT_WINDOW_ADJUSTMENT_DAYS:
        # Event is very close, send it with original date
        return ideal_send_date, False, None
    else:
        # Event is approaching but not immediate
        # Try to find a date that's at least SHORT_WINDOW_ADJUSTMENT_DAYS before the event
        # but after the ideal send date to avoid too many emails close together
        latest_possible_date = event_date - timedelta(days=SHORT_WINDOW_ADJUSTMENT_DAYS)
        
        if latest_possible_date >= date.today():
            # We can still send it, but with adjusted date
            adjusted_date = latest_possible_date
            adjustment_note = f"Adjusted send date due to recent email on {recent_date}"
            return adjusted_date, False, adjustment_note
        else:
            # Too late to adjust, we'd need to send in the past
            skip_reason = f"Too late to adjust send date, would need to send before today"
            return ideal_send_date, True, skip_reason


async def prepare_batch_operations(
    validated_events: List[EventData],
    standard_send_time: str = STANDARD_SEND_TIME
) -> List[Tuple]:
    """
    Prepare the batch operations for the database insert/update.
    
    Args:
        validated_events: List of fully validated EventData objects
        standard_send_time: Default send time for emails
        
    Returns:
        List of tuples ready for batch database operations:
        (contact_id, base_email_type, event_year, event_month, event_day, 
         scheduled_send_date, standard_send_time, status, reason, catchup_note)
    """
    db_operations = []
    
    for event in validated_events:
        # Create catchup note if this is a catch-up email
        catchup_note = event.catchup_note
        if event.ideal_send_date < event.target_send_date and not catchup_note:
            catchup_note = f"Originally scheduled for {event.ideal_send_date.isoformat()}, adjusted to {event.target_send_date.isoformat()}"
        
        # Create the tuple for database operations
        db_operation = (
            event.contact_id,
            event.event_type,
            event.event_year,
            event.event_month,
            event.event_day,
            event.final_send_date.isoformat(),  # Use ISO format for dates
            standard_send_time,
            event.status,
            event.skip_reason,
            catchup_note
        )
        
        db_operations.append(db_operation)
    
    return db_operations


async def execute_batch_database_operations(
    db_conn,
    db_operations: List[Tuple],
    dry_run: bool = False
) -> int:
    """
    Execute the batch database operations for the email_schedules table with optimized chunking.
    Uses the ON CONFLICT DO UPDATE clause to avoid disturbing entries already 
    in 'sent', 'delivered', 'processing', or 'accepted' status.
    
    Args:
        db_conn: Database connection (changed from cursor for connection retry capability)
        db_operations: List of tuples from prepare_batch_operations
        dry_run: If True, simulate the process without making database changes
        
    Returns:
        Number of rows affected
    """
    if not db_operations:
        logger.info("No database operations to execute")
        return 0
    
    if dry_run:
        logger.info(f"DRY RUN: Would execute {len(db_operations)} database operations")
        # For dry run, log what would be done for a few examples
        for i, operation in enumerate(db_operations):
            if i < 5: # Log first 5
                logger.info(f"  DRY RUN Op {i+1}: {operation}")
            elif i == 5:
                logger.info(f"  DRY RUN ... and {len(db_operations) - 5} more operations")
                break
        return len(db_operations)
    
    total_operations_executed = 0
    
    # Process in larger batches for better performance (increased from 100)
    # Turso can handle much larger batches efficiently
    max_batch_size = 2000
    logger.info(f"Processing {len(db_operations)} operations in batches of {max_batch_size}")
    
    for batch_num, i in enumerate(range(0, len(db_operations), max_batch_size)):
        batch_slice = db_operations[i:i + max_batch_size]
        logger.info(f"Executing batch {batch_num + 1}/{(len(db_operations) + max_batch_size - 1) // max_batch_size} with {len(batch_slice)} operations")
        
        try:
            # Verify connection is still active before executing batch
            cursor = db_conn.cursor()
            try:
                cursor.execute("SELECT 1")
                cursor.fetchone()
                logger.debug(f"Database connection verified for batch {batch_num + 1}")
            except Exception as conn_err:
                logger.error(f"Connection lost before batch {batch_num + 1} execution: {conn_err}")
                raise conn_err
            
            script_parts = ["BEGIN TRANSACTION;"]
            
            for operation in batch_slice:
                contact_id, base_email_type, event_year, event_month, event_day, \
                scheduled_send_date_iso, scheduled_send_time, status, reason, catchup_note = operation

                # Safely escape string values for SQL
                safe_base_email_type = base_email_type.replace("'", "''")
                safe_scheduled_send_time = scheduled_send_time.replace("'", "''")
                safe_status = status.replace("'", "''")
                if reason is not None:
                    escaped_reason = reason.replace("'", "''")
                    safe_reason = f"'{escaped_reason}'"
                else:
                    safe_reason = "NULL"
                if catchup_note is not None:
                    escaped_catchup_note = catchup_note.replace("'", "''")
                    safe_catchup_note = f"'{escaped_catchup_note}'"
                else:
                    safe_catchup_note = "NULL"

                sql = f"""
        INSERT INTO email_schedules (
            contact_id, email_type, event_year, event_month, event_day,
            scheduled_send_date, scheduled_send_time, status, skip_reason, catchup_note,
            created_at, updated_at
        )
        VALUES (
            {contact_id},             -- INTEGER
            '{safe_base_email_type}', -- TEXT
            {event_year},             -- INTEGER
            {event_month},            -- INTEGER
            {event_day},              -- INTEGER
            '{scheduled_send_date_iso}', -- TEXT (ISO date)
            '{safe_scheduled_send_time}', -- TEXT (HH:MM:SS)
            '{safe_status}',          -- TEXT ('pre-scheduled', 'skipped', etc.)
            {safe_reason},            -- TEXT (skip reason or NULL)
            {safe_catchup_note},      -- TEXT (catchup note or NULL)
            CURRENT_TIMESTAMP,        -- created_at (only on new insert)
            CURRENT_TIMESTAMP         -- updated_at
        )
        ON CONFLICT(contact_id, email_type, event_year) DO UPDATE SET
        scheduled_send_date = excluded.scheduled_send_date,
        scheduled_send_time = excluded.scheduled_send_time,
            status              = excluded.status,
            skip_reason         = excluded.skip_reason,
            catchup_note        = excluded.catchup_note,
            event_month         = excluded.event_month,
            event_day           = excluded.event_day,
            updated_at          = CURRENT_TIMESTAMP
        WHERE email_schedules.status NOT IN ('sent', 'delivered', 'processing', 'accepted', 'scheduled');
        """
                script_parts.append(sql)
            
            script_parts.append("COMMIT;")
            batch_script = "\n".join(script_parts)
            
            # Execute this batch
            cursor.executescript(batch_script)
            total_operations_executed += len(batch_slice)
            logger.info(f"Successfully executed batch {batch_num + 1} with {len(batch_slice)} operations")
            
        except Exception as e:
            logger.error(f"Error executing batch {batch_num + 1}: {e}")
            logger.error(f"Failed batch had {len(batch_slice)} operations")
            
            # Try to rollback if possible
            try:
                db_conn.rollback()
                logger.info(f"Successfully rolled back failed batch {batch_num + 1}")
            except Exception as rollback_err:
                logger.error(f"Could not rollback batch {batch_num + 1}: {rollback_err}")
            
            # For Turso connection issues, we could potentially retry once
            if "database disk image is malformed" in str(e) or "cannot commit" in str(e):
                logger.error(f"Turso connection issue detected in batch {batch_num + 1}, stopping batch processing")
                break
            else:
                # For other errors, continue with next batch
                logger.warning(f"Non-connection error in batch {batch_num + 1}, continuing with next batch")
                continue
    
    logger.info(f"Batch processing complete. Successfully executed {total_operations_executed} out of {len(db_operations)} total operations")
    return total_operations_executed


def _write_scheduler_log(
    org_id: int,
    run_date: date,
    parameters: Dict[str, Any],
    contacts: List[Dict[str, Any]],
    candidate_events: List[EventData],
    final_schedules: List[EventData],
    results: Dict[str, Any]
) -> None:
    """
    Write detailed scheduler logs to a JSON file.
    
    Args:
        org_id: Organization ID
        run_date: Date the scheduler was run
        parameters: Dictionary of scheduler parameters
        contacts: List of pre-filtered contacts
        candidate_events: List of candidate events before scheduling
        final_schedules: List of final scheduled events
        results: Dictionary of scheduler results
    """
    # Create logs directory if it doesn't exist
    log_dir = Path("scheduler_logs")
    log_dir.mkdir(exist_ok=True)
    
    # Create filename with org_id and date
    filename = f"scheduler_log_{org_id}_{run_date.isoformat()}T{datetime.now().strftime('%H:%M:%S')}.json"
    log_path = log_dir / filename
    
    # Prepare the log data
    log_data = {
        "run_timestamp": datetime.now().isoformat(),
        "org_id": org_id,
        "run_date": run_date.isoformat(),
        "parameters": parameters,
        "results_summary": results,
        "contacts": contacts,
        "candidate_events": [event.to_dict() for event in candidate_events],
        "final_schedules": [event.to_dict() for event in final_schedules]
    }
    
    # Write to file
    with open(log_path, 'w') as f:
        json.dump(log_data, f, indent=2)
    
    logger.info(f"Detailed scheduler log written to {log_path}")


async def run_active_window_scheduling(
    db_conn,
    org_id: int,
    test_date_override: Optional[date] = None,
    lookahead_days: int = LOOKAHEAD_DAYS,
    lookback_days: int = LOOKBACK_DAYS,
    standard_send_time: str = STANDARD_SEND_TIME,
    dry_run: bool = False,
    quiet: bool = False
) -> Dict[str, Any]:
    """
    Main function to run the active window scheduling process.
    
    Args:
        db_conn: Database connection
        test_date_override: Optional date to use for testing
        lookahead_days: Number of days to look ahead for future events
        lookback_days: Number of days to look back for past events that need catch-up
        standard_send_time: Default send time for emails
        dry_run: If True, simulate the process without making database changes
        quiet: If True, minimize logging and use progress bars instead
        
    Returns:
        Dict with summary of the scheduling process
    """
    today = test_date_override or date.today()
    active_window_end = today + timedelta(days=lookahead_days)
    
    if not quiet:
        logger.info(f"Starting Active Window Scheduling (today: {today}, window end: {active_window_end}, lookback start: {today - timedelta(days=lookback_days)})")
    
    # Store parameters for logging
    parameters = {
        "test_date_override": test_date_override.isoformat() if test_date_override else None,
        "lookahead_days": lookahead_days,
        "lookback_days": lookback_days,
        "standard_send_time": standard_send_time,
        "dry_run": dry_run
    }
    
    results = {
        "contacts_fetched": 0,
        "candidate_events_identified": 0,
        "emails_pre_scheduled": 0,
        "emails_skipped": 0,
        "post_window_emails_scheduled": 0,
        "aep_emails_scheduled": 0,
        "catchup_emails_scheduled": 0,
        "birthday_catchup": 0,
        "effective_date_catchup": 0,
        "skipped_due_to_eligibility": 0,
        "conflicts_resolved": 0,
        "total_database_operations": 0,
        "processing_time_ms": 0,
    }
    
    start_time = datetime.now()
    
    try:
        # Step 1: Pre-filter relevant contacts
        if quiet:
            print("Fetching contacts...")
        contacts, total_contacts = await get_contacts_in_scheduling_window(db_conn, today, lookahead_days, lookback_days)
        results["contacts_fetched"] = len(contacts)

        if not quiet:
            logger.info(f"Total contacts: {total_contacts}")    
        
        if not contacts:
            if not quiet:
                logger.info("No contacts found in scheduling window")
            return results
        
        if not quiet:
            logger.info(f"First 10 contacts: {contacts[:10]}")
        
        # Step 2: Identify candidate event instances
        if quiet:
            print("Identifying candidate events...")
        candidate_events = await identify_candidate_events(contacts, today, lookahead_days)
        results["candidate_events_identified"] = len(candidate_events)
        
        if not candidate_events:
            if not quiet:
                logger.info("No candidate events identified")
            return results
        
        # Step 3: Calculate target send dates
        if quiet:
            print("Calculating target send dates...")
        events_with_target_dates = await calculate_target_send_dates(
            candidate_events, 
            today, 
            total_contacts,
            db_conn=db_conn
        )
        
        # Step 3.5: Resolve birthday/effective date conflicts
        if quiet:
            print("Resolving conflicts...")
        events_with_conflicts_resolved = await resolve_birthday_effective_date_conflicts(
            events_with_target_dates,
            test_date_override=test_date_override,
            conflict_window_days=90,
            db_conn=db_conn
        )
        
        # Count conflicts resolved
        conflicts_count = sum(1 for event in events_with_conflicts_resolved 
                            if event.status == 'skipped' and event.skip_reason and 'conflict' in event.skip_reason.lower())
        results["conflicts_resolved"] = conflicts_count
        
        if not quiet:
            logger.info(f"Resolved {conflicts_count} conflicts")
        
        # Step 4: Validate with ContactRuleEngine and apply eligibility check filter
        if quiet:
            print("Validating events...")
        if ELIGIBILITY_CHECK_ENABLED:
            if not quiet:
                logger.info(f"Eligibility check is enabled with lookup window of {RECENT_EMAIL_CHECK_DAYS} days")
            validated_events = await validate_with_contact_rule_engine(
                events_with_conflicts_resolved,
                test_date_override=test_date_override,
                db_conn=db_conn
            )
        else:
            if not quiet:
                logger.info("Eligibility check is disabled")
            validated_events = await validate_with_contact_rule_engine(
                events_with_conflicts_resolved,
                test_date_override=test_date_override
            )
        
        # Count results by status and type
        for event in validated_events:
            is_catchup = event.is_catchup(today)
            
            if event.status == 'pre-scheduled':
                if event.event_type.startswith('POST_WINDOW_'):
                    results["post_window_emails_scheduled"] += 1
                elif event.event_type.startswith('AEP_'):
                    results["aep_emails_scheduled"] += 1
                else:
                    results["emails_pre_scheduled"] += 1
                    
                    if is_catchup:
                        results["catchup_emails_scheduled"] += 1
                        if event.event_type == 'BIRTHDAY':
                            results["birthday_catchup"] += 1
                        elif event.event_type == 'EFFECTIVE_DATE':
                            results["effective_date_catchup"] += 1
                        
            elif event.status == 'skipped':
                results["emails_skipped"] += 1
                
                if event.skip_reason and ('eligibility' in event.skip_reason.lower() or 'answered' in event.skip_reason.lower()):
                    results["skipped_due_to_eligibility"] += 1
        
        # Step 5: Prepare batch operations
        if quiet:
            print("Preparing database operations...")
        db_operations = await prepare_batch_operations(validated_events, standard_send_time)
        
        # Step 6: Execute batch operations
        if quiet:
            print("Executing database operations...")
        cursor = db_conn.cursor()
        rows_affected = await execute_batch_database_operations(db_conn, db_operations, dry_run)
        
        results["total_database_operations"] = rows_affected
        
        # Write detailed log
        if not quiet:
            _write_scheduler_log(
                    org_id=org_id,
                run_date=today,
                parameters=parameters,
                contacts=contacts,
                candidate_events=candidate_events,
                final_schedules=validated_events,
                results=results
            )
        
        # Commit changes if not dry run
        if not dry_run:
            db_conn.commit()
        
    except Exception as e:
        logger.error(f"Error in active window scheduling: {e}")
        if db_conn and not dry_run:
            db_conn.rollback()
    
    end_time = datetime.now()
    processing_time = (end_time - start_time).total_seconds() * 1000
    results["processing_time_ms"] = round(processing_time)
    
    if not quiet:
        logger.info(f"Active Window Scheduling completed in {processing_time:.2f}ms. Results: {results}")
    return results


async def ensure_email_schedules_schema(cursor):
    """
    Ensure the email_schedules table has the necessary schema for 
    active window scheduling, including event_year, event_month, and event_day columns.
    
    Args:
        cursor: Database cursor
    """
    # Check if columns exist by querying table info
    required_columns = {
        "event_year": False,
        "event_month": False,
        "event_day": False,
        "catchup_note": False
    }
    
    try:
        # Get current columns in the table
        cursor.execute("PRAGMA table_info(email_schedules)")
        columns = cursor.fetchall()
        
        # Check which columns already exist
        for col in columns:
            name = col[1].lower()  # Column name is at index 1
            if name in required_columns:
                required_columns[name] = True
                
        # If all columns exist, we're done
        if all(required_columns.values()):
            logger.info("Email schedules table already has all required columns")
            
            # Still ensure indexes exist
            cursor.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_email_schedules_unique_event 
            ON email_schedules(contact_id, email_type, event_year);
            """)
            
            cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_email_schedules_event_date 
            ON email_schedules(event_year, event_month, event_day);
            """)
            
            return
            
        # Otherwise add missing columns
        logger.info("Adding missing columns to email_schedules table")
        
        # Build script to add only missing columns
        script_parts = ["BEGIN TRANSACTION;"]
        
        if not required_columns["event_year"]:
            script_parts.append("ALTER TABLE email_schedules ADD COLUMN event_year INTEGER;")
        
        if not required_columns["event_month"]:
            script_parts.append("ALTER TABLE email_schedules ADD COLUMN event_month INTEGER;")
            
        if not required_columns["event_day"]:
            script_parts.append("ALTER TABLE email_schedules ADD COLUMN event_day INTEGER;")
            
        if not required_columns["catchup_note"]:
            script_parts.append("ALTER TABLE email_schedules ADD COLUMN catchup_note TEXT;")
            
        # Always ensure indexes exist
        script_parts.append("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_email_schedules_unique_event 
        ON email_schedules(contact_id, email_type, event_year);
        """)
        
        script_parts.append("""
        CREATE INDEX IF NOT EXISTS idx_email_schedules_event_date 
        ON email_schedules(event_year, event_month, event_day);
        """)
        
        script_parts.append("COMMIT;")
        
        # Execute script
        cursor.executescript("\n".join(script_parts))
        
        logger.info("Successfully added missing columns to email_schedules table")
    except Exception as e:
        logger.error(f"Error ensuring email_schedules schema: {e}")
        raise
        
        
async def ensure_contact_events_indexes(cursor):
    """
    Ensure the contact_events table has the necessary indexes for optimal
    eligibility check performance.
    
    Args:
        cursor: Database cursor
    """
    try:
        # Check if contact_events table exists first
        try:
            cursor.execute("SELECT COUNT(*) FROM contact_events")
        except Exception as e:
            logger.warning(f"contact_events table doesn't exist: {e}")
            # If it doesn't exist, we can't add indexes
            return False
        
        # Create basic indexes one by one - Turso may have limitations with executescript
        try:
            # Index for contact_id lookups (most frequent query pattern)
            cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_contact_events_contact_id 
            ON contact_events(contact_id)
            """)
            
            # Index for event type lookups
            cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_contact_events_event_type 
            ON contact_events(event_type)
            """)
            
            # Index for timestamps (for date range queries)
            cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_contact_events_created_at 
            ON contact_events(created_at)
            """)
            
            logger.info("Successfully created basic contact_events indexes")
            return True
        except Exception as e:
            logger.warning(f"Error creating individual indexes: {e}")
            # Continue even if indexes fail - they're optional optimizations
            # Still consider this a success as indexes are just for optimization
            return True
        
    except Exception as e:
        logger.error(f"Error creating contact_events indexes: {e}")
        return False


# Function to run the scheduler from CLI or as a task
async def run_scheduler(
    org_id: int,
    test_date_override: Optional[date] = None,
    lookahead_days: int = LOOKAHEAD_DAYS,
    lookback_days: int = LOOKBACK_DAYS,
    standard_send_time: str = STANDARD_SEND_TIME,
    dry_run: bool = False,
    quiet: bool = False
) -> Dict[str, Any]:
    """
    Run the active window scheduler for a specific organization.
    
    Args:
        org_id: Organization ID
        test_date_override: Optional date to use instead of today
        lookahead_days: Days to look ahead for future events
        lookback_days: Days to look back for past events that need catch-up
        standard_send_time: Time of day to send emails (HH:MM:SS)
        dry_run: If True, simulate without making database changes
        quiet: If True, minimize logging and use progress bars instead
        
    Returns:
        Dict with summary of the scheduling process
    """
    db_conn = None
    try:
        # Get database connection
        db_conn = get_org_conn(org_id)
        if not db_conn:
            logger.error(f"Could not connect to database for org_id: {org_id}")
            return {"error": "Database connection failed"}
        
        cursor = db_conn.cursor()
        
        # Ensure the schema has necessary columns
        await ensure_email_schedules_schema(cursor)
        
        # Ensure contact_events table has the required indexes for optimal eligibility check performance
        # This is especially important for the batch eligibility check feature
        indexes_created = await ensure_contact_events_indexes(cursor)
        if indexes_created:
            logger.info("Contact events indexes are properly configured for eligibility checks")
        
        # Run the scheduler
        results = await run_active_window_scheduling(
            db_conn,
            org_id=org_id,  # Pass org_id to the scheduling function
            test_date_override=test_date_override,
            lookahead_days=lookahead_days,
            lookback_days=lookback_days,
            standard_send_time=standard_send_time,
            dry_run=dry_run,
            quiet=quiet
        )
        
        return results
    
    except Exception as e:
        logger.error(f"Error running scheduler for org_id {org_id}: {e}")
        return {"error": str(e)}
    
    finally:
        if db_conn:
            #db_conn.sync()
            db_conn.close()


if __name__ == "__main__":
    # Set up logging
    import argparse
    parser = argparse.ArgumentParser(description="Run the Active Window Scheduler")
    parser.add_argument("org_id", type=int, nargs="?", default=1, help="Organization ID")
    parser.add_argument("--dry-run", "-d", action="store_true", help="Simulate without making changes")
    parser.add_argument("--lookahead", "-l", type=int, default=LOOKAHEAD_DAYS, 
                       help=f"Days to look ahead (default: {LOOKAHEAD_DAYS})")
    parser.add_argument("--lookback", "-b", type=int, default=30, 
                       help="Days to look back for events that need catch-up (default: 30)")
    parser.add_argument("--send-time", "-t", default=STANDARD_SEND_TIME, 
                       help=f"Standard send time (HH:MM:SS) (default: {STANDARD_SEND_TIME})")
    parser.add_argument("--quiet", "-q", action="store_true", help="Run in quiet mode with minimal logging")
    parser.add_argument("--debug", action="store_true", help="Run in debug mode with verbose logging")
    
    args = parser.parse_args()
    
    # Configure logging based on verbosity
    if args.quiet:
        logging.basicConfig(
            level=logging.WARNING,
            format='%(levelname)s: %(message)s'
        )
    elif args.debug:
        logging.basicConfig(
            level=logging.DEBUG,
            format='%(asctime)s - %(levelname)s - %(name)s - %(message)s'
        )
    else:
        # Default info level with minimal format
        logging.basicConfig(
            level=logging.INFO,
            format='%(levelname)s: %(message)s'
        )
    
    # Reduce logging verbosity for specific loggers
    logging.getLogger('src.tasks.active_window_scheduler').setLevel(
        logging.WARNING if args.quiet else logging.INFO
    )
    
    # Disable debug logging for batch operations unless in debug mode
    if not args.debug:
        logging.getLogger('src.tasks.active_window_scheduler').setLevel(logging.INFO)
    
    if not args.quiet:
        logger.info(f"Running active window scheduler for org_id: {args.org_id}")
        logger.info(f"Settings: dry_run={args.dry_run}, lookahead={args.lookahead}, lookback={args.lookback}")
    
    loop = asyncio.get_event_loop()
    results = loop.run_until_complete(
        run_scheduler(
            org_id=args.org_id,
            lookahead_days=args.lookahead,
            lookback_days=args.lookback,
            standard_send_time=args.send_time,
            dry_run=args.dry_run,
            quiet=args.quiet
        )
    )
    
    # Always show final results, even in quiet mode
    print(f"\nResults: {results}")