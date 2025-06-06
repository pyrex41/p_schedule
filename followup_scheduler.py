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
from scheduler import (
    SchedulingConfig, StateRulesEngine, Contact, DatabaseManager
)
from src.config_loader import get_campaign_types_allowing_followups

logger = logging.getLogger(__name__)

# Configuration
FOLLOW_UP_DELAY_DAYS = 2  # Days after initial email to schedule follow-up
LOOK_BACK_DAYS = 35  # How many days to look back for emails needing follow-ups
STANDARD_SEND_TIME = "08:45:00"  # Default send time

INITIAL_EMAIL_TYPES_FOR_FOLLOWUP = get_campaign_types_allowing_followups()

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
                    tracking_id TEXT,
                    clicked_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    url TEXT,
                    ip_address TEXT,
                    user_agent TEXT,
                    FOREIGN KEY (contact_id) REFERENCES contacts(id)
                )
            """)
            
            # Create contact_events table if it doesn't exist
            conn.execute("""
                CREATE TABLE IF NOT EXISTS contact_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    contact_id INTEGER NOT NULL,
                    event_type TEXT NOT NULL,
                    metadata TEXT,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (contact_id) REFERENCES contacts(id)
                )
            """)
            
            # Add indexes for performance
            conn.execute("CREATE INDEX IF NOT EXISTS idx_tracking_clicks_contact_tracking ON tracking_clicks(contact_id, tracking_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_contact_events_contact_type ON contact_events(contact_id, event_type)")

    async def run_active_followup_scheduler(self, lookback_days: int = LOOK_BACK_DAYS, test_mode: bool = False) -> Dict[str, Any]:
        """Main entry point for the active follow-up scheduler."""
        logger.info(f"Starting active follow-up scheduling (lookback: {lookback_days} days)")
        
        stats = {
            'emails_examined': 0,
            'new_followups_scheduled': 0,
            'followups_updated': 0,
            'failures': 0,
            'followup_types': {
                'followup_1_cold': 0,
                'followup_2_clicked_no_hq': 0,
                'followup_3_hq_no_yes': 0,
                'followup_4_hq_with_yes': 0
            }
        }

        end_date = datetime.now()
        start_date = end_date - timedelta(days=lookback_days)

        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            
            initial_emails = await self._find_emails_needing_followups(conn, start_date, end_date)
            stats['emails_examined'] = len(initial_emails)
            logger.info(f"Found {len(initial_emails)} initial emails eligible for follow-up processing.")

            if not initial_emails:
                return stats

            contact_ids = list(set(email['contact_id'] for email in initial_emails))
            
            # Batch fetch all data
            contacts_data = await self._batch_fetch_contact_data(conn, contact_ids)
            clicks_data = await self._batch_fetch_raw_click_data(conn, contact_ids)
            hq_events_data = await self._batch_fetch_raw_hq_event_data(conn, contact_ids)

            sql_statements_to_execute = []
            
            for email in initial_emails:
                contact_id = email['contact_id']
                if contact_id not in contacts_data:
                    logger.warning(f"Contact {contact_id} not found for email {email['id']}. Skipping.")
                    continue

                followup_type, details = await self._determine_contact_followup_type(
                    contact_id,
                    email.get('tracking_uuid'),
                    email['sent_time'],
                    clicks_data.get(contact_id, []),
                    hq_events_data.get(contact_id, [])
                )

                success, sql_statements = await self._schedule_or_update_followup(
                    conn, email, followup_type, details, contacts_data[contact_id]
                )

                if success:
                    sql_statements_to_execute.extend(sql_statements)
                    if sql_statements:
                        if "UPDATE" in sql_statements[0]:
                           stats['followups_updated'] += 1
                        else:
                           stats['new_followups_scheduled'] += 1
                        stats['followup_types'][followup_type.split('.')[0]] = stats['followup_types'].get(followup_type.split('.')[0], 0) + 1
                else:
                    stats['failures'] += 1
            
            if not test_mode and sql_statements_to_execute:
                logger.info(f"Executing {len(sql_statements_to_execute)} SQL statements for follow-ups.")
                cursor = conn.cursor()
                try:
                    cursor.execute("BEGIN;")
                    for stmt in sql_statements_to_execute:
                        cursor.execute(stmt)
                    conn.commit()
                except Exception as e:
                    logger.error(f"Error executing batch follow-up SQL: {e}")
                    conn.rollback()
                    stats['failures'] += len(sql_statements_to_execute)

            elif test_mode:
                 logger.info(f"TEST MODE: Would execute {len(sql_statements_to_execute)} SQL statements.")

        logger.info(f"Follow-up scheduling complete. Stats: {stats}")
        return stats

    async def _find_emails_needing_followups(self, conn: sqlite3.Connection, start_date: datetime, end_date: datetime) -> List[Dict[str, Any]]:
        """Finds initial emails that need follow-ups scheduled."""
        cursor = conn.cursor()
        email_types_placeholders = ', '.join('?' * len(INITIAL_EMAIL_TYPES_FOR_FOLLOWUP))
        
        query = f"""
            SELECT
                es.id, es.contact_id, es.email_type,
                COALESCE(es.actual_send_datetime, es.scheduled_send_date) as sent_time,
                es.metadata, es.event_year,
                json_extract(es.metadata, '$.tracking_uuid') as tracking_uuid
            FROM email_schedules es
            WHERE
                es.status IN ('sent', 'delivered')
                AND es.scheduled_send_date BETWEEN ? AND ?
                AND LOWER(es.email_type) IN ({email_types_placeholders})
                AND json_extract(COALESCE(es.metadata, '{{}}'), '$.followup_processed') IS NULL
                AND NOT EXISTS (
                    SELECT 1 FROM email_schedules es2
                    WHERE es2.contact_id = es.contact_id
                    AND es2.email_type LIKE 'followup_%'
                    AND es2.event_year = es.event_year
                )
        """
        
        cursor.execute(query, (start_date.isoformat(), end_date.isoformat(), *INITIAL_EMAIL_TYPES_FOR_FOLLOWUP))
        return [dict(row) for row in cursor.fetchall()]

    async def _batch_fetch_contact_data(self, conn: sqlite3.Connection, contact_ids: List[int]) -> Dict[int, Dict[str, Any]]:
        """Fetches contact details for a list of contact IDs."""
        if not contact_ids:
            return {}
        placeholders = ', '.join('?' * len(contact_ids))
        query = f"SELECT * FROM contacts WHERE id IN ({placeholders})"
        cursor = conn.cursor()
        cursor.execute(query, contact_ids)
        return {row['id']: dict(row) for row in cursor.fetchall()}

    async def _batch_fetch_raw_click_data(self, conn: sqlite3.Connection, contact_ids: List[int]) -> Dict[int, List[Dict[str, Any]]]:
        """Fetches all tracking_clicks for a list of contact IDs."""
        if not contact_ids:
            return {}
        clicks_by_contact = {cid: [] for cid in contact_ids}
        placeholders = ', '.join('?' * len(contact_ids))
        query = f"SELECT * FROM tracking_clicks WHERE contact_id IN ({placeholders}) ORDER BY clicked_at DESC"
        cursor = conn.cursor()
        cursor.execute(query, contact_ids)
        for row in cursor.fetchall():
            clicks_by_contact[row['contact_id']].append(dict(row))
        return clicks_by_contact

    async def _batch_fetch_raw_hq_event_data(self, conn: sqlite3.Connection, contact_ids: List[int]) -> Dict[int, List[Dict[str, Any]]]:
        """Fetches 'eligibility_answered' contact_events for a list of contact IDs."""
        if not contact_ids:
            return {}
        events_by_contact = {cid: [] for cid in contact_ids}
        placeholders = ', '.join('?' * len(contact_ids))
        query = f"SELECT * FROM contact_events WHERE contact_id IN ({placeholders}) AND event_type = 'eligibility_answered' ORDER BY created_at DESC"
        cursor = conn.cursor()
        cursor.execute(query, contact_ids)
        for row in cursor.fetchall():
            events_by_contact[row['contact_id']].append(dict(row))
        return events_by_contact

    async def _determine_contact_followup_type(
        self, contact_id: int, tracking_uuid: str, initial_email_sent_at_iso: str,
        contact_specific_clicks: List[Dict[str, Any]], contact_specific_hq_events: List[Dict[str, Any]]
    ) -> Tuple[str, Dict[str, Any]]:
        """Evaluate contact's current behavior and determine the appropriate follow-up type."""
        clicked, latest_click_iso = self._get_click_status_from_prefetched(
            contact_specific_clicks, tracking_uuid, initial_email_sent_at_iso
        )
        
        hq_check_after_ts_iso = latest_click_iso or initial_email_sent_at_iso
        
        answered_hq, has_medical_conditions = self._get_hq_status_from_prefetched(
            contact_id, contact_specific_hq_events, hq_check_after_ts_iso
        )
        
        details = {
            "clicked": clicked, "latest_click_time": latest_click_iso,
            "answered_hq": answered_hq, "has_medical_conditions": has_medical_conditions
        }
        
        if answered_hq:
            return 'followup_4_hq_with_yes' if has_medical_conditions else 'followup_3_hq_no_yes', details
        elif clicked:
            return 'followup_2_clicked_no_hq', details
        else:
            return 'followup_1_cold', details

    def _get_click_status_from_prefetched(
        self, clicks: List[Dict[str, Any]], tracking_uuid: str, after_ts: str
    ) -> Tuple[bool, Optional[str]]:
        if not tracking_uuid: return False, None
        for click in clicks:
            if click['tracking_id'] == tracking_uuid and click['clicked_at'] > after_ts:
                return True, click['clicked_at']
        return False, None

    def _get_hq_status_from_prefetched(
        self, contact_id: int, hq_events: List[Dict[str, Any]], after_ts: str
    ) -> Tuple[bool, Optional[bool]]:
        for event in hq_events:
            if event['created_at'] > after_ts:
                try:
                    metadata = json.loads(event['metadata'])
                    if 'has_medical_conditions' in metadata:
                        return True, bool(metadata['has_medical_conditions'])
                    yes_count = metadata.get('main_questions_yes_count', 0)
                    return True, yes_count > 0
                except (json.JSONDecodeError, TypeError):
                    logger.warning(f"Could not parse metadata for contact_event {event['id']}")
                    return True, None
        return False, None

    async def _schedule_or_update_followup(
        self, conn: sqlite3.Connection, email_data: Dict[str, Any], followup_type: str,
        details: Dict[str, Any], contact_data: Dict[str, Any]
    ) -> Tuple[bool, List[str]]:
        """Create or update a follow-up schedule. Returns SQL statements."""
        sql_statements = []
        contact_id = email_data['contact_id']
        event_year = email_data.get('event_year')

        existing_id, existing_type = await self._get_existing_followup_schedule(conn, contact_id, event_year)

        initial_sent_dt = datetime.fromisoformat(email_data['sent_time'].replace('Z', '+00:00'))
        ideal_followup_date = (initial_sent_dt + timedelta(days=FOLLOW_UP_DELAY_DAYS)).date()
        scheduled_send_date = max(ideal_followup_date, date.today()).isoformat()

        metadata = {
            'initial_email_id': email_data['id'],
            'initial_email_type': email_data['email_type'],
            'followup_behavior': details,
            'last_evaluated_at': datetime.now().isoformat()
        }

        def format_sql_literal(value):
            if value is None: return "NULL"
            if isinstance(value, (int, float)): return str(value)
            return f"'{str(value).replace("'", "''")}'"

        if existing_id:
            if existing_type != followup_type:
                sql = f"UPDATE email_schedules SET email_type = {format_sql_literal(followup_type)}, metadata = json_patch(COALESCE(metadata, '{{}}'), {format_sql_literal(json.dumps(metadata))}) WHERE id = {existing_id};"
                sql_statements.append(sql)
        else:
            sql = f"""
                INSERT INTO email_schedules (contact_id, email_type, scheduled_send_date, scheduled_send_time, status, metadata, event_year)
                VALUES ({contact_id}, {format_sql_literal(followup_type)}, {format_sql_literal(scheduled_send_date)}, 
                        {format_sql_literal(STANDARD_SEND_TIME)}, 'pre-scheduled', {format_sql_literal(json.dumps(metadata))}, {format_sql_literal(event_year)});
            """
            sql_statements.append(sql)

        if sql_statements:
            sql_statements.append(f"UPDATE email_schedules SET metadata = json_set(COALESCE(metadata, '{{}}'), '$.followup_processed', 'true') WHERE id = {email_data['id']};")

        return True, sql_statements

    async def _get_existing_followup_schedule(self, conn: sqlite3.Connection, contact_id: int, event_year: Optional[int]) -> Tuple[Optional[int], Optional[str]]:
        """Check for an existing, unsent follow-up."""
        cursor = conn.cursor()
        query = "SELECT id, email_type FROM email_schedules WHERE contact_id = ? AND email_type LIKE 'followup_%' AND status IN ('pre-scheduled', 'scheduled')"
        params = [contact_id]
        if event_year:
            query += " AND event_year = ?"
            params.append(event_year)
        
        cursor.execute(query, params)
        result = cursor.fetchone()
        return (result['id'], result['email_type']) if result else (None, None)

def main():
    """Main entry point for command-line execution."""
    import argparse
    import asyncio
    
    parser = argparse.ArgumentParser(description="Run the Active Follow-up Scheduler.")
    parser.add_argument('--db', required=True, help='SQLite database path')
    parser.add_argument('--lookback', type=int, default=LOOK_BACK_DAYS, help='Days to look back for initial emails.')
    parser.add_argument('--test', action='store_true', help='Run in test mode without committing changes.')
    args = parser.parse_args()

    # A mock SchedulingConfig and StateRulesEngine are needed to instantiate the class.
    # In a real app, these would be properly configured.
    config = SchedulingConfig(send_time="09:00:00", load_balancing_window=1, daily_send_limit=10000)
    state_rules = StateRulesEngine(db_path=args.db)

    scheduler = FollowupEmailScheduler(config, state_rules, args.db)
    
    loop = asyncio.get_event_loop()
    results = loop.run_until_complete(
        scheduler.run_active_followup_scheduler(lookback_days=args.lookback, test_mode=args.test)
    )
    
    print(f"Scheduler run finished. Results: {results}")

if __name__ == '__main__':
    main()