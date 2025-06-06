#!/usr/bin/env python3
"""
Campaign-Based Email Scheduler

This module handles scheduling of campaign-based emails using the flexible
campaign instance system described in the business logic.
"""

import sqlite3
import json
import logging
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from scheduler import (
    EmailType, EmailStatus, CampaignType, CampaignInstance, ContactCampaign,
    Contact, StateRulesEngine, SchedulingConfig, TimingRule
)

logger = logging.getLogger(__name__)

# ============================================================================
# CAMPAIGN EMAIL SCHEDULER
# ============================================================================

class CampaignEmailScheduler:
    """Handles scheduling of campaign-based emails using DSL rules"""
    
    def __init__(self, config: SchedulingConfig, state_rules: StateRulesEngine, db_path: str):
        self.config = config
        self.state_rules = state_rules
        self.db_path = db_path
        self._campaign_types_cache = {}
        self._load_campaign_types()
    
    def _load_campaign_types(self):
        """Load campaign types from database into cache"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("""
                SELECT name, respect_exclusion_windows, enable_followups,
                       days_before_event, target_all_contacts, priority, active
                FROM campaign_types
                WHERE active = TRUE
            """)
            
            for row in cursor.fetchall():
                row_dict = dict(row)
                campaign_type = CampaignType(
                    name=row_dict['name'],
                    respect_exclusion_windows=bool(row_dict['respect_exclusion_windows']),
                    enable_followups=bool(row_dict['enable_followups']),
                    timing_rule=TimingRule(days_before_event=row_dict['days_before_event']),
                    target_all_contacts=bool(row_dict['target_all_contacts']),
                    priority=row_dict['priority'],
                    active=bool(row_dict['active'])
                )
                self._campaign_types_cache[campaign_type.name] = campaign_type
    
    def schedule_all_campaign_emails(self, scheduler_run_id: str) -> List[Dict[str, Any]]:
        """Schedule all campaign-based emails for a scheduler run."""
        schedules = []
        current_date = date.today()
        
        # Get active campaign instances
        active_instances = self._get_active_campaign_instances(current_date)
        if not active_instances:
            return schedules
        
        # Get all targeted contact campaigns for active instances
        instance_ids = [instance.id for instance in active_instances if instance.id]
        targeted_campaigns = self._get_targeted_contact_campaigns(instance_ids)
        
        # Create a lookup for campaign instances by ID
        instances_by_id = {inst.id: inst for inst in active_instances}
        
        # Schedule emails for each targeted contact campaign
        for campaign_data in targeted_campaigns:
            campaign_instance = instances_by_id.get(campaign_data['campaign_instance_id'])
            if not campaign_instance:
                continue
            
            campaign_type = self._campaign_types_cache.get(campaign_instance.campaign_type)
            if not campaign_type:
                logger.warning(f"Campaign type {campaign_instance.campaign_type} not found")
                continue

            # Create Contact and ContactCampaign objects from the joined data
            contact = Contact(
                id=campaign_data['contact_id'],
                email=campaign_data['email'],
                state=campaign_data['state'],
                zip_code=campaign_data['zip_code'],
                birthday=datetime.strptime(campaign_data['birth_date'], '%Y-%m-%d').date() if campaign_data['birth_date'] else None,
                effective_date=datetime.strptime(campaign_data['effective_date'], '%Y-%m-%d').date() if campaign_data['effective_date'] else None,
                phone_number=campaign_data['phone_number']
            )
            
            contact_campaign = ContactCampaign(
                contact_id=campaign_data['contact_id'],
                campaign_instance_id=campaign_data['campaign_instance_id'],
                trigger_date=datetime.strptime(campaign_data['trigger_date'], '%Y-%m-%d').date(),
                status=campaign_data['status'],
                metadata=json.loads(campaign_data['cc_metadata']) if campaign_data['cc_metadata'] else {}
            )

            schedule = self._schedule_campaign_email(
                contact, contact_campaign, campaign_instance, campaign_type, scheduler_run_id
            )
            if schedule:
                schedules.append(schedule)
        
        logger.info(f"Scheduled {len(schedules)} campaign emails.")
        return schedules
    
    def _get_active_campaign_instances(self, current_date: date) -> List[CampaignInstance]:
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
    
    def _get_targeted_contact_campaigns(self, instance_ids: List[int]) -> List[Dict[str, Any]]:
        """
        Get all contact campaign targeting data for a set of campaign instances,
        joined with contact information.
        """
        if not instance_ids:
            return []
        
        placeholders = ','.join('?' * len(instance_ids))
        query = f"""
            SELECT 
                cc.contact_id, 
                cc.campaign_instance_id, 
                cc.trigger_date, 
                cc.status, 
                cc.metadata as cc_metadata,
                c.email,
                c.state,
                c.zip_code,
                c.birth_date,
                c.effective_date,
                c.phone_number
            FROM contact_campaigns cc
            JOIN contacts c ON c.id = cc.contact_id
            WHERE cc.campaign_instance_id IN ({placeholders})
              AND cc.status = 'pending'
        """
        
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(query, instance_ids)
            
            return [dict(row) for row in cursor.fetchall()]
    
    def _schedule_campaign_email(self, contact: Contact, contact_campaign: ContactCampaign,
                               campaign_instance: CampaignInstance, campaign_type: CampaignType,
                               scheduler_run_id: str) -> Optional[Dict[str, Any]]:
        """Schedule a specific campaign email"""
        
        # Calculate send date based on trigger date and campaign timing
        send_date = campaign_type.timing_rule.calculate_send_date(contact_campaign.trigger_date)
        
        # If send date is in the past, skip or reschedule
        if send_date < date.today():
            # For past due campaigns, schedule for tomorrow (catch-up logic)
            send_date = date.today() + timedelta(days=1)
        
        # Check exclusion rules if campaign respects them
        is_excluded = False
        skip_reason = None
        
        if campaign_type.respect_exclusion_windows:
            is_excluded, skip_reason = self.state_rules.is_date_excluded(
                contact.state, send_date, contact.birthday, contact.effective_date
            )
        
        # Generate email type name
        email_type = f"campaign_{campaign_type.name}"
        
        return {
            'contact_id': contact.id,
            'email_type': email_type,
            'scheduled_send_date': send_date.isoformat(),
            'scheduled_send_time': self.config.send_time,
            'status': EmailStatus.SKIPPED.value if is_excluded else EmailStatus.PRE_SCHEDULED.value,
            'skip_reason': skip_reason,
            'priority': campaign_type.priority,
            'campaign_instance_id': campaign_instance.id,
            'email_template': campaign_instance.email_template,
            'sms_template': campaign_instance.sms_template,
            'scheduler_run_id': scheduler_run_id,
            'event_year': contact_campaign.trigger_date.year,
            'event_month': contact_campaign.trigger_date.month,
            'event_day': contact_campaign.trigger_date.day
        }

# ============================================================================
# CAMPAIGN MANAGEMENT UTILITIES
# ============================================================================

class CampaignManager:
    """Utilities for managing campaigns and contact targeting"""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
    
    def create_campaign_type(self, campaign_type: CampaignType) -> bool:
        """Create a new campaign type"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    INSERT OR REPLACE INTO campaign_types
                    (name, respect_exclusion_windows, enable_followups, days_before_event,
                     target_all_contacts, priority, active)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    campaign_type.name,
                    campaign_type.respect_exclusion_windows,
                    campaign_type.enable_followups,
                    campaign_type.timing_rule.days_before_event,
                    campaign_type.target_all_contacts,
                    campaign_type.priority,
                    campaign_type.active
                ))
                logger.info(f"Created campaign type: {campaign_type.name}")
                return True
        except Exception as e:
            logger.error(f"Error creating campaign type {campaign_type.name}: {e}")
            return False
    
    def create_campaign_instance(self, campaign_instance: CampaignInstance) -> Optional[int]:
        """Create a new campaign instance"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute("""
                    INSERT INTO campaign_instances
                    (campaign_type, instance_name, email_template, sms_template,
                     active_start_date, active_end_date, metadata)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    campaign_instance.campaign_type,
                    campaign_instance.instance_name,
                    campaign_instance.email_template,
                    campaign_instance.sms_template,
                    campaign_instance.active_start_date.isoformat() if campaign_instance.active_start_date else None,
                    campaign_instance.active_end_date.isoformat() if campaign_instance.active_end_date else None,
                    json.dumps(campaign_instance.metadata) if campaign_instance.metadata else None
                ))
                campaign_id = cursor.lastrowid
                logger.info(f"Created campaign instance: {campaign_instance.instance_name} (ID: {campaign_id})")
                return campaign_id
        except Exception as e:
            logger.error(f"Error creating campaign instance {campaign_instance.instance_name}: {e}")
            return None
    
    def add_contacts_to_campaign(self, campaign_instance_id: int, contact_campaigns: List[ContactCampaign]) -> int:
        """Add contacts to a campaign instance"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                data = []
                for cc in contact_campaigns:
                    data.append((
                        cc.contact_id,
                        campaign_instance_id,
                        cc.trigger_date.isoformat(),
                        cc.status,
                        json.dumps(cc.metadata) if cc.metadata else None
                    ))
                
                conn.executemany("""
                    INSERT OR IGNORE INTO contact_campaigns
                    (contact_id, campaign_instance_id, trigger_date, status, metadata)
                    VALUES (?, ?, ?, ?, ?)
                """, data)
                
                added_count = len(data)
                logger.info(f"Added {added_count} contacts to campaign instance {campaign_instance_id}")
                return added_count
        except Exception as e:
            logger.error(f"Error adding contacts to campaign {campaign_instance_id}: {e}")
            return 0
    
    def setup_sample_campaigns(self):
        """Set up sample campaigns for demonstration"""
        logger.info("Setting up sample campaigns...")
        
        # Create campaign types
        rate_increase = CampaignType(
            name="rate_increase",
            respect_exclusion_windows=True,
            enable_followups=True,
            timing_rule=TimingRule(days_before_event=14),
            priority=1
        )
        
        seasonal_promo = CampaignType(
            name="seasonal_promo",
            respect_exclusion_windows=True,
            enable_followups=True,
            timing_rule=TimingRule(days_before_event=7),
            priority=5
        )
        
        initial_blast = CampaignType(
            name="initial_blast",
            respect_exclusion_windows=False,
            enable_followups=False,
            timing_rule=TimingRule(days_before_event=0),
            target_all_contacts=True,
            priority=10
        )
        
        # Create campaign types
        for campaign_type in [rate_increase, seasonal_promo, initial_blast]:
            self.create_campaign_type(campaign_type)
        
        # Create sample campaign instances
        q1_rate_increase = CampaignInstance(
            id=None,
            campaign_type="rate_increase",
            instance_name="rate_increase_q1_2025",
            email_template="rate_increase_standard_v2",
            sms_template="rate_increase_sms_v1",
            active_start_date=date(2025, 1, 1),
            active_end_date=date(2025, 3, 31)
        )
        
        spring_promo = CampaignInstance(
            id=None,
            campaign_type="seasonal_promo",
            instance_name="spring_enrollment_2025",
            email_template="spring_promo_email_v1",
            sms_template="spring_promo_sms_v1",
            active_start_date=date(2025, 3, 1),
            active_end_date=date(2025, 5, 31)
        )
        
        # Create campaign instances
        for instance in [q1_rate_increase, spring_promo]:
            instance_id = self.create_campaign_instance(instance)
            if instance_id:
                # Add some sample contacts to campaigns
                self._add_sample_contacts_to_campaign(instance_id)
    
    def _add_sample_contacts_to_campaign(self, campaign_instance_id: int):
        """Add sample contacts to a campaign for demonstration"""
        # Get first 10 valid contacts
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("""
                SELECT id FROM contacts 
                WHERE state IS NOT NULL AND zip_code IS NOT NULL
                LIMIT 10
            """)
            
            contact_campaigns = []
            for row in cursor.fetchall():
                # Set trigger date to be next month for demonstration
                trigger_date = date.today() + timedelta(days=30)
                contact_campaigns.append(ContactCampaign(
                    contact_id=row['id'],
                    campaign_instance_id=campaign_instance_id,
                    trigger_date=trigger_date,
                    status='pending'
                ))
            
            if contact_campaigns:
                self.add_contacts_to_campaign(campaign_instance_id, contact_campaigns)

# ============================================================================
# COMMAND LINE INTERFACE FOR CAMPAIGN MANAGEMENT
# ============================================================================

def main():
    """Main entry point for campaign management"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Campaign Management System')
    parser.add_argument('--db', required=True, help='SQLite database path')
    parser.add_argument('--setup-samples', action='store_true', help='Set up sample campaigns')
    
    args = parser.parse_args()
    
    campaign_manager = CampaignManager(args.db)
    
    if args.setup_samples:
        campaign_manager.setup_sample_campaigns()
        print("Sample campaigns created successfully!")
    else:
        print("Please specify an action (e.g., --setup-samples)")

if __name__ == '__main__':
    main()