#!/usr/bin/env python3
"""
Update Golden Dataset Database Schema

This script updates the golden_dataset.sqlite3 database to match the schema
from org-206.sqlite3, adding all missing tables and columns needed for
comprehensive testing of the scheduler.
"""

import sqlite3
import shutil
from datetime import datetime, date

def backup_golden_dataset():
    """Create a backup of the original golden dataset"""
    backup_name = f"golden_dataset_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.sqlite3"
    shutil.copy2('golden_dataset.sqlite3', backup_name)
    print(f"✓ Created backup: {backup_name}")
    return backup_name

def add_missing_columns_to_email_schedules(conn):
    """Add missing columns to email_schedules table"""
    cursor = conn.cursor()
    
    # Check existing columns
    cursor.execute("PRAGMA table_info(email_schedules)")
    existing_cols = {row[1] for row in cursor.fetchall()}
    
    # Define missing columns with their definitions
    missing_columns = {
        'actual_send_datetime': 'TEXT',
        'priority': 'INTEGER DEFAULT 10',
        'campaign_instance_id': 'INTEGER',
        'email_template': 'TEXT',
        'sms_template': 'TEXT',
        'scheduler_run_id': 'TEXT',
        'metadata': 'TEXT'
    }
    
    # Add missing columns
    for col_name, col_def in missing_columns.items():
        if col_name not in existing_cols:
            try:
                cursor.execute(f"ALTER TABLE email_schedules ADD COLUMN {col_name} {col_def}")
                print(f"✓ Added column {col_name} to email_schedules")
            except sqlite3.OperationalError as e:
                print(f"⚠ Could not add column {col_name}: {e}")

def create_campaign_tables(conn):
    """Create all campaign-related tables"""
    cursor = conn.cursor()
    
    # Campaign types table
    cursor.execute("""
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
    print("✓ Created campaign_types table")
    
    # Campaign instances table
    cursor.execute("""
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
    print("✓ Created campaign_instances table")
    
    # Contact campaigns table
    cursor.execute("""
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
    print("✓ Created contact_campaigns table")
    
    # Campaign change log table
    cursor.execute("""
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
    print("✓ Created campaign_change_log table")

def create_scheduler_tables(conn):
    """Create scheduler-related tables"""
    cursor = conn.cursor()
    
    # Scheduler checkpoints table
    cursor.execute("""
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
    print("✓ Created scheduler_checkpoints table")
    
    # Config versions table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS config_versions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            version_name TEXT NOT NULL,
            config_data TEXT NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            active BOOLEAN DEFAULT FALSE
        )
    """)
    print("✓ Created config_versions table")
    
    # Followup processing log table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS followup_processing_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_timestamp DATETIME NOT NULL,
            scheduler_run_id TEXT NOT NULL,
            contacts_processed INTEGER,
            followups_scheduled INTEGER,
            processing_duration_seconds REAL,
            status TEXT NOT NULL,
            error_message TEXT
        )
    """)
    print("✓ Created followup_processing_log table")
    
    # Email tracking table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS email_tracking (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email_schedule_id INTEGER NOT NULL,
            tracking_event TEXT NOT NULL,
            event_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            event_data TEXT,
            FOREIGN KEY (email_schedule_id) REFERENCES email_schedules(id)
        )
    """)
    print("✓ Created email_tracking table")

def create_performance_indexes(conn):
    """Create indexes for better performance"""
    cursor = conn.cursor()
    
    indexes = [
        "CREATE INDEX IF NOT EXISTS idx_contacts_state_birthday ON contacts(state, birth_date)",
        "CREATE INDEX IF NOT EXISTS idx_contacts_state_effective ON contacts(state, effective_date)",
        "CREATE INDEX IF NOT EXISTS idx_campaigns_active ON campaign_instances(active_start_date, active_end_date)",
        "CREATE INDEX IF NOT EXISTS idx_schedules_lookup ON email_schedules(contact_id, email_type, scheduled_send_date)",
        "CREATE INDEX IF NOT EXISTS idx_schedules_status_date ON email_schedules(status, scheduled_send_date)",
        "CREATE INDEX IF NOT EXISTS idx_schedules_run_id ON email_schedules(scheduler_run_id)",
        "CREATE INDEX IF NOT EXISTS idx_contact_campaigns_lookup ON contact_campaigns(contact_id, campaign_instance_id)",
        "CREATE INDEX IF NOT EXISTS idx_email_tracking_schedule ON email_tracking(email_schedule_id)"
    ]
    
    for index_sql in indexes:
        try:
            cursor.execute(index_sql)
            index_name = index_sql.split("idx_")[1].split(" ")[0]
            print(f"✓ Created index: idx_{index_name}")
        except sqlite3.OperationalError as e:
            print(f"⚠ Could not create index: {e}")

def populate_test_campaign_data(conn):
    """Add some basic campaign data for testing"""
    cursor = conn.cursor()
    
    # Add sample campaign types
    campaign_types = [
        ('rate_increase', True, True, 14, False, 1, True),
        ('seasonal_promo', True, True, 7, False, 5, True),
        ('regulatory_notice', False, False, 0, True, 2, True),  # Ignores exclusion windows
        ('initial_blast', False, True, 0, True, 10, True)
    ]
    
    cursor.executemany("""
        INSERT OR IGNORE INTO campaign_types 
        (name, respect_exclusion_windows, enable_followups, days_before_event, 
         target_all_contacts, priority, active)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, campaign_types)
    
    print("✓ Added sample campaign types")
    
    # Add sample campaign instances
    campaign_instances = [
        ('rate_increase', 'q1_2024_rate_increase', 'rate_increase_email_v1', 'rate_increase_sms_v1', 
         '2024-01-01', '2024-03-31', '{}'),
        ('seasonal_promo', 'spring_2024_enrollment', 'spring_promo_email', 'spring_promo_sms',
         '2024-03-01', '2024-05-31', '{}'),
        ('regulatory_notice', 'privacy_policy_update_2024', 'privacy_email_template', None,
         '2024-06-01', '2024-06-30', '{}')
    ]
    
    cursor.executemany("""
        INSERT OR IGNORE INTO campaign_instances 
        (campaign_type, instance_name, email_template, sms_template, 
         active_start_date, active_end_date, metadata)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, campaign_instances)
    
    print("✓ Added sample campaign instances")

def verify_schema_update(conn):
    """Verify that all updates were applied correctly"""
    cursor = conn.cursor()
    
    print("\n=== SCHEMA VERIFICATION ===")
    
    # Check email_schedules columns
    cursor.execute("PRAGMA table_info(email_schedules)")
    email_cols = {row[1] for row in cursor.fetchall()}
    required_email_cols = {
        'id', 'contact_id', 'email_type', 'scheduled_send_date', 'scheduled_send_time',
        'status', 'skip_reason', 'created_at', 'updated_at', 'batch_id', 'event_year',
        'event_month', 'event_day', 'catchup_note', 'actual_send_datetime', 'priority',
        'campaign_instance_id', 'email_template', 'sms_template', 'scheduler_run_id', 'metadata'
    }
    
    missing_email_cols = required_email_cols - email_cols
    if missing_email_cols:
        print(f"⚠ Missing email_schedules columns: {missing_email_cols}")
    else:
        print("✓ email_schedules table has all required columns")
    
    # Check for required tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = {row[0] for row in cursor.fetchall()}
    required_tables = {
        'contacts', 'email_schedules', 'campaign_types', 'campaign_instances',
        'contact_campaigns', 'campaign_change_log', 'scheduler_checkpoints',
        'config_versions', 'followup_processing_log', 'email_tracking'
    }
    
    missing_tables = required_tables - tables
    if missing_tables:
        print(f"⚠ Missing tables: {missing_tables}")
    else:
        print("✓ All required tables exist")
    
    # Check campaign data
    cursor.execute("SELECT COUNT(*) FROM campaign_types")
    campaign_type_count = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM campaign_instances")
    campaign_instance_count = cursor.fetchone()[0]
    
    print(f"✓ Campaign types: {campaign_type_count}")
    print(f"✓ Campaign instances: {campaign_instance_count}")

def main():
    """Main function to update the golden dataset"""
    print("=== UPDATING GOLDEN DATASET ===")
    
    # Create backup
    backup_name = backup_golden_dataset()
    
    try:
        # Open database connection
        conn = sqlite3.connect('golden_dataset.sqlite3')
        conn.execute("PRAGMA foreign_keys = ON")
        
        # Perform updates
        print("\n--- Adding missing columns ---")
        add_missing_columns_to_email_schedules(conn)
        
        print("\n--- Creating campaign tables ---")
        create_campaign_tables(conn)
        
        print("\n--- Creating scheduler tables ---")
        create_scheduler_tables(conn)
        
        print("\n--- Creating performance indexes ---")
        create_performance_indexes(conn)
        
        print("\n--- Adding test campaign data ---")
        populate_test_campaign_data(conn)
        
        # Commit all changes
        conn.commit()
        
        # Verify updates
        verify_schema_update(conn)
        
        print(f"\n✅ GOLDEN DATASET UPDATED SUCCESSFULLY")
        print(f"   Backup saved as: {backup_name}")
        
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    main()