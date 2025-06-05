# Email Scheduling Business Logic Documentation

This document provides a comprehensive overview of the email scheduling business logic implemented in the FastAPI application. It is designed to facilitate refactoring in a new language while preserving all business rules and functionality.

## Overview

The email scheduling system manages automated email and SMS campaigns for multiple organizations. It uses a sophisticated rule engine to determine when to send different types of communications based on contact information, state-specific regulations, and timing constraints. The system operates in Central Time (CT) and processes databases with up to 3 million contacts.

## Core Components

### 0. System Configuration

#### Time Zone and Processing
- **System Time Zone**: All operations run in Central Time (CT)
- **Processing Model**: Single instance processing (no concurrent schedulers)
- **Database Strategy**: Work with SQLite replica, sync results back to main database
- **Reprocessing**: Clear all pre-scheduled and skipped emails before each run

#### Key Constants (Configurable)
- **send_time**: Time of day to send emails (default: 08:30 CT)
- **batch_size**: Number of contacts to process in a batch (default: 10,000)
- **max_emails_per_period**: Maximum emails per contact per period (configurable)
- **period_days**: Number of days to consider for email frequency limits (configurable)
- **birthday_email_days_before**: Days before birthday to send email (default: 14)
- **effective_date_days_before**: Days before effective date to send email (default: 30)
- **pre_window_exclusion_days**: Extension for exclusion windows (default: 60)

### 1. Email Types

The system handles two categories of emails:

#### 1.1 Anniversary-Based Email Types
These are recurring emails tied to annual dates:
NOTE: these constants should be configurable, likely in a separate config file
- **Birthday**: Sent 14 days before a contact's birthday
- **Effective Date**: Sent 30 days before a contact's policy effective date anniversary
- **AEP (Annual Enrollment Period)**: Sent in September annually
- **Post Window**: Sent after an exclusion window ends (when other emails were skipped)

#### 1.2 Campaign-Based Email Types
These are flexible, configurable campaigns that can be triggered through various mechanisms:
- **Rate Increase**: Advance notification of premium changes
- **Initial Blast**: System introduction emails sent to all contacts
- **Custom Campaigns**: Configurable campaigns for promotions, policy updates, regulatory notices, etc.

Campaign-based emails offer per-campaign configuration of:
- Exclusion window compliance (can be enabled/disabled per campaign)
- Follow-up eligibility (can be enabled/disabled per campaign)
- Timing relative to trigger date (configurable days before/after)
- Target audience (all contacts or specific subset)

### 2. Contact Information Model

Each contact requires:
- **id**: Unique identifier
- **email**: Valid email address (required)
- **zip_code**: US ZIP code (required to get the state)
- **state**: US state (required)
- **birthday**: Date of birth (optional but needed for birthday emails)
- **effective_date**: Policy effective date (optional but needed for effective date emails)

**Invalid Data Handling**:
- Contacts with invalid/missing ZIP codes are skipped during processing
- State must be determinable from ZIP code for processing to occur

Campaign-specific data (such as rate increase dates) is stored separately in the campaign system rather than as contact fields, providing greater flexibility for managing multiple campaigns per contact.

### 3. Campaign System Architecture

The campaign system provides a flexible framework for managing various types of email communications beyond the standard anniversary-based emails. The system uses a two-tier architecture: **Campaign Types** (reusable configurations) and **Campaign Instances** (specific executions with templates and targeting).

#### 3.1 Campaign Type Model (Base Configuration)

Campaign types define reusable behavior patterns:
- **name**: Campaign type identifier (e.g., 'rate_increase', 'seasonal_promo', 'initial_blast')
- **respect_exclusion_windows**: Boolean flag controlling whether state exclusion rules apply
- **enable_followups**: Boolean flag controlling whether follow-up emails are generated
- **days_before_event**: Integer defining timing relative to trigger date (0 = immediate, 14 = two weeks before)
- **target_all_contacts**: Boolean flag for campaigns targeting entire contact base
- **priority**: Integer defining campaign precedence when multiple campaigns conflict

#### 3.2 Campaign Instance Model (Specific Executions)

Campaign instances represent specific executions of campaign types with unique templates and timing:
- **campaign_type**: Reference to the base campaign type
- **instance_name**: Unique identifier for this specific campaign (e.g., 'spring_2024_promo', 'rate_increase_q1_2024')
- **email_template**: Template identifier/name for email content
- **sms_template**: Template identifier/name for SMS content (optional)
- **active_start_date**: When this campaign instance becomes active for scheduling
- **active_end_date**: When this campaign instance stops being active
- **metadata**: JSON field for instance-specific configuration overrides

#### 3.3 Campaign Change Management

The system tracks all campaign changes for audit and rescheduling purposes:

```sql
CREATE TABLE campaign_change_log (
    id INTEGER PRIMARY KEY,
    campaign_instance_id INTEGER NOT NULL,
    field_changed TEXT NOT NULL,
    old_value TEXT,
    new_value TEXT,
    changed_at DATETIME NOT NULL,
    changed_by TEXT,
    requires_rescheduling BOOLEAN DEFAULT TRUE,
    FOREIGN KEY (campaign_instance_id) REFERENCES campaign_instances(id)
);
```

When campaign dates change:
1. Log the change in campaign_change_log
2. Mark affected email schedules for reprocessing
3. Trigger scheduler to run for affected contacts

#### 3.4 Contact Campaign Targeting Model

Campaign targeting links contacts to specific campaign instances:
- **contact_id**: Reference to the target contact
- **campaign_instance_id**: Reference to the specific campaign instance
- **trigger_date**: The event date that triggers the campaign (e.g., rate change date)
- **status**: Current state ('pending', 'scheduled', 'sent', 'skipped')
- **metadata**: JSON field for contact-specific campaign data

#### 3.5 Campaign Examples with Multiple Instances

**Rate Increase Campaign Type:**
```yaml
campaign_type: rate_increase
respect_exclusion_windows: true
enable_followups: true
days_before_event: 14
target_all_contacts: false
priority: 1
```

**Multiple Rate Increase Instances:**
```yaml
# Q1 2024 Rate Increases
instance_name: rate_increase_q1_2024
email_template: rate_increase_standard_v2
sms_template: rate_increase_sms_v1
active_start_date: 2024-01-01
active_end_date: 2024-03-31

# Q2 2024 Rate Increases (different template)
instance_name: rate_increase_q2_2024
email_template: rate_increase_enhanced_v3
sms_template: rate_increase_sms_v2
active_start_date: 2024-04-01
active_end_date: 2024-06-30
```

**Seasonal Promotion Campaign Type:**
```yaml
campaign_type: seasonal_promo
respect_exclusion_windows: true
enable_followups: true
days_before_event: 7
target_all_contacts: false
priority: 5
```

**Multiple Seasonal Instances:**
```yaml
# Spring 2024 Enrollment
instance_name: spring_enrollment_2024
email_template: spring_promo_email
sms_template: spring_promo_sms
active_start_date: 2024-03-01
active_end_date: 2024-05-31

# Fall 2024 Enrollment
instance_name: fall_enrollment_2024
email_template: fall_promo_email
sms_template: fall_promo_sms
active_start_date: 2024-09-01
active_end_date: 2024-11-30
```

#### 3.6 Campaign Triggering Mechanisms

**Manual Targeting:**
- Administrator manually adds contacts to specific campaigns
- Useful for one-off communications or testing

**Automated Population:**
- Rate increases: Triggered when external systems update rate change data
- Regulatory notices: Triggered by compliance calendar events
- Policy updates: Triggered by carrier system integrations

**Bulk Import:**
- CSV uploads for large-scale campaign targeting
- API integrations for systematic campaign population

**Event-Driven:**
- Database triggers or application events automatically enroll contacts
- Real-time campaign activation based on contact behavior or external data

#### 3.7 Campaign Priority and Conflict Resolution

When multiple campaigns target the same contact on the same date:
1. **Priority-Based Selection**: Campaign with lowest priority number wins
2. **Exclusion Window Respect**: Campaigns respecting exclusion windows may be skipped while others proceed
3. **Follow-up Coordination**: Campaigns with follow-ups may influence scheduling of subsequent campaigns
4. **Volume Balancing**: Load balancing algorithms consider all campaign types together

### 4. State-Based Rules Engine

The system implements state-specific exclusion windows where no emails should be sent. These rules are categorized into three types:

#### 4.1 Birthday Window Rules
States with birthday-based exclusion windows:
- **CA**: 30 days before to 60 days after birthday
- **ID**: 0 days before to 63 days after birthday
- **KY**: 0 days before to 60 days after birthday
- **MD**: 0 days before to 30 days after birthday
- **NV**: 0 days before to 60 days after birthday (uses month start of birthday month)
- **OK**: 0 days before to 60 days after birthday
- **OR**: 0 days before to 31 days after birthday
- **VA**: 0 days before to 30 days after birthday

#### 4.2 Effective Date Window Rules
States with effective date-based exclusion windows:
- **MO**: 30 days before to 33 days after effective date anniversary

#### 4.3 Year-Round Exclusion Rules
States where no marketing emails are sent:
- **CT**: No emails sent year-round
- **MA**: No emails sent year-round
- **NY**: No emails sent year-round
- **WA**: No emails sent year-round

### 5. Exclusion Window Calculation

#### 5.1 Pre-Window Exclusion
All exclusion windows are extended by 60 days before their start date. This ensures emails are not sent just prior to the statutory exclusion window, so any new policy effective date won't be in the statutory exclusion window.

Example: If a birthday window starts on March 1st, the actual exclusion period begins on December 30th of the previous year (60 days before March 1st).

#### 5.2 Special Rules
- **Nevada (NV)**: Uses the first day of the birth month instead of the actual birth date for window calculation
- **Age 76+ Rule**: Some states may implement special handling for contacts aged 76 or older (year-round exclusion) -- none currently but this can happen in the future

#### 5.3 Window Spanning Years
Exclusion windows can span across calendar years. The system handles these cases by checking:
1. If the window crosses years (e.g., December to February)
2. Whether the current date falls in the first part (December) or second part (January-February)
(other approaches ok, just have to make sure we gracefully handle the case where the window spans years)

### 6. Email Scheduling Logic

#### 6.1 Anniversary Date Calculation
For both birthdays and effective dates:
1. Calculate the next anniversary from today
2. For February 29th dates, use February 28th in non-leap years
3. If this year's anniversary has passed, use next year's

#### 6.2 Email Date Calculation

**Anniversary-Based Emails:**
- Birthday emails: Anniversary date - 14 days (configurable)
- Effective date emails: Anniversary date - 30 days (configurable)
- AEP emails: September 15th of current year (configurable)
- Post-window emails: Day after exclusion window ends

**Campaign-Based Emails:**
- Campaign send date = trigger_date + days_before_event (from campaign configuration)
- If days_before_event is positive, sent before the trigger date
- If days_before_event is negative, sent after the trigger date
- If days_before_event is 0, sent on the trigger date

#### 6.3 Scheduling Process

**Anniversary-Based Email Scheduling:**
1. Determine contact's state from ZIP code
2. Check for state-specific rules
3. Calculate exclusion window (if applicable)
4. For each anniversary email type:
   - **Birthday**: If birthday is present, calculate anniversary date and scheduled send date
   - **Effective Date**: If effective_date is present, calculate anniversary date and scheduled send date
   - **AEP**: Calculate scheduled send date (September 15th)
   - For each calculated date, check if it falls within exclusion window
   - Mark as "skipped" if excluded, "pre-scheduled" if not
5. If any emails are skipped due to exclusion window:
   - Add a post-window email for the day after the window ends

**Campaign-Based Email Scheduling:**
1. Query active campaign instances (where current_date is between active_start_date and active_end_date)
2. For each active campaign instance, query target contacts from contact_campaigns table
3. For each contact-campaign instance combination:
   - Calculate send date based on trigger_date and campaign type's days_before_event
   - Check campaign type's respect_exclusion_windows flag
   - If flag is true, apply state exclusion window rules
   - If flag is false, schedule regardless of exclusion windows
   - Mark as "skipped" if excluded, "pre-scheduled" if not
   - Include email_template and sms_template from campaign instance
   - Set campaign_instance_id in email_schedules for template resolution
4. Apply campaign priority rules for conflicting send dates

**Complete Scheduling Process:**
1. **Clear Previous Schedules**: Delete all pre-scheduled and skipped emails for contacts being processed
2. **Process Anniversary Emails**: Calculate and schedule birthday, effective date, and AEP emails
3. **Process Campaign Emails**: Calculate and schedule all active campaign emails
4. **Apply Exclusion Windows**: Check state rules and mark excluded emails as skipped
5. **Add Post-Window Emails**: Create catch-up emails for after exclusion periods
6. **Apply Load Balancing**: Distribute emails evenly across days
7. **Enforce Frequency Limits**: Ensure contacts don't receive too many emails
8. **Combine and Sort**: Merge anniversary-based and campaign-based emails
9. Check if the contact has received too many emails in the last period_days days (do *not* do this for followup emails -- but we want to make sure that we don't send too many emails to the same contact in a short period of time. Campaign emails with higher priority take precedence over lower priority emails when frequency limits are reached.)

### 7. Load Balancing and Smoothing Logic

The system implements sophisticated load balancing to prevent email clustering and ensure even distribution of sending volume, particularly important for effective date emails that often cluster around the first of the month.

#### 7.1 Daily Volume Caps
- **Organizational Cap**: Maximum emails per day calculated as a percentage of total contacts (default: 7% of org contacts)
- **Effective Date Soft Limit**: Specific limit for effective date emails per day (default: 15 emails, or 30% of daily org cap, whichever is lower)
- **Over-Limit Detection**: Days exceeding 120% of daily cap are flagged for redistribution

#### 7.2 Effective Date Smoothing
Effective date emails are particularly prone to clustering because many policies have effective dates on the 1st of the month. The smoothing algorithm:

1. **Cluster Detection**: Counts how many effective date emails are scheduled for each day
2. **Threshold Application**: If a day exceeds the effective date soft limit, smoothing is applied
3. **Jitter Calculation**: Uses a deterministic hash of contact_id + event_type + event_year to calculate a jitter value
4. **Window Distribution**: Spreads emails across a configurable window (default: ±2 days from original date)
5. **Future Date Validation**: Ensures smoothed dates are never in the past

Example: If 50 effective date emails are scheduled for March 1st (exceeding the limit), they're redistributed across February 27th through March 3rd using deterministic jitter.

#### 7.3 Global Daily Cap Enforcement
When any day exceeds the organizational daily cap:

1. **Overflow Detection**: Identifies days with excessive email volume
2. **Next-Day Migration**: Moves excess emails to the following day if it has lower volume
3. **Cascade Prevention**: Ensures the next day doesn't become excessively overloaded
4. **Update Tracking**: Adjusts daily counts to reflect redistributed emails

#### 7.4 Catch-Up Email Distribution
For emails whose ideal send date has passed but the event is still in the future:

1. **Catch-Up Window**: Spreads catch-up emails across a configurable window (default: 7 days)
2. **Hash-Based Distribution**: Uses deterministic hashing to ensure consistent assignment
3. **Even Distribution**: Prevents all catch-up emails from being sent on the same day

#### 7.5 Performance Optimization for Scale

For handling up to 3 million contacts:

1. **Streaming Processing**:
   - Process contacts in chunks of 10,000
   - Use database cursors to avoid memory exhaustion
   - Calculate schedules in batches

2. **Optimized Indexes**:
   ```sql
   CREATE INDEX idx_contacts_state_birthday ON contacts(state, birthday);
   CREATE INDEX idx_contacts_state_effective ON contacts(state, effective_date);
   CREATE INDEX idx_campaigns_active ON campaign_instances(active_start_date, active_end_date);
   CREATE INDEX idx_schedules_lookup ON email_schedules(contact_id, email_type, scheduled_send_date);
   ```

3. **Batch Operations**:
   - Use prepared statements for all queries
   - Batch INSERTs up to 2,000 records per transaction
   - Use UPSERT operations where appropriate

#### 7.6 Configuration Parameters
```yaml
load_balancing:
  daily_send_percentage_cap: 0.07          # 7% of org contacts per day
  ed_daily_soft_limit: 15                  # Soft cap for ED emails per day
  ed_smoothing_window_days: 5              # ±2 days window for ED smoothing
  catch_up_spread_days: 7                  # Window for catch-up distribution
  overage_threshold: 1.2                   # 120% of cap triggers redistribution
```

#### 7.7 Benefits of Smoothing
- **Reduced Server Load**: Prevents overwhelming email infrastructure on peak days
- **Better Deliverability**: ISPs are less likely to throttle when volume is consistent
- **Improved User Experience**: Recipients don't receive large bursts of emails
- **Operational Efficiency**: Easier to manage sending infrastructure with predictable volume

### 8. Database Transaction Management

#### 8.1 Transaction Boundaries

All scheduling operations use explicit transaction boundaries:

```sql
BEGIN IMMEDIATE;  -- Prevent concurrent writes

-- 1. Create audit checkpoint
INSERT INTO scheduler_checkpoints (
    run_timestamp, 
    scheduler_run_id,
    contacts_checksum, 
    status
) VALUES (?, ?, ?, 'started');

-- 2. Clear existing schedules in batches
DELETE FROM email_schedules 
WHERE status IN ('pre-scheduled', 'skipped') 
AND contact_id IN (SELECT id FROM contacts LIMIT 10000);

-- 3. Process and insert new schedules
INSERT OR IGNORE INTO email_schedules (...) 
SELECT ... LIMIT 10000;

-- 4. Update checkpoint
UPDATE scheduler_checkpoints 
SET status = 'completed', 
    schedules_after_checksum = ?,
    contacts_processed = ?,
    emails_scheduled = ?,
    emails_skipped = ?,
    completed_at = CURRENT_TIMESTAMP
WHERE id = ?;

COMMIT;
```

#### 8.2 Audit and Recovery

**Checkpoint Table**:
```sql
CREATE TABLE scheduler_checkpoints (
    id INTEGER PRIMARY KEY,
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
);
```

**Point-in-Time Backup Strategy**:
1. Create timestamped backup before processing
2. Verify backup integrity with PRAGMA integrity_check
3. Maintain rolling window of backups (7 days)
4. Store backups on persistent volume (fly.io volume mount)

### 9. Batch Processing

TBD -- no batching should be need for scheduling process, only for scheduling emails. However, it is helpful to have some sort of batch identifier so we can see in the database which when an email schedule was created or updated.

### 10. Database Operations

#### 10.1 Email Schedules Table Schema
```sql
CREATE TABLE email_schedules (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    contact_id INTEGER NOT NULL,
    email_type TEXT NOT NULL,                     -- 'birthday', 'campaign_rate_increase', 'followup_1_cold', etc.
    scheduled_send_date DATE NOT NULL,
    scheduled_send_time TIME DEFAULT '08:30:00',  -- configurable
    status TEXT NOT NULL DEFAULT 'pre-scheduled',
    skip_reason TEXT,
    priority INTEGER DEFAULT 10,                  -- Lower numbers = higher priority
    campaign_instance_id INTEGER,                 -- For campaign-based emails, references campaign_instances.id
    email_template TEXT,                          -- Template to use for this email (from campaign instance or default)
    sms_template TEXT,                            -- Template to use for SMS (if applicable)
    scheduler_run_id TEXT,                        -- Added for audit trail
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    actual_send_datetime DATETIME,
    UNIQUE(contact_id, email_type, scheduled_send_date),
    FOREIGN KEY (campaign_instance_id) REFERENCES campaign_instances(id),
    INDEX idx_scheduler_run (scheduler_run_id),
    INDEX idx_status_date (status, scheduled_send_date)
);
```

#### 10.2 Campaign System Tables
```sql
-- Base campaign type definitions (reusable patterns)
CREATE TABLE campaign_types (
    name TEXT PRIMARY KEY,                        -- 'rate_increase', 'seasonal_promo', etc.
    respect_exclusion_windows BOOLEAN DEFAULT TRUE,
    enable_followups BOOLEAN DEFAULT TRUE,
    days_before_event INTEGER DEFAULT 0,
    target_all_contacts BOOLEAN DEFAULT FALSE,
    priority INTEGER DEFAULT 10,
    active BOOLEAN DEFAULT TRUE,                  -- Can this campaign type be used?
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Specific campaign instances (actual campaigns with templates)
CREATE TABLE campaign_instances (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    campaign_type TEXT NOT NULL,                  -- References campaign_types.name
    instance_name TEXT NOT NULL,                  -- 'spring_2024_promo', 'rate_increase_q1_2024'
    email_template TEXT,                          -- Template identifier for email sending system
    sms_template TEXT,                            -- Template identifier for SMS sending system
    active_start_date DATE,                       -- When this instance becomes active
    active_end_date DATE,                         -- When this instance expires
    metadata TEXT,                                -- JSON for instance-specific config overrides
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(campaign_type, instance_name),
    FOREIGN KEY (campaign_type) REFERENCES campaign_types(name)
);

-- Contact-campaign targeting associations (now references specific instances)
CREATE TABLE contact_campaigns (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    contact_id INTEGER NOT NULL,
    campaign_instance_id INTEGER NOT NULL,       -- References campaign_instances.id
    trigger_date DATE,                            -- When to send (for rate_increase, etc.)
    status TEXT DEFAULT 'pending',               -- 'pending', 'scheduled', 'sent', 'skipped'
    metadata TEXT,                               -- JSON field for contact-specific data
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(contact_id, campaign_instance_id, trigger_date),
    FOREIGN KEY (campaign_instance_id) REFERENCES campaign_instances(id),
    FOREIGN KEY (contact_id) REFERENCES contacts(id)
);
```

#### 10.3 Status Values
- **pre-scheduled**: Email is scheduled for future sending
- **skipped**: Email was skipped due to exclusion window
- **scheduled**: Email is queued for immediate sending
- **processing**: Email is being sent
- **sent**: Email was successfully sent
(The email scheduler we are building here will only use pre-scheduled and skipped statuses -- but will need to be able utilize the other statuses for the purpose of determining if an email is being sent too close to another email for the same contact.)

#### 10.4 Email Types
The email_type field supports the following values:

**Anniversary-Based Email Types:**
- **birthday**: Birthday-based emails (uses default birthday template)
- **effective_date**: Effective date anniversary emails (uses default effective date template)
- **aep**: Annual Enrollment Period emails (uses default AEP template)
- **post_window**: Post-exclusion window emails (uses default post-window template)

**Campaign-Based Email Types:**
- **campaign_{campaign_type}**: Dynamic email types based on campaign type (e.g., 'campaign_rate_increase', 'campaign_seasonal_promo')
  - Template determined by campaign_instance.email_template field
  - SMS template (if applicable) determined by campaign_instance.sms_template field

**Follow-up Email Types:**
- **followup_1_cold**: Cold follow-up emails (uses default cold follow-up template)
- **followup_2_clicked_no_hq**: Follow-up for contacts who clicked but didn't answer health questions
- **followup_3_hq_no_yes**: Follow-up for contacts who answered health questions with no conditions
- **followup_4_hq_with_yes**: Follow-up for contacts who answered health questions with conditions

#### 10.5 Template Resolution
Templates are resolved in the following order:
1. **Campaign-based emails**: Use email_template and sms_template from the campaign_instances table
2. **Anniversary-based emails**: Use predefined templates based on email_type
3. **Follow-up emails**: Use predefined follow-up templates based on email_type and parent email context

#### 10.6 Database Operations
1. **Clear existing schedules**: Removes all pre-scheduled and skipped entries for contacts being processed
2. **Campaign instance synchronization**: Updates contact_campaigns table based on external triggers and active campaign instances
3. **Template resolution**: Determines appropriate email/SMS templates based on campaign instance or email type
4. **Batch insert**: Uses INSERT OR IGNORE with ON CONFLICT to handle duplicates
5. **Transaction management**: Each batch is committed separately for reliability
6. **Campaign management**: CRUD operations for campaign types, instances, and contact targeting
7. **Instance lifecycle**: Automatic activation/deactivation based on active_start_date and active_end_date

### 11. Performance Optimizations

#### 11.1 Date-Based Contact Queries
For daily processing of birthdays and effective dates:
- Uses SQL date functions to find contacts by month and day
- Ignores year component for anniversary matching
- Supports batch processing of multiple dates

#### 11.2 Load Balancing and Smoothing
- Prevents email clustering through deterministic distribution algorithms
- Reduces peak infrastructure load by spreading volume across multiple days
- Maintains consistent daily sending volumes for better deliverability
- Uses hash-based jitter for predictable but distributed email scheduling

#### 11.3 Asynchronous Processing
(TBD -- this was a python-specific optimization, not sure if it's needed here)
- Database operations run in thread pool to avoid blocking
- Multiple batches can be processed concurrently
- Timing metrics track performance of each step

### 12. Configuration Management

#### 12.1 Timing Constants
```yaml
timing_constants:
  birthday_email_days_before: 14        # Days before birthday to send email
  effective_date_days_before: 30        # Days before effective date to send email
  pre_window_exclusion_days: 60         # Days to extend exclusion window backwards
```

#### 12.2 Campaign Configuration

**Campaign Types (Base Configurations):**
```yaml
campaign_types:
  rate_increase:
    respect_exclusion_windows: true
    enable_followups: true
    days_before_event: 14
    target_all_contacts: false
    priority: 1
    active: true
  
  seasonal_promo:
    respect_exclusion_windows: true
    enable_followups: true
    days_before_event: 7
    target_all_contacts: false
    priority: 5
    active: true
  
  initial_blast:
    respect_exclusion_windows: false
    enable_followups: false
    days_before_event: 0
    target_all_contacts: true
    priority: 10
    active: true
```

**Campaign Instances (Specific Executions):**
```yaml
campaign_instances:
  # Multiple rate increase campaigns running simultaneously
  - campaign_type: rate_increase
    instance_name: rate_increase_q1_2024
    email_template: rate_increase_standard_v2
    sms_template: rate_increase_sms_v1
    active_start_date: 2024-01-01
    active_end_date: 2024-03-31
  
  - campaign_type: rate_increase
    instance_name: rate_increase_q2_2024
    email_template: rate_increase_enhanced_v3
    sms_template: rate_increase_sms_v2
    active_start_date: 2024-04-01
    active_end_date: 2024-06-30
  
  # Multiple seasonal promotions with different templates
  - campaign_type: seasonal_promo
    instance_name: spring_enrollment_2024
    email_template: spring_promo_email_v1
    sms_template: spring_promo_sms_v1
    active_start_date: 2024-03-01
    active_end_date: 2024-05-31
  
  - campaign_type: seasonal_promo
    instance_name: fall_enrollment_2024
    email_template: fall_promo_email_v2
    sms_template: fall_promo_sms_v2
    active_start_date: 2024-09-01
    active_end_date: 2024-11-30
```

#### 12.3 AEP Configuration
```yaml
aep_config:
  default_dates:
    - month: 9
      day: 15
  years: [2023, 2024, 2025, 2026, 2027]
```

#### 12.4 State Rules Configuration
Stored in YAML format with:
- Rule type (birthday_window, effective_date_window, year_round)
- Window parameters (window_before, window_after)
- Special rules (use_month_start, age_76_plus)

#### 12.5 Versioned Configuration Management

All configuration stored in versioned format:

```sql
CREATE TABLE config_versions (
    id INTEGER PRIMARY KEY,
    config_type TEXT NOT NULL,
    config_data TEXT NOT NULL,  -- JSON
    valid_from DATETIME NOT NULL,
    valid_to DATETIME,
    created_at DATETIME NOT NULL,
    created_by TEXT
);
```

This ensures configuration changes are tracked and can be rolled back if needed.

### 13. Error Handling and Recovery

- **Missing Required Fields**: Contacts missing email or zip_code are skipped, logged in audit table
- **Invalid ZIP Codes**: Skip contact, increment invalid_contact_count
- **Invalid Dates**: February 29th in non-leap years converts to February 28th
- **Transaction Failures**: Automatic retry with exponential backoff, rollback entire batch
- **Partial Processing**: Track progress in checkpoints for resumability
- **Batch Failures**: Individual batch rollback without affecting other batches
- **Database Errors**: Automatic retry with exponential backoff

### 14. Monitoring and Observability

**Key Metrics to Track**:
- Processing time per batch
- Emails scheduled/skipped per run
- Daily volume distribution
- Exclusion window hit rate
- Campaign effectiveness metrics
- Contacts fetched and processed
- Performance timing for each operation

**Health Checks**:
- Database connection status
- Last successful run timestamp
- Pending schedule backlog
- Error rate thresholds

**Logging and Monitoring**:
The system provides detailed logging for:
- Contacts fetched and processed
- Emails scheduled, skipped, or sent
- Exclusion window calculations
- Performance timing for each operation
- Error conditions with full stack traces

### 15. Key Business Rules Summary

1. **No emails during exclusion windows**: Strictly enforced based on state rules
2. **Post-window catch-up**: Ensures contacts receive communication after exclusion periods
3. **Anniversary-based scheduling**: Emails tied to recurring annual dates
4. **State compliance**: Different rules for different states based on regulations
5. **Batch reliability**: Failed batches don't affect successful ones
6. **Idempotency**: Re-running scheduling won't create duplicates (INSERT OR IGNORE)
7. **Date handling**: Consistent handling of leap years and month-end dates

### 16. Integration Points

- **ZIP to State Mapping**: Uses pre-loaded ZIP code database
- **Contact Rules Engine**: Modular engine for applying state-specific rules
- **Email/SMS Sending**: Integrates with SendGrid (email) and Twilio (SMS)
- **Webhook Handling**: Processes delivery notifications from email/SMS providers

### 17. Data Flow

1. **Daily Scheduling**:
   - Fetch contacts with birthdays/effective dates in target window
   - Apply state rules and calculate exclusion windows
   - Generate email schedules
   - Store in database with appropriate status

2. **Email Sending**:
(handled separately)
   - Query for emails due today with status 'pre-scheduled'
   - Send via appropriate channel (email/SMS)
   - Update status and track delivery

3. **Webhook Processing**:
(handled separately)
   - Receive delivery notifications
   - Update email status
   - Log delivery metrics

### 18. Follow-up Email Scheduling

The system implements an intelligent follow-up scheduling algorithm that:
1. Identifies initial emails (anniversary-based: birthday, effective_date, aep, post_window; campaign-based: any campaign with enable_followups=true) that need follow-ups
2. Schedules follow-ups 2 days after the initial email was sent (configurable)
3. Determines the appropriate follow-up template based on user behavior
4. Respects campaign-specific follow-up settings

#### 18.1 Follow-up Email Types

The system uses four follow-up templates based on user engagement hierarchy:
1. **followup_4_hq_with_yes**: Contact answered health questions with medical conditions (highest priority)
2. **followup_3_hq_no_yes**: Contact answered health questions with no medical conditions
3. **followup_2_clicked_no_hq**: Contact clicked a link but didn't answer health questions
4. **followup_1_cold**: Contact didn't click or answer health questions (lowest priority)

#### 18.2 Follow-up Scheduling Process

1. **Identify Eligible Emails**:
   - Find emails with status 'sent' or 'delivered'
   - Filter for anniversary-based email types (birthday, effective_date, aep, post_window)
   - Filter for campaign-based email types where the campaign has enable_followups=true
   - Look back 35 days by default
   - Exclude contacts that already have follow-ups scheduled or sent

2. **Determine Follow-up Type**:
   - Check if contact clicked links (tracking_clicks table)
   - Check if contact answered health questions (contact_events table with event_type='eligibility_answered')
   - Evaluate medical conditions from metadata (has_medical_conditions flag or main_questions_yes_count)
   - Select highest applicable follow-up type based on behavior

3. **Schedule Follow-up**:
   - Default: 2 days after initial email (configurable)
   - If already past due, schedule for tomorrow
   - Include metadata tracking initial email details and behavior analysis
   - Support for SMS follow-ups if phone number available
   - Inherit priority from original campaign (if campaign-based) or use default priority (if anniversary-based)

#### 18.3 Campaign-Specific Follow-up Rules

- **Campaign Enable/Disable**: Only campaigns with enable_followups=true generate follow-up emails
- **Priority Inheritance**: Follow-up emails inherit the priority of their parent campaign
- **Exclusion Window Respect**: Follow-ups always respect exclusion windows regardless of parent campaign settings
- **Metadata Tracking**: Follow-ups include campaign_name for traceability when generated from campaign emails

#### 18.4 Active Follow-up Scheduler Features

- **Continual Re-evaluation**: Can update follow-up type if user behavior changes before sending
- **Batch Processing**: Processes multiple contacts in parallel for performance
- **Idempotent**: Tracks processed emails to avoid duplicates
- **Metadata Tracking**: Stores decision rationale and behavior details
- **Campaign-Aware**: Handles both anniversary-based and campaign-based initial emails

#### 18.5 Database Schema for Follow-ups

Follow-ups use the same email_schedules table with:
- email_type: 'followup_1_cold', 'followup_2_clicked_no_hq', etc.
- metadata: JSON containing initial_comm_log_id, initial_email_type, followup_behavior details
- campaign_instance_id: Set to parent campaign instance ID for campaign-based follow-ups, null for anniversary-based
- email_template: Default follow-up template unless overridden by campaign instance metadata
- sms_template: Default follow-up SMS template unless overridden by campaign instance metadata
- priority: Inherited from parent email/campaign
- event_year/month/day: Inherited from initial email for birthday/effective_date follow-ups

#### 18.6 Performance Optimizations

- Batch fetching of contact data, click data, and health question events using sql queries
- Parallel processing using multiprocessing pool (TBD -- not sure if this is needed here)
- Large batch SQL execution (up to 2000 statements per transaction)
- Campaign configuration caching to avoid repeated database queries

### 19. Campaign System Benefits and Implementation Notes

The abstract campaign system provides significant advantages over individual email type implementations:

#### 19.1 Operational Benefits
- **Reduced Code Complexity**: New campaign types require only configuration, not code changes
- **Unified Management**: All campaign types use the same scheduling, tracking, and reporting infrastructure
- **Flexible Targeting**: Campaigns can target all contacts or specific subsets based on various criteria
- **Configurable Compliance**: Per-campaign control over exclusion window compliance and follow-up generation

#### 19.2 Business Benefits
- **Rapid Campaign Deployment**: New marketing initiatives can be launched quickly through configuration
- **A/B Testing Support**: Multiple campaign configurations can be tested simultaneously
- **Regulatory Flexibility**: Campaigns can be configured to meet different compliance requirements
- **Scalable Architecture**: System can handle unlimited campaign types without performance degradation

#### 19.3 Implementation Considerations
- **Database Migration**: Existing scheduled_rate_increase emails should be migrated to the campaign instance system
- **Template Management**: Email and SMS sending systems must integrate with campaign instance template resolution
- **Multiple Instance Support**: Scheduler must handle multiple active instances of the same campaign type simultaneously
- **Instance Lifecycle**: Automatic activation/deactivation of campaign instances based on date ranges
- **Configuration Management**: Campaign configurations should be version-controlled and auditable
- **Monitoring and Alerting**: Campaign performance metrics should be tracked per instance and campaign type
- **API Integration**: External systems should be able to create and manage campaign instances programmatically

#### 19.4 Migration Strategy
1. **Create Campaign Type Definitions**: Set up base campaign types (rate_increase, initial_blast, seasonal_promo) in the campaign_types table
2. **Create Initial Campaign Instances**: Set up specific campaign instances with templates and date ranges
3. **Migrate Existing Data**: Convert existing rate increase schedules to campaign instance-based schedules
4. **Integrate Template Resolution**: Update email/SMS sending systems to use template information from email_schedules table
5. **Update Scheduling Logic**: Modify scheduler to handle both anniversary-based and campaign instance-based emails
6. **Test Multiple Instance Support**: Ensure system can handle multiple simultaneous instances of the same campaign type
7. **Deploy Incrementally**: Roll out campaign instance system alongside existing functionality before full cutover

This comprehensive campaign instance-aware business logic ensures reliable, compliant, and efficient email scheduling across multiple states with varying regulations, while providing the flexibility to rapidly deploy multiple simultaneous campaigns with different templates and targeting criteria.

