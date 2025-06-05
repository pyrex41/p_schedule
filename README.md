# Email Scheduling System

A sophisticated, domain-specific language (DSL) based email scheduling system for multi-state compliance with anniversary-based and campaign-based emails.

## Overview

This email scheduling system manages automated email and SMS campaigns for multiple organizations with up to 3 million contacts. It uses a sophisticated rule engine to determine when to send different types of communications based on contact information, state-specific regulations, and timing constraints.

### Key Features

- **State-specific compliance**: Automatically handles different exclusion rules for different states
- **Anniversary-based emails**: Birthday, effective date, and AEP emails
- **Flexible campaign system**: Support for unlimited campaign types with configurable behavior
- **Load balancing**: Sophisticated smoothing to prevent email clustering
- **Domain-specific language**: Declarative configuration for business rules
- **Scalable architecture**: Handles up to 3 million contacts with batch processing
- **Audit trail**: Comprehensive logging and recovery capabilities

## Architecture

### Core Components

1. **Email Scheduler** (`scheduler.py`) - Main orchestrator
2. **Campaign Scheduler** (`campaign_scheduler.py`) - Handles campaign-based emails
3. **State Rules Engine** - Manages state-specific exclusion rules
4. **Database Manager** - Handles all database operations
5. **Load Balancer** - Distributes email volume evenly
6. **Configuration System** - YAML-based declarative configuration

### Email Types

#### Anniversary-Based Emails
- **Birthday**: Sent 14 days before contact's birthday
- **Effective Date**: Sent 30 days before policy effective date anniversary
- **AEP**: Annual Enrollment Period emails (typically September 15)
- **Post Window**: Catch-up emails sent after exclusion windows end

#### Campaign-Based Emails
- **Rate Increase**: Advance notification of premium changes
- **Seasonal Promotions**: Configurable marketing campaigns
- **Initial Blast**: System introduction emails
- **Regulatory Notices**: Compliance-required communications
- **Custom Campaigns**: Flexible campaigns for any purpose

## Installation and Setup

### Prerequisites

- Python 3.8+
- SQLite3
- PyYAML

### Install Dependencies

```bash
# On Ubuntu/Debian
sudo apt install python3-yaml sqlite3

# On other systems, you may need:
pip install PyYAML
```

### Database Setup

The scheduler automatically creates and updates the database schema. No manual setup required.

## Usage

### Basic Usage

```bash
# Run full scheduling for all contacts
python3 scheduler.py --db your-database.sqlite3 --run-full

# Use custom configuration
python3 scheduler.py --db your-database.sqlite3 --config scheduler_config.yaml --run-full

# Enable debug logging
python3 scheduler.py --db your-database.sqlite3 --run-full --debug
```

### Campaign Management

```bash
# Set up sample campaigns
python3 campaign_scheduler.py --db your-database.sqlite3 --setup-samples
```

### Configuration

Create a `scheduler_config.yaml` file to customize behavior:

```yaml
timing_constants:
  send_time: "08:30:00"
  birthday_email_days_before: 14
  effective_date_days_before: 30

load_balancing:
  daily_send_percentage_cap: 0.07
  ed_daily_soft_limit: 15

campaign_types:
  rate_increase:
    respect_exclusion_windows: true
    enable_followups: true
    days_before_event: 14
    priority: 1
```

## State Exclusion Rules

The system implements state-specific exclusion windows where no emails should be sent:

### Year-Round Exclusion States
- **CT** (Connecticut)
- **MA** (Massachusetts) 
- **NY** (New York)
- **WA** (Washington)

### Birthday-Based Exclusion Windows
- **CA**: 30 days before to 60 days after birthday
- **ID**: 0 days before to 63 days after birthday
- **KY**: 0 days before to 60 days after birthday
- **MD**: 0 days before to 30 days after birthday
- **NV**: 0 days before to 60 days after birthday (uses month start)
- **OK**: 0 days before to 60 days after birthday
- **OR**: 0 days before to 31 days after birthday
- **VA**: 0 days before to 30 days after birthday

### Effective Date-Based Exclusion Windows
- **MO**: 30 days before to 33 days after effective date anniversary

All exclusion windows include a 60-day pre-window extension.

## Campaign System

### Campaign Types (Reusable Patterns)

Campaign types define reusable behavior patterns:

```python
rate_increase = CampaignType(
    name="rate_increase",
    respect_exclusion_windows=True,
    enable_followups=True,
    timing_rule=TimingRule(days_before_event=14),
    priority=1
)
```

### Campaign Instances (Specific Executions)

Campaign instances represent specific executions with unique templates:

```python
q1_rate_increase = CampaignInstance(
    campaign_type="rate_increase",
    instance_name="rate_increase_q1_2025",
    email_template="rate_increase_standard_v2",
    sms_template="rate_increase_sms_v1",
    active_start_date=date(2025, 1, 1),
    active_end_date=date(2025, 3, 31)
)
```

### Contact Targeting

Contacts are linked to campaigns through the `contact_campaigns` table:

```sql
INSERT INTO contact_campaigns 
(contact_id, campaign_instance_id, trigger_date, status)
VALUES (123, 1, '2025-07-15', 'pending');
```

## Database Schema

### Core Tables

#### contacts
```sql
id, first_name, last_name, email, zip_code, state, 
birth_date, effective_date, phone_number
```

#### email_schedules
```sql
id, contact_id, email_type, scheduled_send_date, scheduled_send_time,
status, skip_reason, priority, campaign_instance_id, email_template,
sms_template, scheduler_run_id, event_year, event_month, event_day
```

#### campaign_types
```sql
name, respect_exclusion_windows, enable_followups, days_before_event,
target_all_contacts, priority, active
```

#### campaign_instances
```sql
id, campaign_type, instance_name, email_template, sms_template,
active_start_date, active_end_date, metadata
```

#### contact_campaigns
```sql
id, contact_id, campaign_instance_id, trigger_date, status, metadata
```

## Load Balancing and Smoothing

The system implements sophisticated load balancing to prevent email clustering:

### Daily Volume Caps
- **Organizational Cap**: Maximum 7% of total contacts per day
- **Effective Date Soft Limit**: 15 emails per day (configurable)
- **Over-Limit Detection**: Days exceeding 120% of cap trigger redistribution

### Effective Date Smoothing
Spreads clustered effective date emails across a ±2 day window using deterministic hashing.

### Global Daily Cap Enforcement
Migrates excess emails to following days when daily caps are exceeded.

## Domain-Specific Language (DSL)

The system uses DSL components to make business rules declarative:

### Timing Rules
```python
birthday_timing = TimingRule(days_before_event=14)
send_date = birthday_timing.calculate_send_date(anniversary_date)
```

### Exclusion Windows
```python
ca_birthday_window = ExclusionWindow(
    rule_type=ExclusionRuleType.BIRTHDAY_WINDOW,
    window_before_days=30,
    window_after_days=60,
    pre_window_extension_days=60
)
```

### State Rules
```python
ca_rule = StateRule(
    state_code="CA",
    exclusion_windows=[ca_birthday_window]
)
```

## Performance Optimization

### For Large Datasets (Up to 3M contacts)

1. **Batch Processing**: Processes contacts in configurable batches (default: 10,000)
2. **Streaming**: Uses database cursors to avoid memory exhaustion
3. **Optimized Indexes**: Database indexes for common query patterns
4. **Connection Pooling**: Efficient database connection management

### Recommended Indexes
```sql
CREATE INDEX idx_contacts_state_birthday ON contacts(state, birthday);
CREATE INDEX idx_contacts_state_effective ON contacts(state, effective_date);
CREATE INDEX idx_email_schedules_status_date ON email_schedules(status, scheduled_send_date);
```

## Monitoring and Observability

### Key Metrics Tracked
- Contacts processed per run
- Emails scheduled vs skipped
- Daily volume distribution
- Exclusion window hit rate
- Campaign effectiveness metrics
- Processing time per batch

### Audit Trail
- Scheduler checkpoints for recovery
- Configuration version tracking
- Campaign change logs
- Detailed error logging

## API Integration

### External System Integration Points

1. **Contact Management**: Import/update contact data
2. **Campaign Triggers**: External systems can trigger campaigns
3. **Template Management**: Integration with email/SMS sending systems
4. **Webhook Handling**: Process delivery notifications
5. **Analytics**: Export scheduling and delivery metrics

## Example Workflows

### 1. Setting Up a Rate Increase Campaign

```python
# 1. Create campaign type (one-time setup)
campaign_manager = CampaignManager("database.sqlite3")
rate_increase_type = CampaignType(
    name="rate_increase",
    respect_exclusion_windows=True,
    enable_followups=True,
    timing_rule=TimingRule(days_before_event=14),
    priority=1
)
campaign_manager.create_campaign_type(rate_increase_type)

# 2. Create campaign instance
q2_instance = CampaignInstance(
    id=None,
    campaign_type="rate_increase",
    instance_name="rate_increase_q2_2025",
    email_template="rate_increase_v3",
    active_start_date=date(2025, 4, 1),
    active_end_date=date(2025, 6, 30)
)
instance_id = campaign_manager.create_campaign_instance(q2_instance)

# 3. Add contacts to campaign
contact_campaigns = [
    ContactCampaign(
        contact_id=123,
        campaign_instance_id=instance_id,
        trigger_date=date(2025, 7, 1),  # Rate increase date
        status='pending'
    )
]
campaign_manager.add_contacts_to_campaign(instance_id, contact_campaigns)

# 4. Run scheduler
scheduler = EmailScheduler("database.sqlite3")
scheduler.run_full_schedule()
```

### 2. Running Daily Scheduling

```bash
#!/bin/bash
# Daily cron job script

# Run scheduler with error handling
python3 /path/to/scheduler.py \
    --db /path/to/production.sqlite3 \
    --config /path/to/production_config.yaml \
    --run-full

# Check exit code
if [ $? -eq 0 ]; then
    echo "Scheduler completed successfully"
else
    echo "Scheduler failed - check logs"
    exit 1
fi
```

## Troubleshooting

### Common Issues

1. **Missing ZIP codes/states**: Contacts with invalid data are skipped with warnings
2. **No campaign emails**: Check that campaign instances have valid active dates
3. **All emails skipped**: Verify state exclusion rules aren't too restrictive
4. **Performance issues**: Reduce batch size or enable streaming mode

### Debug Mode

```bash
python3 scheduler.py --db database.sqlite3 --run-full --debug
```

### Checking Scheduler Results

```sql
-- View scheduled emails
SELECT email_type, status, COUNT(*) 
FROM email_schedules 
WHERE scheduler_run_id = 'your-run-id'
GROUP BY email_type, status;

-- Check campaign emails
SELECT * FROM email_schedules 
WHERE email_type LIKE 'campaign_%' 
LIMIT 10;

-- View exclusion reasons
SELECT skip_reason, COUNT(*) 
FROM email_schedules 
WHERE status = 'skipped'
GROUP BY skip_reason;
```

## Contributing

### Adding New State Rules

1. Update `StateRulesEngine._load_default_rules()`
2. Add configuration to `scheduler_config.yaml`
3. Test with contacts from the new state

### Adding New Campaign Types

1. Define campaign type in configuration
2. Create campaign instances as needed
3. Add contacts to campaigns via API or direct database insertion

### Extending the DSL

1. Create new dataclasses for business concepts
2. Implement validation and calculation methods
3. Update scheduler to use new DSL components

## License

This system implements complex business logic for email scheduling compliance. Please ensure you understand and comply with all applicable regulations in your jurisdiction.

## Support

For questions about the business logic implementation, refer to the original `business_logic.md` document which contains the complete specification this system implements.