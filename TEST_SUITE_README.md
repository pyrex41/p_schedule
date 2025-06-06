# Core Scheduling Engine - Comprehensive Test Suite

This test suite provides rigorous validation of the Core Scheduling Engine's hybrid implementation, ensuring both functional correctness and performance excellence.

## Overview

The test suite validates that the hybrid scheduler implementation successfully combines:

- **Verifiable Correctness**: Clean, declarative business logic that can be audited
- **High Performance**: Query-driven, batch-oriented architecture that scales
- **Reliability**: Robust error handling and edge case management

## Test Suite Components

### 1. Functional Correctness Tests (`test_scheduler.py`)

Validates all business logic rules defined in `business_logic.md`:

#### State Exclusion Rules Testing
- **Year-round exclusion states** (NY, CT, MA, WA): Verifies all emails are properly skipped
- **Birthday window exclusions**: Tests state-specific birthday exclusion periods (CA, ID, KY, MD, NV, OK, OR, VA)
- **Effective date exclusions**: Validates MO's effective date exclusion window
- **Nevada special rule**: Tests the month-start calculation for NV birthday windows
- **No exclusion states**: Ensures states without rules (AZ, TX, FL) process normally

#### Anniversary Email Scheduling
- **Birthday email timing**: 14 days before birthday anniversary
- **Effective date timing**: 30 days before effective date anniversary  
- **AEP scheduling**: September 15th annual enrollment period emails
- **Leap year handling**: February 29th birthdays in non-leap years
- **Post-window emails**: Catch-up emails after exclusion windows end

#### Campaign Email Scheduling
- **Exclusion compliance**: Campaigns that respect vs. ignore exclusion windows
- **Timing calculations**: Based on `days_before_event` configuration
- **Campaign priorities**: Higher priority campaigns take precedence
- **Template assignment**: Proper email/SMS template selection

#### Load Balancing & Smoothing
- **Effective date smoothing**: Distributes clustered emails across multiple days
- **Daily volume caps**: Respects organizational sending limits
- **Even distribution**: Prevents overwhelming email infrastructure

#### Frequency Limiting
- **Contact frequency limits**: Prevents too many emails per contact per period
- **Follow-up exemptions**: Follow-up emails bypass frequency limits
- **Priority-based selection**: Higher priority emails take precedence

#### Edge Cases & Error Handling
- **Missing contact data**: Graceful handling of incomplete records
- **Invalid date formats**: Robust date parsing and error recovery
- **Concurrent access**: Prevention of duplicate schedules

### 2. Performance & Scalability Tests (`test_performance_comprehensive.py`)

Validates the performance claims of the hybrid architecture:

#### Query-Driven Performance
- **Contact filtering efficiency**: ~1000x improvement through targeted queries
- **Memory scaling**: Sub-linear memory growth with dataset size
- **Large dataset handling**: Performance with 2000+ contacts

#### Batch Processing Efficiency
- **N+1 query elimination**: Batch operations instead of per-contact queries
- **Campaign scheduling**: Efficient bulk campaign processing
- **Anniversary scheduling**: Optimized anniversary email generation

#### Load Balancing Performance
- **Smoothing at scale**: Distribution of 500+ clustered emails
- **Volume calculations**: Efficient daily capacity management
- **Distribution analysis**: Verification of even email spreading

#### Database Performance
- **Index effectiveness**: Query optimization through proper indexing
- **Batch inserts**: High-performance schedule creation
- **Query plan analysis**: Database optimization validation

### 3. Original Performance Tests (`test_performance.py`)

Legacy performance validation focusing on:
- Basic scheduling functionality
- Contact processing efficiency
- Memory usage patterns

## Setup and Installation

### Prerequisites

1. **Python 3.8+**
2. **Required packages**:
   ```bash
   pip install psutil PyYAML
   ```

3. **Required files** (must be in the same directory):
   - `golden_dataset.sqlite3` - Test database with representative data
   - `scheduler.py` - Main scheduler implementation
   - `scheduler_config.yaml` - Configuration file
   - Test files (`test_scheduler.py`, `test_performance_comprehensive.py`, etc.)

### Golden Dataset

The `golden_dataset.sqlite3` database contains:
- **24,701 contacts** across all US states
- **Representative data distribution** for testing state rules
- **Campaign system tables** with sample campaign types and instances
- **Proper schema** matching the production database
- **Performance indexes** for optimized testing

The dataset includes contacts specifically chosen to test:
- All state exclusion rules (year-round, birthday windows, effective date windows)
- Edge cases (February 29th birthdays, Nevada month-start rules)
- Load balancing scenarios (clustered effective dates)
- Campaign targeting examples

## Running the Tests

### Quick Start

Run the complete test suite:
```bash
python run_all_tests.py
```

### Test Options

**Functional tests only:**
```bash
python run_all_tests.py --functional-only
```

**Performance tests only:**
```bash
python run_all_tests.py --performance-only
```

**Quick validation (functional + basic performance):**
```bash
python run_all_tests.py --quick
```

**Individual test files:**
```bash
# Functional correctness
python test_scheduler.py

# Comprehensive performance
python test_performance_comprehensive.py

# Original performance tests
python test_performance.py
```

### Expected Output

Successful test runs will show:

```
🚀 CORE SCHEDULING ENGINE - COMPREHENSIVE TEST SUITE
================================================================================

🔍 CHECKING PREREQUISITES...
✅ All prerequisites satisfied

================================================================================
🧪 RUNNING FUNCTIONAL CORRECTNESS TESTS
================================================================================
[Detailed test execution output...]

📊 FUNCTIONAL CORRECTNESS TESTS Results:
   Duration: 15.23 seconds
   Status: ✅ PASSED
   Exit code: 0

[Additional test suites...]

================================================================================
📋 COMPREHENSIVE TEST REPORT
================================================================================
Overall Status: ✅ ALL PASSED
Total Test Suites: 3
Passed: 3
Failed: 0
Success Rate: 100.0%

🎉 CONGRATULATIONS!
All tests passed! The Core Scheduling Engine is working correctly and
demonstrates the expected performance characteristics.
```

## Test Categories and Coverage

### Business Logic Coverage

| Business Rule | Test Coverage |
|---------------|---------------|
| State Exclusion Rules | ✅ All states tested |
| Anniversary Scheduling | ✅ All email types |
| Campaign Scheduling | ✅ All campaign behaviors |
| Load Balancing | ✅ Smoothing algorithms |
| Frequency Limiting | ✅ Contact limits |
| Edge Cases | ✅ Error conditions |

### Performance Coverage

| Performance Aspect | Validation Method |
|-------------------|-------------------|
| Query Efficiency | Contact filtering ratios |
| Memory Scaling | Sub-linear growth verification |
| Batch Processing | Query count analysis |
| Database Optimization | Index usage verification |
| Load Distribution | Email clustering analysis |

## Interpreting Test Results

### Success Indicators

- **All tests pass**: Implementation meets functional and performance requirements
- **Sub-second functional tests**: Business logic is efficient
- **Linear performance scaling**: Architecture handles growth well
- **High filtering ratios**: Query-driven approach is effective

### Failure Indicators

- **State rule failures**: Business logic implementation issues
- **Performance degradation**: Architecture problems
- **Memory growth**: Inefficient data handling
- **Query count issues**: N+1 query problems

### Common Issues and Solutions

**Import Errors:**
```
ModuleNotFoundError: No module named 'scheduler'
```
*Solution*: Ensure `scheduler.py` is in the same directory as test files.

**Database Errors:**
```
FileNotFoundError: Golden dataset not found
```
*Solution*: Verify `golden_dataset.sqlite3` exists and was properly updated.

**Performance Failures:**
```
Memory usage should not scale linearly
```
*Solution*: Check for memory leaks or inefficient data structures in scheduler.

## Test Development Guidelines

### Adding New Tests

1. **Identify the business rule** from `business_logic.md`
2. **Create test data** that exercises the rule
3. **Write assertions** that verify correct behavior
4. **Add to appropriate test class** in `test_scheduler.py`

### Performance Test Guidelines

1. **Use realistic data sizes** (1000+ contacts for meaningful results)
2. **Measure actual metrics** (time, memory, query counts)
3. **Set reasonable thresholds** based on expected performance
4. **Test edge cases** (clustering, large datasets)

### Test Data Management

- **Use `create_test_contact()`** for specific test scenarios
- **Leverage golden dataset** for representative data
- **Create campaign data** with `add_campaign_data()`
- **Clean up** with proper `setUp()`/`tearDown()` methods

## Continuous Integration

The test suite is designed for CI/CD integration:

- **Exit codes**: 0 for success, 1 for failure
- **Timeout handling**: 10-minute limit per test suite
- **Detailed reporting**: Machine-readable output
- **Dependency checking**: Automatic prerequisite validation

Example CI configuration:
```yaml
test:
  script:
    - python run_all_tests.py --quick
  timeout: 15m
  artifacts:
    when: always
    reports:
      junit: test-results.xml
```

## Performance Benchmarks

Expected performance characteristics:

- **Contact filtering**: >90% reduction in processed contacts
- **Memory usage**: <50MB growth per 1000 additional contacts  
- **Schedule creation**: <5 seconds for 1000 contacts
- **Query efficiency**: <10 queries per 100 campaign emails

## Troubleshooting

### Test Environment Issues

**SQLite version compatibility:**
- Ensure SQLite 3.24+ for proper index support
- Check `PRAGMA foreign_keys = ON` functionality

**Python environment:**
- Use virtual environments to avoid package conflicts
- Verify Python 3.8+ for proper datetime handling

### Scheduler Issues

**Configuration problems:**
- Verify `scheduler_config.yaml` exists and is valid
- Check that all required configuration keys are present

**Database schema mismatches:**
- Run `update_golden_dataset.py` to ensure schema compatibility
- Verify all required tables and columns exist

### Performance Issues

**Slow test execution:**
- Check available system memory (8GB+ recommended)
- Ensure SSD storage for database operations
- Monitor background processes during testing

**Memory errors:**
- Reduce test dataset sizes if system memory is limited
- Check for memory leaks in scheduler implementation

## Contributing

When contributing to the test suite:

1. **Follow existing patterns** in test structure and naming
2. **Add comprehensive documentation** for new test cases
3. **Include both positive and negative test cases**
4. **Verify tests work with the golden dataset**
5. **Update this README** with new test categories

## Conclusion

This comprehensive test suite ensures the Core Scheduling Engine hybrid implementation delivers on its promises of correctness, performance, and scalability. Regular execution of these tests provides confidence that the scheduler will handle production workloads reliably and efficiently.

The combination of functional and performance testing creates a robust validation framework that can detect both business logic errors and performance regressions, making it an essential tool for maintaining and evolving the Core Scheduling Engine.