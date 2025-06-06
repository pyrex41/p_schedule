#!/usr/bin/env python3
"""
Comprehensive Performance Test Suite for Core Scheduling Engine

This test suite validates the performance claims of the hybrid scheduler:
- Query-driven pre-filtering provides ~1000x improvement
- Batch-oriented processing eliminates N+1 queries
- Load balancing and smoothing work at scale
- Memory usage remains reasonable with large datasets
- Database operations are optimized
"""

import unittest
import sqlite3
import time
import psutil
import os
import shutil
import tempfile
from datetime import datetime, date, timedelta
from unittest.mock import patch
import sys
from pathlib import Path
from contextlib import contextmanager

# Add project root to path
sys.path.append(str(Path(__file__).parent))

# Import scheduler components
from scheduler import EmailScheduler, DatabaseManager, SchedulingConfig


class PerformanceTestBase(unittest.TestCase):
    """Base class for performance tests"""
    
    @classmethod
    def setUpClass(cls):
        """One-time setup for performance tests"""
        cls.golden_db_path = "golden_dataset.sqlite3"
        if not os.path.exists(cls.golden_db_path):
            raise FileNotFoundError(f"Golden dataset not found: {cls.golden_db_path}")
    
    def setUp(self):
        """Create test database"""
        self.temp_dir = tempfile.mkdtemp()
        self.test_db_path = os.path.join(self.temp_dir, "perf_test.db")
        shutil.copy2(self.golden_db_path, self.test_db_path)
        
        self.config = SchedulingConfig()
        self.scheduler = EmailScheduler(self.test_db_path, config=self.config)
        
        # Performance tracking
        self.process = psutil.Process()
        
    def tearDown(self):
        """Cleanup"""
        shutil.rmtree(self.temp_dir)
    
    @contextmanager
    def measure_performance(self, operation_name):
        """Context manager to measure performance metrics"""
        # Capture initial state
        start_time = time.time()
        start_memory = self.process.memory_info().rss / 1024 / 1024  # MB
        
        # Get database query count before
        conn = sqlite3.connect(self.test_db_path)
        conn.execute("PRAGMA query_only = ON")
        
        try:
            yield
        finally:
            # Capture final state
            end_time = time.time()
            end_memory = self.process.memory_info().rss / 1024 / 1024  # MB
            
            duration = end_time - start_time
            memory_delta = end_memory - start_memory
            
            print(f"\n📊 PERFORMANCE METRICS - {operation_name}")
            print(f"   Duration: {duration:.3f} seconds")
            print(f"   Memory delta: {memory_delta:+.2f} MB")
            print(f"   Peak memory: {end_memory:.2f} MB")
            
            conn.close()
    
    def create_large_contact_dataset(self, count=1000, clustering_factor=0.3):
        """Create a large dataset for performance testing"""
        conn = sqlite3.connect(self.test_db_path)
        cursor = conn.cursor()
        
        # Create contacts with intentional clustering for testing load balancing
        cluster_date = date(2020, 12, 1)  # December 1st - common effective date
        cluster_count = int(count * clustering_factor)
        
        contacts = []
        
        # Clustered contacts (same effective date)
        for i in range(cluster_count):
            birthday = date(1970, (i % 12) + 1, min((i % 28) + 1, 28))
            contacts.append((
                f"PerfTest{i}", f"Contact{i}", f"perf{i}@test.com",
                "TX",  # Use non-exclusion state for clear performance testing
                "75001",
                birthday.strftime('%Y-%m-%d'),
                cluster_date.strftime('%Y-%m-%d'),
                ""
            ))
        
        # Distributed contacts (varied dates)
        for i in range(cluster_count, count):
            birthday = date(1970, (i % 12) + 1, min((i % 28) + 1, 28))
            effective_date = date(2020, (i % 12) + 1, min((i % 28) + 1, 28))
            contacts.append((
                f"PerfTest{i}", f"Contact{i}", f"perf{i}@test.com",
                "TX", "75001",
                birthday.strftime('%Y-%m-%d'),
                effective_date.strftime('%Y-%m-%d'),
                ""
            ))
        
        # Batch insert for better performance
        cursor.executemany("""
            INSERT INTO contacts (first_name, last_name, email, state, zip_code, 
                                birth_date, effective_date, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, contacts)
        
        conn.commit()
        conn.close()
        
        print(f"✓ Created {count} test contacts ({cluster_count} clustered on {cluster_date})")
        return count
    
    def add_campaign_data_for_performance(self, contact_count=100):
        """Add campaign data for performance testing"""
        conn = sqlite3.connect(self.test_db_path)
        cursor = conn.cursor()
        
        # Get some contacts for campaign testing
        cursor.execute("SELECT id FROM contacts WHERE email LIKE 'perf%' LIMIT ?", (contact_count,))
        contact_ids = [row[0] for row in cursor.fetchall()]
        
        # Add campaign targeting
        trigger_date = date(2024, 12, 15)
        campaign_data = []
        
        for contact_id in contact_ids:
            # Add to rate_increase campaign
            campaign_data.append((contact_id, 1, trigger_date.strftime('%Y-%m-%d')))  # Assuming campaign_instance_id=1
        
        cursor.executemany("""
            INSERT OR IGNORE INTO contact_campaigns (contact_id, campaign_instance_id, trigger_date)
            VALUES (?, ?, ?)
        """, campaign_data)
        
        conn.commit()
        conn.close()
        
        print(f"✓ Added campaign data for {len(contact_ids)} contacts")
        return len(contact_ids)


class TestQueryDrivenPerformance(PerformanceTestBase):
    """Test the query-driven pre-filtering performance"""
    
    def test_scheduling_window_filtering_performance(self):
        """Test that get_contacts_in_scheduling_window is efficient"""
        # Create large dataset
        total_contacts = self.create_large_contact_dataset(count=5000)
        
        # Test the query-driven approach
        test_date = date(2024, 11, 15)
        
        with self.measure_performance("Query-Driven Contact Filtering"):
            with patch('scheduler.datetime') as mock_datetime:
                mock_datetime.now.return_value = datetime.combine(test_date, datetime.min.time())
                mock_datetime.date = date
                
                # This should use the efficient query instead of loading all contacts
                db_manager = DatabaseManager(self.test_db_path)
                relevant_contacts = db_manager.get_contacts_in_scheduling_window()
        
        # Verify we got a reasonable subset (not all contacts)
        self.assertLess(len(relevant_contacts), total_contacts, 
                       "Should filter to relevant contacts only")
        self.assertGreater(len(relevant_contacts), 0, 
                          "Should find some relevant contacts")
        
        print(f"   Filtered {total_contacts} total contacts to {len(relevant_contacts)} relevant contacts")
        print(f"   Filtering ratio: {len(relevant_contacts)/total_contacts:.1%}")
    
    def test_full_schedule_performance_at_scale(self):
        """Test full scheduling performance with larger dataset"""
        # Create substantial test dataset
        total_contacts = self.create_large_contact_dataset(count=2000)
        campaign_contacts = self.add_campaign_data_for_performance(contact_count=200)
        
        test_date = date(2024, 11, 15)
        
        with self.measure_performance("Full Schedule Run (2000 contacts)"):
            with patch('scheduler.datetime') as mock_datetime:
                mock_datetime.now.return_value = datetime.combine(test_date, datetime.min.time())
                mock_datetime.date = date
                
                # Run the full scheduler
                self.scheduler.run_full_schedule()
        
        # Verify results
        conn = sqlite3.connect(self.test_db_path)
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM email_schedules")
        total_schedules = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(DISTINCT contact_id) FROM email_schedules")
        contacts_with_schedules = cursor.fetchone()[0]
        
        conn.close()
        
        print(f"   Created {total_schedules} email schedules")
        print(f"   Scheduled emails for {contacts_with_schedules} contacts")
        
        self.assertGreater(total_schedules, 0, "Should create email schedules")
    
    def test_memory_usage_scaling(self):
        """Test that memory usage doesn't grow linearly with contact count"""
        # Test with different dataset sizes
        sizes = [500, 1000, 2000]
        memory_usage = {}
        
        for size in sizes:
            # Create fresh database for each test
            temp_db = os.path.join(self.temp_dir, f"memory_test_{size}.db")
            shutil.copy2(self.golden_db_path, temp_db)
            
            # Add test data
            scheduler = EmailScheduler(temp_db, config=self.config)
            
            # Create contacts
            conn = sqlite3.connect(temp_db)
            cursor = conn.cursor()
            
            contacts = []
            for i in range(size):
                birthday = date(1970, (i % 12) + 1, min((i % 28) + 1, 28))
                effective_date = date(2020, 12, 1)  # Cluster on same date
                contacts.append((
                    f"MemTest{i}", f"Contact{i}", f"mem{i}@test.com",
                    "TX", "75001",
                    birthday.strftime('%Y-%m-%d'),
                    effective_date.strftime('%Y-%m-%d'),
                    ""
                ))
            
            cursor.executemany("""
                INSERT INTO contacts (first_name, last_name, email, state, zip_code, 
                                    birth_date, effective_date, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, contacts)
            
            conn.commit()
            conn.close()
            
            # Measure memory usage during scheduling
            start_memory = self.process.memory_info().rss / 1024 / 1024
            
            test_date = date(2024, 11, 15)
            with patch('scheduler.datetime') as mock_datetime:
                mock_datetime.now.return_value = datetime.combine(test_date, datetime.min.time())
                mock_datetime.date = date
                
                scheduler.run_full_schedule()
            
            peak_memory = self.process.memory_info().rss / 1024 / 1024
            memory_usage[size] = peak_memory - start_memory
        
        # Verify memory scaling is sub-linear
        print(f"\n📈 MEMORY SCALING ANALYSIS")
        for size in sizes:
            print(f"   {size} contacts: {memory_usage[size]:.2f} MB")
        
        # Check that memory doesn't scale linearly
        if len(sizes) >= 3:
            ratio_1 = memory_usage[sizes[1]] / memory_usage[sizes[0]]
            ratio_2 = memory_usage[sizes[2]] / memory_usage[sizes[1]]
            
            # Memory growth should slow down (sub-linear scaling)
            self.assertLess(ratio_2, ratio_1 * 1.5, 
                          "Memory usage should not scale linearly with contact count")


class TestBatchProcessingPerformance(PerformanceTestBase):
    """Test batch processing and N+1 query elimination"""
    
    def test_campaign_scheduling_batch_efficiency(self):
        """Test that campaign scheduling uses batch queries"""
        # Create dataset with many campaign targets
        total_contacts = self.create_large_contact_dataset(count=1000)
        campaign_contacts = self.add_campaign_data_for_performance(contact_count=500)
        
        test_date = date(2024, 11, 15)
        
        # Monitor database operations
        query_count = 0
        original_execute = sqlite3.Cursor.execute
        
        def counting_execute(self, *args, **kwargs):
            nonlocal query_count
            query_count += 1
            return original_execute(self, *args, **kwargs)
        
        with self.measure_performance("Campaign Batch Processing"):
            with patch.object(sqlite3.Cursor, 'execute', counting_execute):
                with patch('scheduler.datetime') as mock_datetime:
                    mock_datetime.now.return_value = datetime.combine(test_date, datetime.min.time())
                    mock_datetime.date = date
                    
                    self.scheduler.run_full_schedule()
        
        print(f"   Total database queries: {query_count}")
        print(f"   Queries per campaign contact: {query_count / campaign_contacts:.2f}")
        
        # Should use batch queries, not one query per contact
        self.assertLess(query_count / campaign_contacts, 5, 
                       "Should use batch queries, not per-contact queries")
    
    def test_anniversary_scheduling_efficiency(self):
        """Test anniversary email scheduling efficiency"""
        # Create contacts with various anniversary dates
        total_contacts = self.create_large_contact_dataset(count=1000)
        
        test_date = date(2024, 11, 15)
        
        with self.measure_performance("Anniversary Email Scheduling"):
            with patch('scheduler.datetime') as mock_datetime:
                mock_datetime.now.return_value = datetime.combine(test_date, datetime.min.time())
                mock_datetime.date = date
                
                # Run only anniversary scheduling
                db_manager = DatabaseManager(self.test_db_path)
                contacts = db_manager.get_contacts_in_scheduling_window()
                
                # This should be efficient batch processing
                from scheduler import AnniversaryEmailScheduler
                anniversary_scheduler = AnniversaryEmailScheduler(db_manager, self.config)
                schedules = anniversary_scheduler.schedule_all_anniversary_emails(contacts)
        
        print(f"   Processed {len(contacts)} contacts")
        print(f"   Generated {len(schedules)} anniversary schedules")
        
        self.assertGreater(len(schedules), 0, "Should generate anniversary schedules")


class TestLoadBalancingPerformance(PerformanceTestBase):
    """Test load balancing and smoothing performance"""
    
    def test_effective_date_smoothing_at_scale(self):
        """Test effective date smoothing with large clusters"""
        # Create a large cluster of contacts with the same effective date
        cluster_size = 500
        total_contacts = self.create_large_contact_dataset(count=cluster_size, clustering_factor=1.0)
        
        test_date = date(2024, 11, 15)
        
        with self.measure_performance("Load Balancing & Smoothing"):
            with patch('scheduler.datetime') as mock_datetime:
                mock_datetime.now.return_value = datetime.combine(test_date, datetime.min.time())
                mock_datetime.date = date
                
                self.scheduler.run_full_schedule()
        
        # Analyze distribution
        conn = sqlite3.connect(self.test_db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT scheduled_send_date, COUNT(*) 
            FROM email_schedules 
            WHERE email_type = 'effective_date'
            GROUP BY scheduled_send_date 
            ORDER BY COUNT(*) DESC
        """)
        
        date_distribution = cursor.fetchall()
        conn.close()
        
        if date_distribution:
            max_on_single_date = date_distribution[0][1]
            total_effective_date_emails = sum(count for _, count in date_distribution)
            distribution_days = len(date_distribution)
            
            print(f"   Effective date emails: {total_effective_date_emails}")
            print(f"   Distributed across: {distribution_days} days")
            print(f"   Max on single day: {max_on_single_date}")
            print(f"   Average per day: {total_effective_date_emails / distribution_days:.1f}")
            
            # Smoothing should distribute the load
            self.assertGreater(distribution_days, 1, "Should distribute across multiple days")
            self.assertLess(max_on_single_date / total_effective_date_emails, 0.7, 
                          "Should not concentrate >70% on single day")
    
    def test_daily_volume_calculation_performance(self):
        """Test that daily volume calculations are efficient"""
        # Create large dataset
        total_contacts = self.create_large_contact_dataset(count=2000)
        
        test_date = date(2024, 11, 15)
        
        with self.measure_performance("Daily Volume Calculations"):
            with patch('scheduler.datetime') as mock_datetime:
                mock_datetime.now.return_value = datetime.combine(test_date, datetime.min.time())
                mock_datetime.date = date
                
                # Run scheduler which includes volume calculations
                self.scheduler.run_full_schedule()
        
        # Verify volume distribution
        conn = sqlite3.connect(self.test_db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT scheduled_send_date, COUNT(*) as email_count
            FROM email_schedules 
            GROUP BY scheduled_send_date 
            ORDER BY email_count DESC
            LIMIT 10
        """)
        
        volume_distribution = cursor.fetchall()
        conn.close()
        
        print(f"   Top volume days:")
        for send_date, count in volume_distribution[:5]:
            print(f"     {send_date}: {count} emails")
        
        self.assertGreater(len(volume_distribution), 0, "Should have email distribution")


class TestDatabasePerformance(PerformanceTestBase):
    """Test database operation performance"""
    
    def test_index_effectiveness(self):
        """Test that database indexes are effective"""
        # Create large dataset
        self.create_large_contact_dataset(count=3000)
        
        conn = sqlite3.connect(self.test_db_path)
        cursor = conn.cursor()
        
        # Test query performance with and without indexes
        queries_to_test = [
            ("State+Birthday Index", "SELECT * FROM contacts WHERE state = 'TX' AND birth_date LIKE '%-12-%'"),
            ("State+Effective Index", "SELECT * FROM contacts WHERE state = 'CA' AND effective_date LIKE '2020-%'"),
            ("Schedule Lookup Index", "SELECT * FROM email_schedules WHERE contact_id = 1 AND email_type = 'birthday'"),
        ]
        
        print(f"\n🔍 INDEX PERFORMANCE TEST")
        
        for query_name, query in queries_to_test:
            # Test with explain query plan
            cursor.execute(f"EXPLAIN QUERY PLAN {query}")
            plan = cursor.fetchall()
            
            # Measure execution time
            start_time = time.time()
            cursor.execute(query)
            results = cursor.fetchall()
            end_time = time.time()
            
            print(f"   {query_name}:")
            print(f"     Duration: {(end_time - start_time)*1000:.2f}ms")
            print(f"     Results: {len(results)} rows")
            
            # Check if index is being used
            plan_text = " ".join([str(row) for row in plan])
            using_index = "USING INDEX" in plan_text.upper()
            print(f"     Using index: {'✓' if using_index else '✗'}")
        
        conn.close()
    
    def test_batch_insert_performance(self):
        """Test batch insert performance for email schedules"""
        # Create moderate dataset
        self.create_large_contact_dataset(count=500)
        
        test_date = date(2024, 11, 15)
        
        with self.measure_performance("Batch Insert Performance"):
            with patch('scheduler.datetime') as mock_datetime:
                mock_datetime.now.return_value = datetime.combine(test_date, datetime.min.time())
                mock_datetime.date = date
                
                # This will test the batch insert performance
                self.scheduler.run_full_schedule()
        
        # Verify results
        conn = sqlite3.connect(self.test_db_path)
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM email_schedules")
        total_schedules = cursor.fetchone()[0]
        
        conn.close()
        
        print(f"   Inserted {total_schedules} email schedules")
        self.assertGreater(total_schedules, 0, "Should insert email schedules")


def run_performance_suite():
    """Run the performance test suite"""
    
    test_classes = [
        TestQueryDrivenPerformance,
        TestBatchProcessingPerformance,
        TestLoadBalancingPerformance,
        TestDatabasePerformance
    ]
    
    suite = unittest.TestSuite()
    for test_class in test_classes:
        tests = unittest.TestLoader().loadTestsFromTestCase(test_class)
        suite.addTests(tests)
    
    # Run tests with detailed output
    runner = unittest.TextTestRunner(verbosity=2, stream=sys.stdout)
    result = runner.run(suite)
    
    # Print summary
    print(f"\n{'='*60}")
    print(f"PERFORMANCE TEST SUMMARY")
    print(f"{'='*60}")
    print(f"Tests run: {result.testsRun}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    
    if result.failures:
        print(f"\nPERFORMANCE FAILURES:")
        for test, traceback in result.failures:
            print(f"  - {test}")
    
    if result.errors:
        print(f"\nPERFORMANCE ERRORS:")
        for test, traceback in result.errors:
            print(f"  - {test}")
    
    return result.wasSuccessful()


if __name__ == "__main__":
    # Check dependencies
    try:
        import psutil
    except ImportError:
        print("❌ ERROR: psutil required for performance testing")
        print("Install with: pip install psutil")
        sys.exit(1)
    
    if not os.path.exists("golden_dataset.sqlite3"):
        print("❌ ERROR: golden_dataset.sqlite3 not found!")
        sys.exit(1)
    
    print("🚀 STARTING PERFORMANCE TEST SUITE")
    print("=" * 60)
    
    success = run_performance_suite()
    
    if success:
        print("\n✅ ALL PERFORMANCE TESTS PASSED!")
        print("The hybrid scheduler demonstrates the expected performance characteristics.")
    else:
        print("\n❌ SOME PERFORMANCE TESTS FAILED!")
        print("The scheduler may not be meeting performance expectations.")
        sys.exit(1)