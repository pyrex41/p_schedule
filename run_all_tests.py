#!/usr/bin/env python3
"""
Comprehensive Test Runner for Core Scheduling Engine

This script runs the complete test suite including:
- Functional correctness tests (test_scheduler.py)
- Performance and scalability tests (test_performance_comprehensive.py)
- Original performance tests (test_performance.py)

Usage:
    python run_all_tests.py [--functional-only] [--performance-only] [--quick]
"""

import sys
import os
import subprocess
import time
import argparse
from pathlib import Path


def check_dependencies():
    """Check that all required dependencies are available"""
    dependencies = [
        ('sqlite3', 'sqlite3'),
        ('psutil', 'psutil'),
        ('yaml', 'PyYAML'),
    ]
    
    missing = []
    
    for module, package in dependencies:
        try:
            __import__(module)
        except ImportError:
            missing.append(package)
    
    if missing:
        print("❌ MISSING DEPENDENCIES:")
        for package in missing:
            print(f"   - {package}")
        print("\nInstall with: pip install " + " ".join(missing))
        return False
    
    return True


def check_files():
    """Check that all required files exist"""
    required_files = [
        'golden_dataset.sqlite3',
        'scheduler.py',
        'test_scheduler.py',
        'test_performance_comprehensive.py',
        'test_performance.py',
        'scheduler_config.yaml'
    ]
    
    missing = []
    
    for file in required_files:
        if not os.path.exists(file):
            missing.append(file)
    
    if missing:
        print("❌ MISSING FILES:")
        for file in missing:
            print(f"   - {file}")
        return False
    
    return True


def run_test_script(script_name, description):
    """Run a test script and return success status"""
    print(f"\n{'='*80}")
    print(f"🧪 RUNNING {description}")
    print(f"{'='*80}")
    
    start_time = time.time()
    
    try:
        result = subprocess.run([
            sys.executable, script_name
        ], capture_output=False, text=True, timeout=600)  # 10 minute timeout
        
        end_time = time.time()
        duration = end_time - start_time
        
        success = result.returncode == 0
        
        print(f"\n📊 {description} Results:")
        print(f"   Duration: {duration:.2f} seconds")
        print(f"   Status: {'✅ PASSED' if success else '❌ FAILED'}")
        print(f"   Exit code: {result.returncode}")
        
        return success
        
    except subprocess.TimeoutExpired:
        print(f"\n⏰ TIMEOUT: {description} took longer than 10 minutes")
        return False
    except Exception as e:
        print(f"\n💥 ERROR running {description}: {e}")
        return False


def run_functional_tests():
    """Run the functional correctness test suite"""
    return run_test_script('test_scheduler.py', 'FUNCTIONAL CORRECTNESS TESTS')


def run_performance_tests():
    """Run the performance test suite"""
    return run_test_script('test_performance_comprehensive.py', 'PERFORMANCE & SCALABILITY TESTS')


def run_original_performance_tests():
    """Run the original performance tests"""
    return run_test_script('test_performance.py', 'ORIGINAL PERFORMANCE TESTS')


def generate_test_report(results):
    """Generate a comprehensive test report"""
    print(f"\n{'='*80}")
    print(f"📋 COMPREHENSIVE TEST REPORT")
    print(f"{'='*80}")
    
    total_tests = len(results)
    passed_tests = sum(1 for _, success in results if success)
    failed_tests = total_tests - passed_tests
    
    print(f"Overall Status: {'✅ ALL PASSED' if failed_tests == 0 else '❌ SOME FAILED'}")
    print(f"Total Test Suites: {total_tests}")
    print(f"Passed: {passed_tests}")
    print(f"Failed: {failed_tests}")
    print(f"Success Rate: {(passed_tests/total_tests*100):.1f}%")
    
    print(f"\nDetailed Results:")
    for test_name, success in results:
        status = "✅ PASSED" if success else "❌ FAILED"
        print(f"   {test_name}: {status}")
    
    if failed_tests == 0:
        print(f"\n🎉 CONGRATULATIONS!")
        print(f"All tests passed! The Core Scheduling Engine is working correctly and")
        print(f"demonstrates the expected performance characteristics.")
        print(f"\nThe hybrid implementation successfully combines:")
        print(f"   • Verifiable correctness (declarative business logic)")
        print(f"   • High performance (query-driven, batch-oriented)")
        print(f"   • Scalability (handles large datasets efficiently)")
        print(f"   • Reliability (robust error handling and edge cases)")
    else:
        print(f"\n⚠️  ACTION REQUIRED")
        print(f"Some tests failed. Please review the detailed output above and:")
        print(f"   • Fix any implementation issues")
        print(f"   • Verify business logic compliance")
        print(f"   • Check performance bottlenecks")
        print(f"   • Re-run tests after fixes")
    
    return failed_tests == 0


def main():
    """Main test runner function"""
    parser = argparse.ArgumentParser(description='Run Core Scheduling Engine test suite')
    parser.add_argument('--functional-only', action='store_true', 
                       help='Run only functional correctness tests')
    parser.add_argument('--performance-only', action='store_true', 
                       help='Run only performance tests')
    parser.add_argument('--quick', action='store_true', 
                       help='Run quick subset of tests (functional + original performance)')
    parser.add_argument('--no-original-perf', action='store_true',
                       help='Skip original performance tests')
    
    args = parser.parse_args()
    
    print("🚀 CORE SCHEDULING ENGINE - COMPREHENSIVE TEST SUITE")
    print("=" * 80)
    print("This test suite validates the hybrid scheduler implementation")
    print("for correctness, performance, and scalability.")
    
    # Check prerequisites
    print(f"\n🔍 CHECKING PREREQUISITES...")
    
    if not check_dependencies():
        sys.exit(1)
    
    if not check_files():
        sys.exit(1)
    
    print("✅ All prerequisites satisfied")
    
    # Determine which tests to run
    test_results = []
    
    if args.functional_only:
        # Run only functional tests
        success = run_functional_tests()
        test_results.append(("Functional Correctness Tests", success))
        
    elif args.performance_only:
        # Run only performance tests
        success = run_performance_tests()
        test_results.append(("Performance & Scalability Tests", success))
        
    elif args.quick:
        # Run quick subset
        functional_success = run_functional_tests()
        test_results.append(("Functional Correctness Tests", functional_success))
        
        if not args.no_original_perf:
            original_perf_success = run_original_performance_tests()
            test_results.append(("Original Performance Tests", original_perf_success))
        
    else:
        # Run full test suite
        functional_success = run_functional_tests()
        test_results.append(("Functional Correctness Tests", functional_success))
        
        performance_success = run_performance_tests()
        test_results.append(("Performance & Scalability Tests", performance_success))
        
        if not args.no_original_perf:
            original_perf_success = run_original_performance_tests()
            test_results.append(("Original Performance Tests", original_perf_success))
    
    # Generate final report
    overall_success = generate_test_report(test_results)
    
    # Exit with appropriate code
    sys.exit(0 if overall_success else 1)


if __name__ == "__main__":
    main()