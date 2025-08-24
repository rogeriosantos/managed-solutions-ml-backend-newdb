#!/usr/bin/env python3
"""
Test script to verify CIMCO data sync results
"""

import requests
import json
from datetime import datetime

BASE_URL = "http://localhost:8000/api/v1/data"

def test_table_counts():
    """Test 1: Check table record counts"""
    print("🔍 Test 1: Checking table counts...")
    try:
        response = requests.get(f"{BASE_URL}/postgres/tables/counts")
        response.raise_for_status()
        data = response.json()
        
        if data.get("status") == "success":
            counts = data.get("table_counts", {})
            print(f"  ✅ Job Records: {counts.get('job_records', 'N/A'):,}")
            print(f"  ✅ Machines: {counts.get('machines', 'N/A'):,}")
            print(f"  ✅ Operators: {counts.get('operators', 'N/A'):,}")
            print(f"  ✅ Sync Logs: {counts.get('sync_logs', 'N/A'):,}")
            return counts.get('job_records', 0)
        else:
            print(f"  ❌ Error: {data}")
            return 0
    except Exception as e:
        print(f"  ❌ Error: {e}")
        return 0

def test_sync_history():
    """Test 2: Check recent sync history"""
    print("\n🔍 Test 2: Checking sync history...")
    try:
        response = requests.get(f"{BASE_URL}/sync-status")
        response.raise_for_status()
        data = response.json()
        
        if data.get("status") == "success":
            syncs = data.get("recent_syncs", [])
            print(f"  ✅ Found {len(syncs)} recent syncs")
            
            # Show summary of recent syncs
            total_processed = sum(sync.get("records_processed", 0) for sync in syncs)
            total_inserted = sum(sync.get("records_inserted", 0) for sync in syncs)
            successful_syncs = sum(1 for sync in syncs if sync.get("status") == "completed")
            
            print(f"  ✅ Successful syncs: {successful_syncs}/{len(syncs)}")
            print(f"  ✅ Total records processed: {total_processed:,}")
            print(f"  ✅ Total records inserted: {total_inserted:,}")
            
            # Show last few syncs
            print("  📋 Last 3 syncs:")
            for sync in syncs[:3]:
                duration = sync.get("duration", 0)
                print(f"    - {sync.get('sync_type', 'N/A')} sync: {sync.get('records_processed', 0)} processed, "
                      f"{sync.get('records_inserted', 0)} inserted ({duration:.1f}s)")
        else:
            print(f"  ❌ Error: {data}")
    except Exception as e:
        print(f"  ❌ Error: {e}")

def test_sample_data():
    """Test 3: Get sample job data to verify content"""
    print("\n🔍 Test 3: Checking sample job data...")
    try:
        # This endpoint might not exist yet, so we'll create a simple query
        response = requests.get(f"{BASE_URL}/joblog?limit=5")
        response.raise_for_status()
        data = response.json()
        
        if data.get("status") == "success":
            jobs = data.get("jobs", [])
            print(f"  ✅ Retrieved {len(jobs)} sample jobs")
            
            if jobs:
                job = jobs[0]
                print("  📋 Sample job data:")
                print(f"    - Job Number: {job.get('JobNumber', 'N/A')}")
                print(f"    - Machine: {job.get('MachineID', 'N/A')}")
                print(f"    - Part Number: {job.get('PartNumber', 'N/A')}")
                print(f"    - State: {job.get('State', 'N/A')}")
                print(f"    - Start Time: {job.get('StartTime', 'N/A')}")
                print(f"    - Parts Produced: {job.get('PartsProduced', 'N/A')}")
        else:
            print(f"  ❌ Error: {data}")
    except Exception as e:
        print(f"  ⚠️  Could not test sample data (endpoint may not exist): {e}")

def test_date_range():
    """Test 4: Check data distribution by date"""
    print("\n🔍 Test 4: Checking data date range...")
    try:
        # Try to get joblog summary which might show date ranges
        response = requests.get(f"{BASE_URL}/joblog/summary")
        response.raise_for_status()
        data = response.json()
        
        if data.get("status") == "success":
            summary = data.get("summary", {})
            print("  ✅ Data summary:")
            for key, value in summary.items():
                print(f"    - {key}: {value}")
        else:
            print(f"  ❌ Error: {data}")
    except Exception as e:
        print(f"  ⚠️  Could not get date range summary: {e}")

def test_machine_list():
    """Test 5: Check machine data"""
    print("\n🔍 Test 5: Checking machine data...")
    try:
        response = requests.get(f"{BASE_URL}/machines")
        response.raise_for_status()
        data = response.json()
        
        if data.get("status") == "success":
            machines = data.get("machines", [])
            print(f"  ✅ Found {len(machines)} machines")
            
            if machines:
                print("  📋 Sample machines:")
                for machine in machines[:5]:  # Show first 5
                    print(f"    - {machine}")
        else:
            print(f"  ❌ Error: {data}")
    except Exception as e:
        print(f"  ⚠️  Could not get machine list: {e}")

def main():
    print("🧪 CIMCO Data Sync Verification Tests")
    print("=" * 50)
    
    # Run all tests
    job_count = test_table_counts()
    test_sync_history()
    test_sample_data()
    test_date_range()
    test_machine_list()
    
    # Final assessment
    print("\n" + "=" * 50)
    print("📊 FINAL ASSESSMENT")
    
    if job_count > 0:
        print(f"✅ SUCCESS: Found {job_count:,} job records in the database!")
        print("✅ Your CIMCO data has been successfully synced to the analytics database.")
        
        if job_count >= 90000:  # Assuming you expected around 100k
            print("✅ Record count looks complete (90k+ records)")
        elif job_count >= 50000:
            print("⚠️  Record count is substantial but may be incomplete")
        else:
            print("⚠️  Record count seems low - you may want to run more sync batches")
            
    else:
        print("❌ ISSUE: No job records found in the database")
        print("❌ The sync may not have completed successfully")
    
    print("\n🎉 Testing completed!")

if __name__ == "__main__":
    main()