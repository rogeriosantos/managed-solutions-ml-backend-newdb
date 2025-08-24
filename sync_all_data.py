#!/usr/bin/env python3
"""
Script to sync all CIMCO data to analytics database
Syncs 100k+ records in batches of 5000 with progress tracking
"""

import requests
import time
import json
from datetime import datetime, timedelta
import sys

# Configuration
BASE_URL = "http://localhost:8000/api/v1/data"
BATCH_SIZE = 5000
MAX_BATCHES = 30  # Safety limit (150k records max)
DELAY_BETWEEN_BATCHES = 2  # seconds

def make_sync_request(batch_num):
    """Make a sync request and return the results"""
    url = f"{BASE_URL}/sync/jobs"
    params = {
        "limit": BATCH_SIZE,
        "incremental": "false"
    }
    
    try:
        print(f"  Making request to: {url}")
        response = requests.post(url, params=params, timeout=300)  # 5 minute timeout
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"  ❌ API Error: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"  ❌ JSON Error: {e}")
        return None

def get_table_counts():
    """Get current table counts"""
    try:
        response = requests.get(f"{BASE_URL}/postgres/tables/counts", timeout=30)
        response.raise_for_status()
        return response.json()
    except:
        return None

def format_duration(seconds):
    """Format duration in human readable format"""
    return str(timedelta(seconds=int(seconds)))

def main():
    print("🚀 Starting CIMCO Data Sync")
    print("=" * 50)
    print(f"Batch size: {BATCH_SIZE}")
    print(f"Max batches: {MAX_BATCHES}")
    print(f"Delay between batches: {DELAY_BETWEEN_BATCHES}s")
    print()
    
    # Initialize counters
    batch_count = 0
    total_processed = 0
    total_inserted = 0
    total_updated = 0
    total_failed = 0
    start_time = time.time()
    
    print("Starting sync batches...")
    print("Press Ctrl+C to stop at any time")
    print()
    
    try:
        while batch_count < MAX_BATCHES:
            batch_count += 1
            print(f"📦 Processing batch {batch_count}...")
            
            # Make sync request
            result = make_sync_request(batch_count)
            
            if not result:
                print("  ❌ Batch failed, stopping sync")
                break
                
            if result.get("status") != "success":
                print(f"  ❌ Sync failed: {result.get('error', 'Unknown error')}")
                break
            
            # Extract metrics
            processed = result.get("jobs_processed", 0)
            inserted = result.get("jobs_inserted", 0)
            updated = result.get("jobs_updated", 0)
            failed = result.get("jobs_failed", 0)
            sync_id = result.get("sync_id", "N/A")
            
            # Update totals
            total_processed += processed
            total_inserted += inserted
            total_updated += updated
            total_failed += failed
            
            # Print batch results
            print(f"  ✅ Processed: {processed}")
            print(f"  ✅ Inserted: {inserted}")
            print(f"  ✅ Updated: {updated}")
            print(f"  {'❌' if failed > 0 else '✅'} Failed: {failed}")
            print(f"  🆔 Sync ID: {sync_id}")
            
            # Check if we're done (fewer records than batch size)
            if processed < BATCH_SIZE:
                print()
                print(f"🏁 Reached end of data (processed {processed} < {BATCH_SIZE})")
                break
            
            # Progress update every 5 batches
            if batch_count % 5 == 0:
                elapsed = time.time() - start_time
                records_per_second = total_processed / elapsed if elapsed > 0 else 0
                
                print()
                print("📊 Progress Update")
                print("-" * 30)
                print(f"Batches completed: {batch_count}")
                print(f"Total processed: {total_processed:,}")
                print(f"Total inserted: {total_inserted:,}")
                print(f"Total updated: {total_updated:,}")
                print(f"Total failed: {total_failed:,}")
                print(f"Records/second: {records_per_second:.2f}")
                print(f"Elapsed time: {format_duration(elapsed)}")
                print()
            
            # Delay between batches
            if batch_count < MAX_BATCHES:
                print(f"  ⏳ Waiting {DELAY_BETWEEN_BATCHES}s before next batch...")
                time.sleep(DELAY_BETWEEN_BATCHES)
            
            print()
    
    except KeyboardInterrupt:
        print()
        print("🛑 Sync interrupted by user")
    
    # Final summary
    end_time = time.time()
    total_time = end_time - start_time
    
    print()
    print("🎉 SYNC COMPLETE")
    print("=" * 50)
    print(f"Total batches: {batch_count}")
    print(f"Total processed: {total_processed:,}")
    print(f"Total inserted: {total_inserted:,}")
    print(f"Total updated: {total_updated:,}")
    print(f"Total failed: {total_failed:,}")
    print(f"Total time: {format_duration(total_time)}")
    if total_time > 0:
        print(f"Average records/second: {total_processed / total_time:.2f}")
    
    # Check final table counts
    print()
    print("📋 Checking final table counts...")
    counts = get_table_counts()
    if counts and counts.get("status") == "success":
        job_records = counts.get("table_counts", {}).get("job_records", "Unknown")
        print(f"Job records in database: {job_records:,}" if isinstance(job_records, int) else f"Job records: {job_records}")
    else:
        print("Could not retrieve table counts")
    
    print()
    print("✨ Sync script completed!")

if __name__ == "__main__":
    main()