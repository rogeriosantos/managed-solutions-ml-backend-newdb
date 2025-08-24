#!/usr/bin/env python3
"""
Simple test script for the Predictive Maintenance API endpoints
"""

import requests
import json
from datetime import datetime

BASE_URL = "http://localhost:8000/api/v1/maintenance"

def test_maintenance_summary():
    """Test maintenance summary endpoint"""
    print("🔍 Testing Maintenance Summary...")
    try:
        response = requests.get(f"{BASE_URL}/summary")
        response.raise_for_status()
        data = response.json()
        
        if data.get("status") == "success":
            summary = data["summary"]
            print(f"✅ Maintenance Summary:")
            print(f"  Total Jobs: {summary['total_jobs']:,}")
            print(f"  Jobs with Maintenance: {summary['jobs_with_maintenance']:,}")
            print(f"  Maintenance Rate: {summary['maintenance_rate_percent']}%")
            print(f"  Total Maintenance Hours: {summary['total_maintenance_hours']}")
            print(f"  Average Efficiency: {summary['avg_efficiency_percent']}%")
            print(f"  Total Machines: {summary['total_machines']}")
            return True
        else:
            print(f"❌ Error: {data}")
            return False
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def test_maintenance_alerts():
    """Test maintenance alerts endpoint"""
    print("\n🚨 Testing Maintenance Alerts...")
    try:
        response = requests.get(f"{BASE_URL}/alerts")
        response.raise_for_status()
        data = response.json()
        
        if data.get("status") == "success":
            alerts = data["alerts"]
            print(f"✅ Found {len(alerts)} maintenance alerts")
            
            if alerts:
                print("🚨 Top Priority Alerts:")
                for alert in alerts[:5]:  # Show top 5
                    print(f"  {alert['machine_id']} - {alert['alert_level']}")
                    print(f"    Reasons: {', '.join(alert['reasons'])}")
                    print(f"    Efficiency: {alert['efficiency_percent']}%")
                    print(f"    Avg Maintenance: {alert['avg_maintenance_minutes']} min")
                    print()
            else:
                print("✅ No maintenance alerts - all machines operating normally!")
            return True
        else:
            print(f"❌ Error: {data}")
            return False
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def test_maintenance_by_machine():
    """Test maintenance by machine endpoint"""
    print("🔧 Testing Maintenance by Machine...")
    try:
        response = requests.get(f"{BASE_URL}/by-machine?limit=10")
        response.raise_for_status()
        data = response.json()
        
        if data.get("status") == "success":
            machines = data["machines"]
            print(f"✅ Found maintenance data for {len(machines)} machines")
            
            if machines:
                print("🏭 Top Maintenance-Heavy Machines:")
                for machine in machines[:5]:  # Show top 5
                    print(f"  {machine['machine_id']}:")
                    print(f"    Total Jobs: {machine['total_jobs']}")
                    print(f"    Maintenance Rate: {machine['maintenance_rate_percent']}%")
                    print(f"    Total Maintenance: {machine['total_maintenance_hours']} hours")
                    print(f"    Efficiency: {machine['efficiency_percent']}%")
                    print()
            return True
        else:
            print(f"❌ Error: {data}")
            return False
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def test_machine_detail():
    """Test specific machine detail endpoint"""
    print("🔍 Testing Machine Detail...")
    try:
        # First get a machine ID from the by-machine endpoint
        response = requests.get(f"{BASE_URL}/by-machine?limit=1")
        response.raise_for_status()
        data = response.json()
        
        if data.get("status") == "success" and data["machines"]:
            machine_id = data["machines"][0]["machine_id"]
            
            # Now get detailed info for this machine
            detail_response = requests.get(f"{BASE_URL}/machine/{machine_id}")
            detail_response.raise_for_status()
            detail_data = detail_response.json()
            
            if detail_data.get("status") == "success":
                stats = detail_data["statistics"]
                events = detail_data["recent_maintenance_events"]
                
                print(f"✅ Machine Detail for {machine_id}:")
                print(f"  Total Jobs: {stats['total_jobs']}")
                print(f"  Maintenance Rate: {stats['maintenance_rate_percent']}%")
                print(f"  Average Efficiency: {stats['avg_efficiency_percent']}%")
                print(f"  Recent Maintenance Events: {len(events)}")
                
                if events:
                    print(f"  Latest Maintenance: {events[0]['date']}")
                    print(f"    Duration: {events[0]['maintenance_minutes']} minutes")
                
                return True
            else:
                print(f"❌ Detail Error: {detail_data}")
                return False
        else:
            print("❌ No machines found to test detail endpoint")
            return False
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def test_maintenance_trends():
    """Test maintenance trends endpoint"""
    print("\n📈 Testing Maintenance Trends...")
    try:
        response = requests.get(f"{BASE_URL}/trends?days=7")
        response.raise_for_status()
        data = response.json()
        
        if data.get("status") == "success":
            trends = data["trends"]
            print(f"✅ Found {len(trends)} days of trend data")
            
            if trends:
                print("📊 Recent Trends (last 3 days):")
                for trend in trends[:3]:
                    print(f"  {trend['date']}:")
                    print(f"    Jobs: {trend['jobs_count']}")
                    print(f"    Maintenance Rate: {trend['maintenance_rate_percent']}%")
                    print(f"    Efficiency: {trend['efficiency_percent']}%")
                    print()
            return True
        else:
            print(f"❌ Error: {data}")
            return False
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def main():
    print("🧪 PREDICTIVE MAINTENANCE API TESTS")
    print("=" * 50)
    
    tests = [
        test_maintenance_summary,
        test_maintenance_alerts,
        test_maintenance_by_machine,
        test_machine_detail,
        test_maintenance_trends
    ]
    
    passed = 0
    total = len(tests)
    
    for test in tests:
        if test():
            passed += 1
        print("-" * 30)
    
    print(f"\n📊 TEST RESULTS: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed! Your predictive maintenance API is working!")
        print("\n🚀 Next Steps:")
        print("1. Use these endpoints in a dashboard")
        print("2. Set up automated alerts")
        print("3. Create maintenance schedules based on predictions")
    else:
        print("⚠️  Some tests failed. Check the API endpoints.")

if __name__ == "__main__":
    main()