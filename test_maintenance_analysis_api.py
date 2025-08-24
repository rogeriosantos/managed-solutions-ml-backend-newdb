#!/usr/bin/env python3
"""
Test script for the new predictive maintenance analysis API endpoint
"""
import requests
import json
from datetime import datetime

# API base URL
BASE_URL = "http://localhost:8000/api/v1"

def test_comprehensive_analysis():
    """Test the comprehensive maintenance analysis endpoint"""
    print("🔧 Testing Comprehensive Maintenance Analysis API")
    print("=" * 60)
    
    try:
        # Test the new comprehensive analysis endpoint
        response = requests.get(f"{BASE_URL}/maintenance/maintenance/analysis?limit=3000")
        
        if response.status_code == 200:
            data = response.json()
            print("✅ API Response successful!")
            print(f"Status: {data['status']}")
            print(f"Message: {data['message']}")
            
            results = data['results']
            
            # Display data summary
            print("\n📊 Data Summary:")
            summary = results['data_summary']
            print(f"  Total Jobs Analyzed: {summary['total_jobs_analyzed']:,}")
            print(f"  Jobs with Maintenance: {summary['jobs_with_maintenance']:,}")
            print(f"  Maintenance Rate: {summary['maintenance_rate_percent']}%")
            print(f"  Total Maintenance Time: {summary['total_maintenance_time_seconds']:,} seconds")
            
            # Display top maintenance machines
            print("\n🚨 Top High-Maintenance Machines:")
            patterns = results['maintenance_patterns']
            for i, machine in enumerate(patterns['top_high_maintenance_machines'][:5], 1):
                print(f"  {i}. {machine['machine_id']}:")
                print(f"     Total Maintenance: {machine['total_maintenance_time']:,}s")
                print(f"     Frequency: {machine['maintenance_frequency']}%")
                print(f"     Efficiency: {machine['average_efficiency']:.1f}%")
            
            # Display model performance
            print("\n🤖 Model Performance:")
            model = results['model_performance']['model_performance']
            print(f"  Mean Absolute Error: {model['mean_absolute_error']:.2f} seconds")
            print(f"  R² Score: {model['r2_score']:.3f}")
            print(f"  Training Samples: {model['training_samples']:,}")
            
            # Display feature importance
            print("\n📈 Top Feature Importance:")
            features = results['model_performance']['feature_importance']
            for feature, importance in list(features.items())[:3]:
                print(f"  {feature}: {importance:.3f}")
            
            # Display anomaly detection
            print("\n🔍 Anomaly Detection:")
            anomalies = results['anomaly_detection']
            print(f"  Total Anomalies: {anomalies['total_anomalies']:,}")
            print(f"  Anomaly Rate: {anomalies['anomaly_rate_percent']:.1f}%")
            
            # Display sample predictions
            print("\n🔮 Sample Predictions:")
            for pred in results['sample_predictions'][:3]:
                print(f"  {pred['machine_id']}:")
                print(f"    Predicted Maintenance: {pred['predicted_maintenance_time']:.0f}s")
                print(f"    Risk Level: {pred['risk_level']}")
                print(f"    Recent Efficiency: {pred['recent_efficiency']:.1f}%")
            
        else:
            print(f"❌ API Error: {response.status_code}")
            print(f"Response: {response.text}")
            
    except requests.exceptions.ConnectionError:
        print("❌ Connection Error: Make sure the FastAPI server is running on localhost:8000")
    except Exception as e:
        print(f"❌ Error: {e}")

def test_maintenance_prediction():
    """Test the maintenance prediction endpoint"""
    print("\n🔮 Testing Maintenance Prediction API")
    print("=" * 60)
    
    try:
        # Test prediction for specific machines
        payload = ["0007", "0020", "0017"]  # Use actual machine ID format
        response = requests.post(
            f"{BASE_URL}/maintenance/maintenance/predict",
            json=payload
        )
        
        if response.status_code == 200:
            data = response.json()
            print("✅ Prediction API Response successful!")
            
            for pred in data['predictions']:
                print(f"\n{pred['machine_id']}:")
                print(f"  Predicted Maintenance: {pred['predicted_maintenance_time']:.0f} seconds")
                print(f"  Risk Level: {pred['risk_level']}")
                print(f"  Recent Efficiency: {pred['recent_efficiency']:.1f}%")
                
        else:
            print(f"❌ Prediction API Error: {response.status_code}")
            print(f"Response: {response.text}")
            
    except Exception as e:
        print(f"❌ Prediction Error: {e}")

def test_machine_schedule():
    """Test the machine maintenance schedule endpoint"""
    print("\n📅 Testing Machine Maintenance Schedule API")
    print("=" * 60)
    
    try:
        machine_id = "0007"  # Use the actual machine ID format from the data
        response = requests.get(f"{BASE_URL}/maintenance/maintenance/machine/{machine_id}/schedule")
        
        if response.status_code == 200:
            data = response.json()
            print(f"✅ Schedule API Response successful for {machine_id}!")
            
            schedule = data['maintenance_schedule']
            print(f"\nMaintenance Schedule:")
            print(f"  Predicted Time: {schedule['predicted_maintenance_time_seconds']:.0f} seconds")
            print(f"  Recommended Date: {schedule['recommended_maintenance_date']}")
            print(f"  Priority: {schedule['priority']}")
            print(f"  Days Until: {schedule['days_until_maintenance']}")
            print(f"  Current Efficiency: {schedule['current_efficiency_percent']:.1f}%")
            
            history = schedule['maintenance_history']
            print(f"\nMaintenance History:")
            print(f"  Average: {history['mean_seconds']:.0f} seconds")
            print(f"  Maximum: {history['max_seconds']:.0f} seconds")
            print(f"  Frequency: {history['frequency_percent']:.1f}%")
                
        else:
            print(f"❌ Schedule API Error: {response.status_code}")
            print(f"Response: {response.text}")
            
    except Exception as e:
        print(f"❌ Schedule Error: {e}")

if __name__ == "__main__":
    print("🚀 Testing Predictive Maintenance API Endpoints")
    print("=" * 60)
    print(f"Timestamp: {datetime.now().isoformat()}")
    
    # Test all endpoints
    test_comprehensive_analysis()
    test_maintenance_prediction()
    test_machine_schedule()
    
    print("\n✅ Testing Complete!")