#!/usr/bin/env python3
"""
Predictive Maintenance Analysis for CIMCO Manufacturing Data
Uses machine learning to predict maintenance needs and equipment failures
"""

import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.ensemble import RandomForestRegressor, IsolationForest
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, r2_score
from sklearn.preprocessing import StandardScaler
import warnings
warnings.filterwarnings('ignore')

class PredictiveMaintenanceAnalyzer:
    def __init__(self, base_url="http://localhost:8000/api/v1/data"):
        self.base_url = base_url
        self.data = None
        self.models = {}
        self.scalers = {}
        
    def fetch_data(self, limit=5000):
        """Fetch job data from the API in batches"""
        print("📊 Fetching manufacturing data...")
        
        # API limit is 1000, so we need to fetch in batches
        max_batch_size = 1000
        all_data = []
        
        try:
            batches_needed = min((limit + max_batch_size - 1) // max_batch_size, 10)  # Max 10 batches
            
            for batch in range(batches_needed):
                batch_limit = min(max_batch_size, limit - len(all_data))
                if batch_limit <= 0:
                    break
                
                print(f"  Fetching batch {batch + 1}/{batches_needed} ({batch_limit} records)...")
                
                response = requests.get(f"{self.base_url}/joblog", params={"limit": batch_limit})
                response.raise_for_status()
                data = response.json()
                
                if data.get("status") == "success":
                    batch_data = data["data"]
                    all_data.extend(batch_data)
                    
                    # If we got fewer records than requested, we've reached the end
                    if len(batch_data) < batch_limit:
                        break
                else:
                    print(f"❌ API Error: {data}")
                    break
            
            if all_data:
                df = pd.DataFrame(all_data)
                print(f"✅ Loaded {len(df)} job records")
                return df
            else:
                print("❌ No data received")
                return None
                
        except Exception as e:
            print(f"❌ Error fetching data: {e}")
            return None
    
    def preprocess_data(self, df):
        """Clean and prepare data for analysis"""
        print("🔧 Preprocessing data...")
        
        # Convert timestamps
        df['StartTime'] = pd.to_datetime(df['StartTime'])
        df['EndTime'] = pd.to_datetime(df['EndTime'])
        
        # Handle invalid end times (1969 dates)
        df.loc[df['EndTime'].dt.year < 1970, 'EndTime'] = pd.NaT
        
        # Calculate actual job duration where possible
        df['ActualDuration'] = (df['EndTime'] - df['StartTime']).dt.total_seconds()
        
        # Create maintenance indicators
        df['HasMaintenance'] = (df['MaintenanceTime'] > 0).astype(int)
        df['HighMaintenance'] = (df['MaintenanceTime'] > df['MaintenanceTime'].quantile(0.8)).astype(int)
        
        # Calculate efficiency metrics
        df['Efficiency'] = np.where(df['JobDuration'] > 0, 
                                   df['RunningTime'] / df['JobDuration'], 0)
        df['SetupRatio'] = np.where(df['JobDuration'] > 0,
                                   df['SetupTime'] / df['JobDuration'], 0)
        df['IdleRatio'] = np.where(df['JobDuration'] > 0,
                                  df['IdleTime'] / df['JobDuration'], 0)
        
        # Create time-based features
        df['Hour'] = df['StartTime'].dt.hour
        df['DayOfWeek'] = df['StartTime'].dt.dayofweek
        df['Month'] = df['StartTime'].dt.month
        
        # Calculate rolling averages for trend analysis
        df = df.sort_values('StartTime')
        for machine in df['machine'].unique():
            mask = df['machine'] == machine
            df.loc[mask, 'MaintenanceTime_MA7'] = df.loc[mask, 'MaintenanceTime'].rolling(7, min_periods=1).mean()
            df.loc[mask, 'Efficiency_MA7'] = df.loc[mask, 'Efficiency'].rolling(7, min_periods=1).mean()
            df.loc[mask, 'SetupTime_MA7'] = df.loc[mask, 'SetupTime'].rolling(7, min_periods=1).mean()
        
        print(f"✅ Preprocessed {len(df)} records")
        return df
    
    def analyze_maintenance_patterns(self, df):
        """Analyze maintenance patterns by machine"""
        print("🔍 Analyzing maintenance patterns...")
        
        # Group by machine
        machine_stats = df.groupby('machine').agg({
            'MaintenanceTime': ['count', 'sum', 'mean', 'std'],
            'JobDuration': 'sum',
            'Efficiency': 'mean',
            'HasMaintenance': 'sum',
            'PartsProduced': 'sum'
        }).round(2)
        
        machine_stats.columns = ['_'.join(col).strip() for col in machine_stats.columns]
        machine_stats['MaintenanceFrequency'] = machine_stats['HasMaintenance_sum'] / machine_stats['MaintenanceTime_count']
        machine_stats['MaintenanceIntensity'] = machine_stats['MaintenanceTime_sum'] / machine_stats['JobDuration_sum']
        
        # Identify high-maintenance machines
        high_maintenance = machine_stats.nlargest(10, 'MaintenanceTime_sum')
        
        print("\n🚨 Top 10 High-Maintenance Machines:")
        print("=" * 60)
        for machine, row in high_maintenance.iterrows():
            print(f"{machine}:")
            print(f"  Total Maintenance Time: {row['MaintenanceTime_sum']:,.0f} seconds")
            print(f"  Maintenance Frequency: {row['MaintenanceFrequency']:.2%}")
            print(f"  Average Efficiency: {row['Efficiency_mean']:.2%}")
            print()
        
        return machine_stats
    
    def build_maintenance_prediction_model(self, df):
        """Build ML model to predict maintenance needs"""
        print("🤖 Building maintenance prediction model...")
        
        # Prepare features for modeling
        feature_cols = [
            'JobDuration', 'RunningTime', 'SetupTime', 'IdleTime',
            'PartsProduced', 'Efficiency', 'SetupRatio', 'IdleRatio',
            'Hour', 'DayOfWeek', 'Month',
            'MaintenanceTime_MA7', 'Efficiency_MA7', 'SetupTime_MA7'
        ]
        
        # Filter data with valid features
        model_data = df[feature_cols + ['MaintenanceTime', 'machine']].dropna()
        
        if len(model_data) < 100:
            print("❌ Insufficient data for modeling")
            return None
        
        X = model_data[feature_cols]
        y = model_data['MaintenanceTime']
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
        
        # Scale features
        scaler = StandardScaler()
        X_train_scaled = scaler.fit_transform(X_train)
        X_test_scaled = scaler.transform(X_test)
        
        # Train model
        model = RandomForestRegressor(n_estimators=100, random_state=42)
        model.fit(X_train_scaled, y_train)
        
        # Evaluate
        y_pred = model.predict(X_test_scaled)
        mae = mean_absolute_error(y_test, y_pred)
        r2 = r2_score(y_test, y_pred)
        
        print(f"✅ Model Performance:")
        print(f"  Mean Absolute Error: {mae:.2f} seconds")
        print(f"  R² Score: {r2:.3f}")
        
        # Feature importance
        feature_importance = pd.DataFrame({
            'feature': feature_cols,
            'importance': model.feature_importances_
        }).sort_values('importance', ascending=False)
        
        print(f"\n📊 Top 5 Most Important Features:")
        for _, row in feature_importance.head().iterrows():
            print(f"  {row['feature']}: {row['importance']:.3f}")
        
        self.models['maintenance_predictor'] = model
        self.scalers['maintenance_predictor'] = scaler
        
        return model, scaler, feature_importance
    
    def detect_anomalies(self, df):
        """Detect anomalous machine behavior that might indicate issues"""
        print("🚨 Detecting anomalies...")
        
        # Prepare features for anomaly detection
        anomaly_features = [
            'Efficiency', 'SetupRatio', 'IdleRatio', 'MaintenanceTime'
        ]
        
        anomaly_data = df[anomaly_features + ['machine', 'StartTime']].dropna()
        
        if len(anomaly_data) < 50:
            print("❌ Insufficient data for anomaly detection")
            return None
        
        # Fit isolation forest
        iso_forest = IsolationForest(contamination=0.1, random_state=42)
        anomaly_data['anomaly'] = iso_forest.fit_predict(anomaly_data[anomaly_features])
        
        # Get anomalies
        anomalies = anomaly_data[anomaly_data['anomaly'] == -1]
        
        print(f"🔍 Found {len(anomalies)} anomalous records ({len(anomalies)/len(anomaly_data):.1%})")
        
        # Group anomalies by machine
        anomaly_summary = anomalies.groupby('machine').size().sort_values(ascending=False)
        
        print(f"\n🚨 Machines with Most Anomalies:")
        for machine, count in anomaly_summary.head(10).items():
            print(f"  {machine}: {count} anomalies")
        
        return anomalies, anomaly_summary
    
    def predict_next_maintenance(self, machine_id, recent_jobs=10):
        """Predict when next maintenance will be needed for a specific machine"""
        if 'maintenance_predictor' not in self.models:
            print("❌ Maintenance prediction model not trained")
            return None
        
        # Get recent data for the machine
        machine_data = self.data[self.data['machine'] == machine_id].tail(recent_jobs)
        
        if len(machine_data) == 0:
            print(f"❌ No data found for machine {machine_id}")
            return None
        
        # Prepare features
        feature_cols = [
            'JobDuration', 'RunningTime', 'SetupTime', 'IdleTime',
            'PartsProduced', 'Efficiency', 'SetupRatio', 'IdleRatio',
            'Hour', 'DayOfWeek', 'Month',
            'MaintenanceTime_MA7', 'Efficiency_MA7', 'SetupTime_MA7'
        ]
        
        latest_features = machine_data[feature_cols].iloc[-1:].dropna()
        
        if len(latest_features) == 0:
            print(f"❌ Insufficient feature data for {machine_id}")
            return None
        
        # Make prediction
        model = self.models['maintenance_predictor']
        scaler = self.scalers['maintenance_predictor']
        
        features_scaled = scaler.transform(latest_features)
        predicted_maintenance = model.predict(features_scaled)[0]
        
        # Calculate risk level
        avg_maintenance = self.data['MaintenanceTime'].mean()
        risk_level = "LOW"
        if predicted_maintenance > avg_maintenance * 2:
            risk_level = "HIGH"
        elif predicted_maintenance > avg_maintenance:
            risk_level = "MEDIUM"
        
        return {
            'machine': machine_id,
            'predicted_maintenance_time': predicted_maintenance,
            'risk_level': risk_level,
            'average_maintenance': avg_maintenance,
            'recent_efficiency': machine_data['Efficiency'].mean()
        }
    
    def generate_maintenance_report(self):
        """Generate comprehensive maintenance report"""
        print("\n" + "="*60)
        print("🔧 PREDICTIVE MAINTENANCE REPORT")
        print("="*60)
        
        if self.data is None:
            print("❌ No data loaded")
            return
        
        # Overall statistics
        total_maintenance_time = self.data['MaintenanceTime'].sum()
        avg_maintenance_per_job = self.data['MaintenanceTime'].mean()
        maintenance_jobs = (self.data['MaintenanceTime'] > 0).sum()
        
        print(f"\n📊 Overall Statistics:")
        print(f"  Total Jobs Analyzed: {len(self.data):,}")
        print(f"  Jobs with Maintenance: {maintenance_jobs:,} ({maintenance_jobs/len(self.data):.1%})")
        print(f"  Total Maintenance Time: {total_maintenance_time:,.0f} seconds")
        print(f"  Average Maintenance per Job: {avg_maintenance_per_job:.1f} seconds")
        
        # Machine recommendations
        machine_stats = self.data.groupby('machine').agg({
            'MaintenanceTime': ['sum', 'mean', 'count'],
            'Efficiency': 'mean'
        })
        
        machine_stats.columns = ['_'.join(col) for col in machine_stats.columns]
        machine_stats['maintenance_frequency'] = (
            self.data.groupby('machine')['HasMaintenance'].sum() / 
            self.data.groupby('machine').size()
        )
        
        # Top priority machines
        priority_machines = machine_stats.nlargest(5, 'MaintenanceTime_sum')
        
        print(f"\n🚨 Priority Machines for Maintenance Review:")
        for machine, row in priority_machines.iterrows():
            print(f"  {machine}:")
            print(f"    Total Maintenance: {row['MaintenanceTime_sum']:,.0f}s")
            print(f"    Maintenance Frequency: {row['maintenance_frequency']:.1%}")
            print(f"    Average Efficiency: {row['Efficiency_mean']:.1%}")
    
    def run_full_analysis(self):
        """Run complete predictive maintenance analysis"""
        print("🚀 Starting Predictive Maintenance Analysis")
        print("="*50)
        
        # Fetch and preprocess data
        raw_data = self.fetch_data(limit=3000)  # Use 3 batches of 1000 records
        if raw_data is None:
            return
        
        self.data = self.preprocess_data(raw_data)
        
        # Run analyses
        machine_stats = self.analyze_maintenance_patterns(self.data)
        model_results = self.build_maintenance_prediction_model(self.data)
        anomalies = self.detect_anomalies(self.data)
        
        # Generate report
        self.generate_maintenance_report()
        
        # Example predictions for top machines
        print(f"\n🔮 Sample Maintenance Predictions:")
        top_machines = self.data['machine'].value_counts().head(3).index
        
        for machine in top_machines:
            prediction = self.predict_next_maintenance(machine)
            if prediction:
                print(f"\n  {machine}:")
                print(f"    Predicted Maintenance Time: {prediction['predicted_maintenance_time']:.1f}s")
                print(f"    Risk Level: {prediction['risk_level']}")
                print(f"    Recent Efficiency: {prediction['recent_efficiency']:.1%}")
        
        print(f"\n✅ Analysis Complete!")
        return self

def main():
    analyzer = PredictiveMaintenanceAnalyzer()
    analyzer.run_full_analysis()

if __name__ == "__main__":
    main()