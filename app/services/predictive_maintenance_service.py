"""
Predictive Maintenance Service
Provides comprehensive machine learning-based maintenance analysis
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
import logging
from sklearn.ensemble import RandomForestRegressor, IsolationForest
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, r2_score
from sklearn.preprocessing import StandardScaler
import warnings
warnings.filterwarnings('ignore')

logger = logging.getLogger(__name__)

class PredictiveMaintenanceService:
    """Service for predictive maintenance analysis and ML predictions"""
    
    def __init__(self):
        self.model = None
        self.scaler = StandardScaler()
        self.anomaly_detector = None
        self.feature_importance = None
        
    async def fetch_data(self, db: AsyncSession, limit: int = 5000) -> pd.DataFrame:
        """Fetch manufacturing data from database"""
        try:
            # Fetch data in batches to respect API limits
            batch_size = 1000
            all_data = []
            
            for offset in range(0, limit, batch_size):
                current_batch_size = min(batch_size, limit - offset)
                
                result = await db.execute(text("""
                    SELECT 
                        machine_id,
                        job_number,
                        start_time,
                        job_duration,
                        running_time,
                        setup_time,
                        idle_time,
                        maintenance_time,
                        parts_produced,
                        emp_id,
                        CASE WHEN job_duration > 0 THEN running_time::float / job_duration ELSE 0 END as efficiency
                    FROM job_records
                    ORDER BY start_time DESC
                    LIMIT :limit OFFSET :offset
                """), {"limit": current_batch_size, "offset": offset})
                
                batch_data = [dict(row._mapping) for row in result]
                all_data.extend(batch_data)
                
                logger.info(f"Fetched batch {offset//batch_size + 1}/{(limit-1)//batch_size + 1} ({len(batch_data)} records)")
            
            df = pd.DataFrame(all_data)
            logger.info(f"Total records loaded: {len(df)}")
            return df
            
        except Exception as e:
            logger.error(f"Error fetching data: {e}")
            raise
    
    def preprocess_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Preprocess the manufacturing data for ML analysis"""
        try:
            # Convert datetime
            df['start_time'] = pd.to_datetime(df['start_time'])
            
            # Fill missing values
            numeric_columns = ['job_duration', 'running_time', 'setup_time', 'idle_time', 
                             'maintenance_time', 'parts_produced', 'efficiency']
            for col in numeric_columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            
            # Create time-based features
            df['hour'] = df['start_time'].dt.hour
            df['day_of_week'] = df['start_time'].dt.dayofweek
            df['month'] = df['start_time'].dt.month
            
            # Create efficiency ratios
            df['setup_ratio'] = df['setup_time'] / (df['job_duration'] + 1)
            df['idle_ratio'] = df['idle_time'] / (df['job_duration'] + 1)
            df['maintenance_ratio'] = df['maintenance_time'] / (df['job_duration'] + 1)
            
            # Create rolling averages (7-day windows)
            df = df.sort_values(['machine_id', 'start_time'])
            
            for machine in df['machine_id'].unique():
                mask = df['machine_id'] == machine
                df.loc[mask, 'maintenance_time_ma7'] = df.loc[mask, 'maintenance_time'].rolling(window=7, min_periods=1).mean()
                df.loc[mask, 'efficiency_ma7'] = df.loc[mask, 'efficiency'].rolling(window=7, min_periods=1).mean()
                df.loc[mask, 'setup_time_ma7'] = df.loc[mask, 'setup_time'].rolling(window=7, min_periods=1).mean()
            
            # Fill any remaining NaN values
            df = df.fillna(0)
            
            logger.info(f"Preprocessed {len(df)} records")
            return df
            
        except Exception as e:
            logger.error(f"Error preprocessing data: {e}")
            raise
    
    def analyze_maintenance_patterns(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze maintenance patterns and identify high-maintenance machines"""
        try:
            # Calculate maintenance statistics by machine
            maintenance_stats = df.groupby('machine_id').agg({
                'maintenance_time': ['sum', 'mean', 'count'],
                'efficiency': 'mean',
                'job_duration': 'count'
            }).round(2)
            
            # Flatten column names
            maintenance_stats.columns = ['total_maintenance', 'avg_maintenance', 'maintenance_jobs', 
                                       'avg_efficiency', 'total_jobs']
            
            # Calculate maintenance frequency
            maintenance_stats['maintenance_frequency'] = (
                maintenance_stats['maintenance_jobs'] / maintenance_stats['total_jobs'] * 100
            ).round(2)
            
            # Filter machines with maintenance > 0
            high_maintenance = maintenance_stats[
                maintenance_stats['total_maintenance'] > 0
            ].sort_values('total_maintenance', ascending=False)
            
            # Convert to list of dictionaries for JSON serialization
            top_machines = []
            for machine_id, row in high_maintenance.head(10).iterrows():
                top_machines.append({
                    'machine_id': machine_id,
                    'total_maintenance_time': int(row['total_maintenance']),
                    'maintenance_frequency': float(row['maintenance_frequency']),
                    'average_efficiency': float(row['avg_efficiency'] * 100),
                    'total_jobs': int(row['total_jobs'])
                })
            
            return {
                'top_high_maintenance_machines': top_machines,
                'total_machines_analyzed': len(maintenance_stats),
                'machines_with_maintenance': len(high_maintenance)
            }
            
        except Exception as e:
            logger.error(f"Error analyzing maintenance patterns: {e}")
            raise
    
    def build_prediction_model(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Build and train maintenance prediction model"""
        try:
            # Prepare features for ML model
            feature_columns = [
                'job_duration', 'setup_time', 'idle_time', 'parts_produced',
                'efficiency', 'hour', 'day_of_week', 'month',
                'setup_ratio', 'idle_ratio', 'maintenance_time_ma7', 
                'efficiency_ma7', 'setup_time_ma7'
            ]
            
            # Filter out rows with missing features
            model_df = df[feature_columns + ['maintenance_time']].dropna()
            
            if len(model_df) < 100:
                raise ValueError("Insufficient data for model training")
            
            X = model_df[feature_columns]
            y = model_df['maintenance_time']
            
            # Split data
            X_train, X_test, y_train, y_test = train_test_split(
                X, y, test_size=0.2, random_state=42
            )
            
            # Scale features
            X_train_scaled = self.scaler.fit_transform(X_train)
            X_test_scaled = self.scaler.transform(X_test)
            
            # Train model
            self.model = RandomForestRegressor(
                n_estimators=100,
                max_depth=10,
                random_state=42,
                n_jobs=-1
            )
            
            self.model.fit(X_train_scaled, y_train)
            
            # Evaluate model
            y_pred = self.model.predict(X_test_scaled)
            mae = mean_absolute_error(y_test, y_pred)
            r2 = r2_score(y_test, y_pred)
            
            # Get feature importance
            self.feature_importance = dict(zip(
                feature_columns,
                self.model.feature_importances_
            ))
            
            # Sort by importance
            sorted_features = sorted(
                self.feature_importance.items(),
                key=lambda x: x[1],
                reverse=True
            )
            
            return {
                'model_performance': {
                    'mean_absolute_error': float(mae),
                    'r2_score': float(r2),
                    'training_samples': len(X_train),
                    'test_samples': len(X_test)
                },
                'feature_importance': {
                    name: float(importance) for name, importance in sorted_features[:5]
                }
            }
            
        except Exception as e:
            logger.error(f"Error building prediction model: {e}")
            raise
    
    def detect_anomalies(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Detect anomalous maintenance patterns"""
        try:
            # Prepare features for anomaly detection
            anomaly_features = [
                'maintenance_time', 'setup_time', 'idle_time', 'efficiency',
                'setup_ratio', 'idle_ratio', 'maintenance_ratio'
            ]
            
            anomaly_df = df[anomaly_features].fillna(0)
            
            # Train anomaly detector
            self.anomaly_detector = IsolationForest(
                contamination=0.1,
                random_state=42,
                n_jobs=-1
            )
            
            anomaly_scores = self.anomaly_detector.fit_predict(anomaly_df)
            
            # Add anomaly scores to dataframe
            df_with_anomalies = df.copy()
            df_with_anomalies['is_anomaly'] = anomaly_scores == -1
            
            # Count anomalies by machine
            anomaly_counts = df_with_anomalies[
                df_with_anomalies['is_anomaly']
            ].groupby('machine_id').size().sort_values(ascending=False)
            
            total_anomalies = df_with_anomalies['is_anomaly'].sum()
            anomaly_rate = (total_anomalies / len(df_with_anomalies) * 100)
            
            # Top machines with anomalies
            top_anomaly_machines = []
            for machine_id, count in anomaly_counts.head(10).items():
                top_anomaly_machines.append({
                    'machine_id': machine_id,
                    'anomaly_count': int(count)
                })
            
            return {
                'total_anomalies': int(total_anomalies),
                'anomaly_rate_percent': float(anomaly_rate),
                'machines_with_most_anomalies': top_anomaly_machines
            }
            
        except Exception as e:
            logger.error(f"Error detecting anomalies: {e}")
            raise
    
    def generate_predictions(self, df: pd.DataFrame, sample_size: int = 5) -> List[Dict[str, Any]]:
        """Generate sample maintenance predictions for machines"""
        try:
            if self.model is None:
                raise ValueError("Model not trained. Call build_prediction_model first.")
            
            # Get recent data for each machine
            recent_data = df.groupby('machine_id').tail(1).head(sample_size)
            
            feature_columns = [
                'job_duration', 'setup_time', 'idle_time', 'parts_produced',
                'efficiency', 'hour', 'day_of_week', 'month',
                'setup_ratio', 'idle_ratio', 'maintenance_time_ma7', 
                'efficiency_ma7', 'setup_time_ma7'
            ]
            
            predictions = []
            
            for _, row in recent_data.iterrows():
                try:
                    # Prepare features
                    features = row[feature_columns].values.reshape(1, -1)
                    features_scaled = self.scaler.transform(features)
                    
                    # Make prediction
                    predicted_maintenance = self.model.predict(features_scaled)[0]
                    
                    # Determine risk level
                    if predicted_maintenance > 3600:  # > 1 hour
                        risk_level = "HIGH"
                    elif predicted_maintenance > 1800:  # > 30 minutes
                        risk_level = "MEDIUM"
                    else:
                        risk_level = "LOW"
                    
                    predictions.append({
                        'machine_id': row['machine_id'],
                        'predicted_maintenance_time': float(predicted_maintenance),
                        'risk_level': risk_level,
                        'recent_efficiency': float(row['efficiency'] * 100)
                    })
                    
                except Exception as e:
                    logger.warning(f"Error predicting for machine {row['machine_id']}: {e}")
                    continue
            
            return predictions
            
        except Exception as e:
            logger.error(f"Error generating predictions: {e}")
            raise
    
    async def run_comprehensive_analysis(self, db: AsyncSession, limit: int = 3000) -> Dict[str, Any]:
        """Run complete predictive maintenance analysis"""
        try:
            logger.info("Starting comprehensive predictive maintenance analysis")
            
            # Fetch and preprocess data
            raw_data = await self.fetch_data(db, limit=limit)
            processed_data = self.preprocess_data(raw_data)
            
            # Run analysis components
            maintenance_patterns = self.analyze_maintenance_patterns(processed_data)
            model_results = self.build_prediction_model(processed_data)
            anomaly_results = self.detect_anomalies(processed_data)
            predictions = self.generate_predictions(processed_data)
            
            # Calculate overall statistics
            total_jobs = len(processed_data)
            jobs_with_maintenance = len(processed_data[processed_data['maintenance_time'] > 0])
            maintenance_rate = (jobs_with_maintenance / total_jobs * 100) if total_jobs > 0 else 0
            total_maintenance_time = processed_data['maintenance_time'].sum()
            avg_maintenance_time = processed_data[processed_data['maintenance_time'] > 0]['maintenance_time'].mean()
            
            return {
                'analysis_timestamp': datetime.now().isoformat(),
                'data_summary': {
                    'total_jobs_analyzed': total_jobs,
                    'jobs_with_maintenance': jobs_with_maintenance,
                    'maintenance_rate_percent': round(maintenance_rate, 1),
                    'total_maintenance_time_seconds': int(total_maintenance_time),
                    'average_maintenance_time_seconds': int(avg_maintenance_time) if not pd.isna(avg_maintenance_time) else 0
                },
                'maintenance_patterns': maintenance_patterns,
                'model_performance': model_results,
                'anomaly_detection': anomaly_results,
                'sample_predictions': predictions
            }
            
        except Exception as e:
            logger.error(f"Error in comprehensive analysis: {e}")
            raise