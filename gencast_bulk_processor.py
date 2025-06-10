from google.cloud import bigquery, storage
from google.oauth2 import service_account
import pandas as pd
from datetime import datetime, timedelta
import os
from typing import Dict, List, Tuple

class GenCastBulkProcessor:
    def __init__(self, service_account_path: str = 'service_acct.json'):
        """Initialize the bulk processor with service account credentials."""
        self.credentials = service_account.Credentials.from_service_account_file(
            service_account_path,
            scopes=[
                "https://www.googleapis.com/auth/cloud-platform",
                "https://www.googleapis.com/auth/devstorage.read_write"
            ]
        )
        
        # Initialize clients
        self.bq_client = bigquery.Client(
            credentials=self.credentials,
            project=self.credentials.project_id
        )
        self.storage_client = storage.Client(
            credentials=self.credentials,
            project=self.credentials.project_id
        )
        
        # Constants
        self.project_id = "ultra-task-456813-d5"
        self.dataset_id = "weathernext_gen_forecasts"
        self.table_id = "126478713_1_0"
        self.bucket_name = "gencast_exports"  # You'll need to create this bucket
        
    def get_bulk_data(
        self,
        start_date: datetime,
        end_date: datetime,
        latitude: float,
        longitude: float,
        radius_km: float = 10
    ) -> str:
        """
        Get bulk GenCast predictions for a date range and location.
        
        Args:
            start_date: Start date for predictions
            end_date: End date for predictions
            latitude: Target latitude
            longitude: Target longitude
            radius_km: Radius in kilometers to search for nearest point
            
        Returns:
            Path to downloaded CSV file
        """
        # Format dates for query
        start_str = start_date.strftime('%Y-%m-%d')
        end_str = end_date.strftime('%Y-%m-%d')
        
        # Build query to get all predictions within date range
        query = f"""
        WITH nearest_points AS (
          SELECT 
            init_time,
            ST_DISTANCE(
              geography,
              ST_GEOGPOINT({longitude}, {latitude})
            ) as distance_meters,
            forecast.*
          FROM `{self.project_id}.{self.dataset_id}.{self.table_id}`
          WHERE 
            DATE(init_time) BETWEEN '{start_str}' AND '{end_str}'
            AND ST_DISTANCE(
              geography,
              ST_GEOGPOINT({longitude}, {latitude})
            ) <= {radius_km * 1000}  -- Convert km to meters
        )
        SELECT 
          init_time,
          distance_meters,
          temperature_2m,
          ensemble_member,
          forecast_hour
        FROM nearest_points
        ORDER BY init_time, ensemble_member, forecast_hour
        """
        
        # Create unique export filename
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"gencast_export_{timestamp}.csv"
        
        try:
            # Configure the export job
            bucket = self.storage_client.bucket(self.bucket_name)
            destination_uri = f"gs://{self.bucket_name}/{filename}"
            
            # Run query and export to GCS
            job_config = bigquery.QueryJobConfig(
                destination=f"{self.project_id}.{self.dataset_id}.temp_export",
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
            )
            
            # Run the query
            query_job = self.bq_client.query(query, job_config=job_config)
            query_job.result()  # Wait for query to complete
            
            # Export results to GCS
            export_config = bigquery.ExtractJobConfig()
            export_config.destination_format = bigquery.DestinationFormat.CSV
            
            extract_job = self.bq_client.extract_table(
                f"{self.project_id}.{self.dataset_id}.temp_export",
                destination_uri,
                job_config=export_config
            )
            extract_job.result()  # Wait for export to complete
            
            # Download the file locally
            local_filename = f"data/{filename}"
            os.makedirs("data", exist_ok=True)
            
            blob = bucket.blob(filename)
            blob.download_to_filename(local_filename)
            
            # Clean up GCS
            blob.delete()
            
            return local_filename
            
        except Exception as e:
            print(f"Error in bulk export: {str(e)}")
            return None
            
    def process_bulk_data(
        self,
        csv_path: str,
        target_dates: List[datetime]
    ) -> Dict[datetime, float]:
        """
        Process downloaded CSV into temperature predictions.
        
        Args:
            csv_path: Path to downloaded CSV file
            target_dates: List of dates to get predictions for
            
        Returns:
            Dictionary mapping dates to temperature predictions
        """
        try:
            # Read CSV
            df = pd.read_csv(csv_path)
            
            # Convert init_time to datetime
            df['init_time'] = pd.to_datetime(df['init_time'])
            
            # Process each target date
            predictions = {}
            for target_date in target_dates:
                # Get predictions initialized before target date
                mask = (df['init_time'] <= target_date)
                relevant_preds = df[mask]
                
                if not relevant_preds.empty:
                    # Get latest initialization time
                    latest_init = relevant_preds['init_time'].max()
                    latest_preds = relevant_preds[relevant_preds['init_time'] == latest_init]
                    
                    # Calculate hours between init time and target date
                    hours_diff = int((target_date - latest_init).total_seconds() / 3600)
                    
                    # Get predictions for target forecast hour
                    target_preds = latest_preds[latest_preds['forecast_hour'] == hours_diff]
                    
                    if not target_preds.empty:
                        # Average across ensemble members
                        avg_temp = target_preds['temperature_2m'].mean()
                        predictions[target_date] = avg_temp
                    else:
                        predictions[target_date] = None
                else:
                    predictions[target_date] = None
            
            return predictions
            
        except Exception as e:
            print(f"Error processing bulk data: {str(e)}")
            return None
            
    def cleanup(self, csv_path: str):
        """Clean up downloaded CSV file."""
        try:
            if os.path.exists(csv_path):
                os.remove(csv_path)
        except Exception as e:
            print(f"Error cleaning up file {csv_path}: {str(e)}")

def get_gencast_predictions(
    start_date: datetime,
    end_date: datetime,
    latitude: float,
    longitude: float
) -> List[float]:
    """
    Get GenCast predictions for a date range.
    
    Args:
        start_date: Start date for predictions
        end_date: End date for predictions
        latitude: Target latitude
        longitude: Target longitude
        
    Returns:
        List of temperature predictions in Fahrenheit
    """
    processor = GenCastBulkProcessor()
    
    # Generate list of target dates
    target_dates = []
    current_date = start_date
    while current_date <= end_date:
        target_dates.append(current_date)
        current_date += timedelta(days=1)
    
    try:
        # Get and process bulk data
        csv_path = processor.get_bulk_data(
            start_date=start_date - timedelta(days=10),  # Get extra history
            end_date=end_date,
            latitude=latitude,
            longitude=longitude
        )
        
        if csv_path:
            predictions = processor.process_bulk_data(csv_path, target_dates)
            processor.cleanup(csv_path)
            
            # Convert to list in correct order
            return [predictions.get(date, 0.0) for date in target_dates]
        else:
            return [0.0] * len(target_dates)
            
    except Exception as e:
        print(f"Error getting GenCast predictions: {str(e)}")
        return [0.0] * len(target_dates) 