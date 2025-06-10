from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, timedelta
import os
from typing import List, Optional

class GenCastInterface:
    def __init__(self, service_account_path: str = 'service_acct.json'):
        """
        Initialize GenCast interface using service account credentials.
        
        Args:
            service_account_path: Path to service account JSON file
        """
        credentials = service_account.Credentials.from_service_account_file(
            service_account_path,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        
        self.client = bigquery.Client(
            credentials=credentials,
            project=credentials.project_id,
        )
        
        # GenCast table details
        self.project_id = "ultra-task-456813-d5"
        self.dataset_id = "weathernext_gen_forecasts"
        self.table_id = "126478713_1_0"
        
    def get_temperature_prediction(
        self,
        target_date: datetime,
        latitude: float,
        longitude: float,
        num_ensemble_members: int = 50
    ) -> Optional[float]:
        """
        Get average 2m temperature prediction from ensemble members.
        
        Args:
            target_date: Date to get prediction for (will use 12:00 UTC)
            latitude: Target latitude
            longitude: Target longitude
            num_ensemble_members: Number of ensemble members to average
            
        Returns:
            Average temperature in Fahrenheit, or None if no data found
        """
        # Format the date for SQL
        forecast_date = target_date.strftime('%Y-%m-%d')
        
        # Define the query
        query = f"""
        WITH nearest_point AS (
          SELECT 
            latitude,
            longitude,
            ensemble_member,
            temperature_2m,  -- Adjust column name based on actual schema
            ST_DISTANCE(
              ST_GEOGPOINT(longitude, latitude),
              ST_GEOGPOINT({longitude}, {latitude})
            ) as distance_meters
          FROM 
            `{self.project_id}.{self.dataset_id}.{self.table_id}`
          WHERE 
            forecast_date = '{forecast_date}'  -- Adjust column name based on actual schema
            AND forecast_time = '12:00:00'  -- Adjust column name based on actual schema
            AND ensemble_member <= {num_ensemble_members}  -- Adjust column name based on actual schema
        )
        SELECT 
            AVG(temperature_2m) as avg_temperature_2m,
            COUNT(*) as num_ensemble_members,
            STDDEV(temperature_2m) as temp_stddev,
            MIN(distance_meters) as distance_to_target_meters
        FROM nearest_point
        WHERE distance_meters = (
            SELECT MIN(distance_meters) 
            FROM nearest_point
        )
        """
        
        try:
            # Run the query
            query_job = self.client.query(query)
            results = query_job.result()
            
            # Get the first (and should be only) row
            for row in results:
                if row.num_ensemble_members > 0:
                    # Convert temperature to Fahrenheit if needed
                    # Assuming temperature is in Kelvin
                    temp_kelvin = row.avg_temperature_2m
                    temp_fahrenheit = (temp_kelvin - 273.15) * 9/5 + 32
                    return temp_fahrenheit
                    
            return None
            
        except Exception as e:
            print(f"Error querying GenCast data: {str(e)}")
            return None
            
    def get_predictions(
        self,
        start_date: datetime,
        latitude: float,
        longitude: float,
        num_days: int = 10,
        num_ensemble_members: int = 50
    ) -> List[float]:
        """
        Get temperature predictions for multiple days.
        
        Args:
            start_date: Starting date for predictions
            latitude: Target latitude
            longitude: Target longitude
            num_days: Number of days to predict (default 10)
            num_ensemble_members: Number of ensemble members to average
            
        Returns:
            List of temperature predictions in Fahrenheit
        """
        predictions = []
        for i in range(num_days):
            target_date = start_date + timedelta(days=i)
            temp = self.get_temperature_prediction(
                target_date=target_date,
                latitude=latitude,
                longitude=longitude,
                num_ensemble_members=num_ensemble_members
            )
            predictions.append(temp if temp is not None else 0.0)
            
        return predictions

# Create a singleton instance
gencast_client = GenCastInterface() 