from datetime import datetime, timedelta
from typing import Dict, List
import argparse
import sys
from weather_models import (
    get_aifs, get_gencast, get_graphcast, get_fourcastnet,
    calculate_accuracy_metrics
)
from era5_interface import era5_client

class ModelAccuracyTracker:
    def __init__(self):
        self.thresholds = ["0.1", "0.5", "1.0", "3.0", "3+"]
        self.models = ['aifs', 'gencast', 'graphcast', 'fourcastnet']
        # Track accuracy by model, forecast day (1-9), and threshold
        self.accuracies = {
            model: {
                f"day_{day}": {threshold: [] for threshold in self.thresholds}
                for day in range(1, 10)  # 1-day ahead to 9-day ahead
            }
            for model in self.models
        }
    
    def update_accuracies(self, model: str, forecast_day: int, metrics: Dict[str, float]):
        """Update accuracy metrics for a specific model and forecast day."""
        day_key = f"day_{forecast_day}"
        for threshold in self.thresholds:
            self.accuracies[model][day_key][threshold].append(metrics[threshold])
    
    def get_average_accuracies(self) -> Dict[str, Dict[str, Dict[str, float]]]:
        """Get average accuracies by model and forecast day."""
        result = {}
        for model in self.models:
            result[model] = {}
            for day in range(1, 10):
                day_key = f"day_{day}"
                result[model][day_key] = {}
                for threshold in self.thresholds:
                    values = self.accuracies[model][day_key][threshold]
                    result[model][day_key][threshold] = sum(values) / len(values) if values else 0
        return result

def parse_date(date_string: str) -> datetime:
    """
    Parse a date string in DD-MM-YYYY format.
    
    Args:
        date_string: Date in DD-MM-YYYY format
        
    Returns:
        datetime object
        
    Raises:
        ValueError: If date format is invalid
    """
    try:
        return datetime.strptime(date_string, "%d-%m-%Y")
    except ValueError:
        raise ValueError(f"Invalid date format: {date_string}. Please use DD-MM-YYYY format.")

def validate_date_range(start_date: datetime, end_date: datetime) -> None:
    """
    Validate that end_date is after start_date.
    
    Args:
        start_date: Starting date
        end_date: Ending date
        
    Raises:
        ValueError: If end_date is not after start_date
    """
    if end_date <= start_date:
        raise ValueError(f"End date ({end_date.strftime('%d-%m-%Y')}) must be after start date ({start_date.strftime('%d-%m-%Y')})")

def run_weather_comparison(
    latitude: float,
    longitude: float,
    start_date: datetime,
    end_date: datetime
) -> Dict[str, Dict[str, Dict[str, float]]]:
    """
    Run weather model comparisons between start_date and end_date.
    
    Args:
        latitude: Location latitude
        longitude: Location longitude
        start_date: Starting date for predictions
        end_date: Ending date for predictions
        
    Returns:
        Dictionary containing accuracy metrics by model and forecast day
    """
    # Calculate total days
    total_days = (end_date - start_date).days + 1
    
    print(f"Downloading ERA5 data for entire test period...")
    print(f"Test period: {total_days} days")
    print(f"Need ERA5 data through: {(end_date + timedelta(days=9)).strftime('%d-%m-%Y')} (for 9-day forecasts)")
    
    # Calculate the full range needed (end_date + 9 days for 9-day forecasts)
    era5_end_date = end_date + timedelta(days=9)
    
    # Download ERA5 data for the entire test period + 9-day forecast window
    era5_cached_file = era5_client.download_date_range(
        start_date=start_date,
        end_date=era5_end_date,
        latitude=latitude,
        longitude=longitude
    )
    
    print(f"ERA5 data cached to: {era5_cached_file}")
    print(f"Running model comparisons by forecast lead time...")
    
    tracker = ModelAccuracyTracker()
    current_date = start_date
    day_count = 0
    
    while current_date <= end_date:
        day_count += 1
        print(f"Processing predictions for {current_date.strftime('%Y-%m-%d')} ({day_count}/{total_days})")
        
        # Get predictions from each model (10-day forecast: today + 9 days ahead)
        predictions = {
            'aifs': get_aifs(latitude, longitude, current_date),
            'gencast': get_gencast(latitude, longitude, current_date),
            'graphcast': get_graphcast(latitude, longitude, current_date),
            'fourcastnet': get_fourcastnet(latitude, longitude, current_date)
        }
        
        # For each forecast day (1-day ahead to 9-day ahead)
        for forecast_day in range(1, 10):
            target_date = current_date + timedelta(days=forecast_day)
            
            # Get ERA5 temperature for this target date
            era5_temp = era5_client.get_temperature_from_cached_file(
                era5_cached_file, latitude, longitude, target_date
            )
            
            # Compare each model's prediction for this forecast day
            for model, model_predictions in predictions.items():
                # The prediction for this forecast day (index 0 is today, index 1 is 1-day ahead, etc.)
                predicted_temp = model_predictions[forecast_day]  # forecast_day 1-9 maps to index 1-9
                
                # Calculate accuracy metrics for this single prediction vs actual
                metrics = calculate_accuracy_metrics([predicted_temp], [era5_temp])
                
                # Update tracker for this model and forecast day
                tracker.update_accuracies(model, forecast_day, metrics)
        
        current_date += timedelta(days=1)
    
    return tracker.get_average_accuracies()

def format_accuracy_results(accuracies: Dict[str, Dict[str, Dict[str, float]]]) -> str:
    """Format accuracy results by forecast lead time."""
    output = []
    output.append("\nAccuracy Results by Forecast Lead Time:")
    output.append("=" * 100)
    
    for model in ['aifs', 'gencast', 'graphcast', 'fourcastnet']:
        output.append(f"\n{model.upper()} Model:")
        output.append("-" * 80)
        output.append(f"{'Lead Time':<12} {'≤0.1°F':<10} {'≤0.5°F':<10} {'≤1.0°F':<10} {'≤3.0°F':<10} {'>3.0°F':<10}")
        output.append("-" * 80)
        
        for day in range(1, 10):
            day_key = f"day_{day}"
            metrics = accuracies[model][day_key]
            row = [
                f"{day}-day ahead",
                f"{metrics['0.1']:>8.1f}%",
                f"{metrics['0.5']:>8.1f}%",
                f"{metrics['1.0']:>8.1f}%",
                f"{metrics['3.0']:>8.1f}%",
                f"{metrics['3+']:>8.1f}%"
            ]
            output.append(" ".join(row))
    
    return "\n".join(output)

def main():
    parser = argparse.ArgumentParser(
        description="Compare weather model predictions against ERA5 reanalysis data by forecast lead time",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python weather_runner.py 01-01-2024 31-01-2024    # Test January 2024
  python weather_runner.py 15-06-2023 15-09-2023    # Test summer 2023
  python weather_runner.py 01-01-2023 31-12-2023    # Test entire year 2023

Date format: DD-MM-YYYY

Note: For N test days, ERA5 data will be downloaded for N+9 days total
to accommodate 9-day-ahead forecasts.
        """
    )
    
    parser.add_argument(
        'start_date',
        help='Start date in DD-MM-YYYY format'
    )
    
    parser.add_argument(
        'end_date',
        help='End date in DD-MM-YYYY format'
    )
    
    parser.add_argument(
        '--latitude',
        type=float,
        default=40.7128,
        help='Location latitude (default: 40.7128 - NYC)'
    )
    
    parser.add_argument(
        '--longitude',
        type=float,
        default=-74.0060,
        help='Location longitude (default: -74.0060 - NYC)'
    )
    
    args = parser.parse_args()
    
    try:
        # Parse and validate dates
        start_date = parse_date(args.start_date)
        end_date = parse_date(args.end_date)
        validate_date_range(start_date, end_date)
        
        print(f"\nRunning weather model comparison for location: ({args.latitude}, {args.longitude})")
        print(f"Date range: {start_date.strftime('%d-%m-%Y')} to {end_date.strftime('%d-%m-%Y')}")
        
        # Run the comparison
        accuracies = run_weather_comparison(args.latitude, args.longitude, start_date, end_date)
        print(format_accuracy_results(accuracies))
        
    except ValueError as e:
        print(f"Error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 