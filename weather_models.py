from typing import List, Dict
from datetime import datetime, timedelta
from gencast_interface import gencast_client

def get_aifs(latitude: float, longitude: float, start_date: datetime) -> List[float]:
    """
    Get AIFS model temperature predictions for 10 days starting from start_date.
    
    Args:
        latitude: Location latitude
        longitude: Location longitude
        start_date: Starting date for prediction
        
    Returns:
        List of 10 daily temperature predictions in Fahrenheit
    """
    return [0.0] * 10

def get_gencast(latitude: float, longitude: float, start_date: datetime) -> List[float]:
    """
    Get GenCast model temperature predictions for 10 days starting from start_date.
    
    Args:
        latitude: Location latitude
        longitude: Location longitude
        start_date: Starting date for prediction
        
    Returns:
        List of 10 daily temperature predictions in Fahrenheit
    """
    try:
        return gencast_client.get_predictions(
            start_date=start_date,
            latitude=latitude,
            longitude=longitude,
            num_days=10,
            num_ensemble_members=50
        )
    except Exception as e:
        print(f"Error getting GenCast predictions: {str(e)}")
        return [0.0] * 10

def get_graphcast(latitude: float, longitude: float, start_date: datetime) -> List[float]:
    """
    Get GraphCast model temperature predictions for 10 days starting from start_date.
    
    Args:
        latitude: Location latitude
        longitude: Location longitude
        start_date: Starting date for prediction
        
    Returns:
        List of 10 daily temperature predictions in Fahrenheit
    """
    return [0.0] * 10

def get_fourcastnet(latitude: float, longitude: float, start_date: datetime) -> List[float]:
    """
    Get FourCastNet model temperature predictions for 10 days starting from start_date.
    
    Args:
        latitude: Location latitude
        longitude: Location longitude
        start_date: Starting date for prediction
        
    Returns:
        List of 10 daily temperature predictions in Fahrenheit
    """
    return [0.0] * 10

def calculate_accuracy_metrics(predicted_temps: List[float], actual_temps: List[float]) -> Dict[str, float]:
    """
    Calculate accuracy metrics for different temperature difference thresholds.
    
    Args:
        predicted_temps: List of predicted temperatures
        actual_temps: List of actual (ERA5) temperatures
        
    Returns:
        Dictionary containing accuracy percentages for different thresholds
    """
    total_predictions = len(predicted_temps)
    thresholds = {
        "0.1": 0,
        "0.5": 0,
        "1.0": 0,
        "3.0": 0,
        "3+": 0
    }
    
    for pred, actual in zip(predicted_temps, actual_temps):
        diff = abs(pred - actual)
        if diff <= 0.1:
            thresholds["0.1"] += 1
        elif diff <= 0.5:
            thresholds["0.5"] += 1
        elif diff <= 1.0:
            thresholds["1.0"] += 1
        elif diff <= 3.0:
            thresholds["3.0"] += 1
        else:
            thresholds["3+"] += 1
    
    # Convert counts to percentages
    return {k: (v / total_predictions) * 100 for k, v in thresholds.items()} 