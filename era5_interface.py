import os
import cdsapi
import xarray as xr
import numpy as np
from datetime import datetime, timedelta
from typing import Optional, Tuple, List
import tempfile
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class ERA5Interface:
    def __init__(self):
        """
        Initialize the ERA5 interface using the existing ~/.cdsapirc file.
        """
        self.temp_dir = tempfile.mkdtemp()
        test_file = os.path.join(self.temp_dir, 'test_connection.nc')
        
        try:
            self.client = cdsapi.Client()
            # Test the connection with a minimal request
            self.client.retrieve(
                'reanalysis-era5-single-levels',
                {
                    'variable': '2m_temperature',
                    'year': '2024',
                    'month': '01',
                    'day': '01',
                    'time': '12:00',
                    'data_format': 'netcdf',
                },
                test_file
            )
            # Clean up test file
            if os.path.exists(test_file):
                os.remove(test_file)
        except Exception as e:
            raise ConnectionError(
                f"Failed to connect to ERA5. Please ensure:\n"
                f"1. Your ~/.cdsapirc file exists and contains valid credentials\n"
                f"2. You've accepted the ERA5 terms at https://cds.climate.copernicus.eu/cdsapp#!/terms/licence-to-use-copernicus-products\n"
                f"Error: {str(e)}"
            )
        
    def __del__(self):
        """Cleanup temporary directory."""
        if hasattr(self, 'temp_dir') and os.path.exists(self.temp_dir):
            try:
                import shutil
                shutil.rmtree(self.temp_dir)
            except:
                pass

    def _kelvin_to_fahrenheit(self, kelvin: float) -> float:
        """Convert temperature from Kelvin to Fahrenheit."""
        celsius = kelvin - 273.15
        return (celsius * 9/5) + 32
    
    def _get_data_filename(self, date: datetime) -> str:
        """Generate a filename for the downloaded data."""
        return os.path.join(self.temp_dir, f"era5_{date.strftime('%Y%m%d')}.nc")
    
    def _download_era5_data(self, target_date: datetime) -> str:
        """
        Download ERA5 temperature data for a specific date at 12:00 UTC.
        
        Args:
            target_date: Date for which to download data
            
        Returns:
            Path to the downloaded file
        """
        output_file = self._get_data_filename(target_date)
        
        # Skip if file already exists
        if os.path.exists(output_file):
            return output_file
            
        try:
            self.client.retrieve(
                'reanalysis-era5-single-levels',
                {
                    'variable': '2m_temperature',
                    'year': target_date.strftime('%Y'),
                    'month': target_date.strftime('%m'),
                    'day': target_date.strftime('%d'),
                    'time': '12:00',
                    'data_format': 'netcdf',
                },
                output_file
            )
        except Exception as e:
            raise ConnectionError(f"Failed to download ERA5 data: {str(e)}")
        
        return output_file
    
    def get_temperature(self, latitude: float, longitude: float, target_date: datetime) -> float:
        """
        Get the 12:00 UTC temperature for a specific location and date.
        
        Args:
            latitude: Location latitude
            longitude: Location longitude
            target_date: Target date for temperature data
            
        Returns:
            Temperature in Fahrenheit at 12:00 UTC
        """
        try:
            # Download data if needed
            filename = self._download_era5_data(target_date)
            
            # Read the NetCDF file
            with xr.open_dataset(filename) as ds:
                # Extract temperature data for the location
                temp_data = ds.sel(
                    latitude=latitude,
                    longitude=longitude,
                    method='nearest'
                )['t2m'].values
                
                # Get the temperature (data is in Kelvin)
                temp_kelvin = float(temp_data.item())  # Single value now, no need for mean
                
                # Convert to Fahrenheit
                return self._kelvin_to_fahrenheit(temp_kelvin)
                
        except Exception as e:
            print(f"Error retrieving ERA5 data: {str(e)}")
            raise

    def download_multiple_years(
        self,
        start_year: int,
        end_year: int,
        output_file: str,
        area: Optional[List[float]] = None,
        data_format: str = 'netcdf'
    ) -> str:
        """
        Download multiple years of ERA5 temperature data.
        
        Args:
            start_year: Starting year (inclusive)
            end_year: Ending year (inclusive)
            output_file: Path to save the downloaded data
            area: Optional list of [north, west, south, east] coordinates
            data_format: Either 'netcdf' or 'grib'
            
        Returns:
            Path to the downloaded file
        """
        years = [str(year) for year in range(start_year, end_year + 1)]
        months = [f"{month:02d}" for month in range(1, 13)]
        days = [f"{day:02d}" for day in range(1, 32)]
        
        request = {
            "product_type": ["reanalysis"],
            "variable": ["2m_temperature"],
            "year": years,
            "month": months,
            "day": days,
            "time": ["12:00"],
            "data_format": data_format,
        }
        
        if area:
            request["area"] = area
            
        try:
            self.client.retrieve('reanalysis-era5-single-levels', request, output_file)
            return output_file
        except Exception as e:
            raise ConnectionError(f"Failed to download ERA5 data: {str(e)}")

    def download_single_month(
        self,
        year: int,
        month: int,
        output_file: str,
        area: Optional[List[float]] = None,
        data_format: str = 'netcdf'
    ) -> str:
        """
        Download a single month of ERA5 temperature data.
        
        Args:
            year: Year to download
            month: Month to download (1-12)
            output_file: Path to save the downloaded data
            area: Optional list of [north, west, south, east] coordinates
            data_format: Either 'netcdf' or 'grib'
            
        Returns:
            Path to the downloaded file
        """
        request = {
            "product_type": ["reanalysis"],
            "variable": ["2m_temperature"],
            "year": [str(year)],
            "month": [f"{month:02d}"],
            "day": [f"{day:02d}" for day in range(1, 32)],
            "time": ["12:00"],
            "data_format": data_format,
        }
        
        if area:
            request["area"] = area
            
        try:
            self.client.retrieve('reanalysis-era5-single-levels', request, output_file)
            return output_file
        except Exception as e:
            raise ConnectionError(f"Failed to download ERA5 data: {str(e)}")

    def download_date_range(
        self,
        start_date: datetime,
        end_date: datetime,
        latitude: float,
        longitude: float,
        buffer_degrees: float = 0.125,
        data_format: str = 'netcdf'
    ) -> str:
        """
        Download ERA5 data for a specific date range and location.
        
        Args:
            start_date: Starting date for data download
            end_date: Ending date for data download
            latitude: Center latitude for data extraction
            longitude: Center longitude for data extraction
            buffer_degrees: Buffer around the point (default 0.125 for 0.25° grid)
            data_format: Either 'netcdf' or 'grib'
            
        Returns:
            Path to the downloaded file
        """
        # Create filename based on date range and location
        filename = os.path.join(
            self.temp_dir, 
            f"era5_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}_"
            f"{latitude:.3f}_{longitude:.3f}.nc"
        )
        
        # Skip if file already exists
        if os.path.exists(filename):
            return filename
            
        # Calculate area bounds
        north = latitude + buffer_degrees
        south = latitude - buffer_degrees
        west = longitude - buffer_degrees
        east = longitude + buffer_degrees
        area = [north, west, south, east]
        
        # Generate year, month, day lists
        years = []
        months = set()
        days = set()
        
        current_date = start_date
        while current_date <= end_date:
            years.append(current_date.year)
            months.add(current_date.month)
            days.add(current_date.day)
            current_date += timedelta(days=1)
            
        # Convert to strings and remove duplicates
        years = list(set([str(year) for year in years]))
        months = [f"{month:02d}" for month in sorted(months)]
        days = [f"{day:02d}" for day in sorted(days)]
        
        request = {
            "product_type": ["reanalysis"],
            "variable": ["2m_temperature"],
            "year": years,
            "month": months,
            "day": days,
            "time": ["12:00"],
            "data_format": data_format,
            "area": area
        }
        
        try:
            self.client.retrieve('reanalysis-era5-single-levels', request, filename)
            return filename
        except Exception as e:
            raise ConnectionError(f"Failed to download ERA5 data: {str(e)}")
    
    def get_temperature_from_cached_file(
        self, 
        cached_file: str, 
        latitude: float, 
        longitude: float, 
        target_date: datetime
    ) -> float:
        """
        Get temperature from a cached ERA5 file for a specific location and date.
        
        Args:
            cached_file: Path to the cached ERA5 NetCDF file
            latitude: Location latitude
            longitude: Location longitude
            target_date: Target date for temperature data
            
        Returns:
            Temperature in Fahrenheit at 12:00 UTC
        """
        try:
            with xr.open_dataset(cached_file) as ds:
                # Select the specific date
                target_time = target_date.replace(hour=12, minute=0, second=0, microsecond=0)
                
                # Extract temperature data for the location and time
                temp_data = ds.sel(
                    latitude=latitude,
                    longitude=longitude,
                    valid_time=target_time,
                    method='nearest'
                )['t2m'].values
                
                # Get the temperature (data is in Kelvin)
                temp_kelvin = float(temp_data.item())
                
                # Convert to Fahrenheit
                return self._kelvin_to_fahrenheit(temp_kelvin)
                
        except Exception as e:
            print(f"Error retrieving temperature from cached file: {str(e)}")
            raise

# Create a singleton instance
era5_client = ERA5Interface() 