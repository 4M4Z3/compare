from datetime import datetime
from era5_interface import era5_client

def test_era5_connection():
    """Test ERA5 connection and data retrieval for a single coordinate and month."""
    try:
        # Test with a single coordinate (0.25 degree resolution)
        lat, lon = 40.75, -74.00  # Example: New York City area
        year, month = 2024, 1  # January 2024
        
        print(f"Testing ERA5 connection...")
        print(f"Retrieving temperature for coordinate ({lat}, {lon}) for {year}-{month:02d}")
        
        # Download one month of data
        output_file = "era5_test_data.nc"
        era5_client.download_single_month(
            year=year,
            month=month,
            output_file=output_file,
            area=[lat + 0.125, lon - 0.125, lat - 0.125, lon + 0.125],  # 0.25 degree box
            data_format='netcdf'
        )
        
        # Get temperature for the specific coordinate
        date = datetime(year, month, 1)
        temp = era5_client.get_temperature(lat, lon, date)
        print(f"Successfully retrieved temperature: {temp:.1f}°F")
        print("ERA5 setup is working correctly!")
        
    except Exception as e:
        print(f"Error testing ERA5 connection: {str(e)}")
        print("Please check your ~/.cdsapirc file and ensure you have accepted the ERA5 terms of use.")

if __name__ == "__main__":
    test_era5_connection() 