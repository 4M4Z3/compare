import xarray as xr
import sys

def inspect_netcdf(file_path):
    """
    Inspect the structure of a NetCDF file.
    
    Args:
        file_path: Path to the NetCDF file
    """
    try:
        # Open the dataset
        ds = xr.open_dataset(file_path)
        
        print("\n=== Dataset Information ===")
        print(f"File: {file_path}")
        print("\n=== Dimensions ===")
        for dim, size in ds.dims.items():
            print(f"{dim}: {size}")
            
        print("\n=== Variables ===")
        for var_name, var in ds.variables.items():
            print(f"\n{var_name}:")
            print(f"  Shape: {var.shape}")
            print(f"  Dimensions: {var.dims}")
            print(f"  Attributes: {var.attrs}")
            
        print("\n=== Global Attributes ===")
        for attr_name, attr_value in ds.attrs.items():
            print(f"{attr_name}: {attr_value}")
            
        # Close the dataset
        ds.close()
        
    except Exception as e:
        print(f"Error reading NetCDF file: {str(e)}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python inspect_nc.py <netcdf_file>")
        sys.exit(1)
        
    inspect_netcdf(sys.argv[1]) 