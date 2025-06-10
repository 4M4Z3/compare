from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, timedelta
import os

def download_gencast_2024():
    # Setup
    credentials = service_account.Credentials.from_service_account_file(
        'service_acct.json',
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    client = bigquery.Client(credentials=credentials)
    
    # Create data directory
    os.makedirs('data', exist_ok=True)
    
    # Generate all dates in 2024
    start_date = datetime(2024, 1, 1)
    end_date = datetime(2024, 12, 31)
    current_date = start_date
    
    while current_date <= end_date:
        date_str = current_date.strftime('%Y-%m-%d')
        filename = f"data/gencast_{current_date.strftime('%Y%m%d')}.csv"
        
        # Skip if file already exists
        if os.path.exists(filename):
            print(f"Skipping {date_str} - file already exists")
            current_date += timedelta(days=1)
            continue
            
        print(f"Downloading {date_str}...")
        
        query = f"""
        SELECT
          f.time AS forecast_time,
          ST_Y(t.geography) AS latitude,
          ST_X(t.geography) AS longitude,
          AVG(e.`2m_temperature`) AS temp_2m,
          STDDEV(e.`2m_temperature`) AS temp_2m_stddev
        FROM
          `ultra-task-456813-d5.weathernext_gen_forecasts.126478713_1_0` AS t,
          UNNEST(t.forecast) AS f,
          UNNEST(f.ensemble) AS e
        WHERE
          t.init_time = TIMESTAMP('{date_str} 12:00:00')
        GROUP BY
          forecast_time, latitude, longitude
        ORDER BY 
          forecast_time, latitude, longitude
        """
        
        try:
            # Run query and save directly to CSV
            job = client.query(query)
            job.result().to_dataframe().to_csv(filename, index=False)
            print(f"✓ Saved {filename}")
        except Exception as e:
            print(f"✗ Error on {date_str}: {str(e)}")
            # Create error log
            with open('data/error_log.txt', 'a') as f:
                f.write(f"{date_str}: {str(e)}\n")
        
        current_date += timedelta(days=1)

if __name__ == "__main__":
    print("Starting GenCast 2024 downloads...")
    print("Files will be saved in ./data/")
    print("Each file will be ~47GB of processed data")
    print("Total for year: ~17.15TB")
    print("Press Ctrl+C to stop at any time - script can be resumed\n")
    
    try:
        download_gencast_2024()
        print("\nDownload complete!")
    except KeyboardInterrupt:
        print("\nDownload interrupted - progress saved")
        print("Run script again to continue from last successful download") 