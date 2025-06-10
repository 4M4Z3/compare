from google.cloud import bigquery, storage
from google.oauth2 import service_account
from datetime import datetime, timedelta
import os
import subprocess
import calendar
import logging
import sys
from time import time
import json

def setup_logger(name):
    """Setup logger."""
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    
    # Create handlers
    c_handler = logging.StreamHandler(sys.stdout)
    f_handler = logging.FileHandler(f'data/debug_{name.lower()}.log')
    
    # Create formatters and add it to handlers
    c_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    f_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    c_handler.setFormatter(c_format)
    f_handler.setFormatter(f_format)
    
    # Add handlers to the logger
    logger.addHandler(c_handler)
    logger.addHandler(f_handler)
    
    return logger

def process_day(date, project_id, bucket_name, credentials, logger):
    """Process one day of data using BigQuery → GCS → Local approach."""
    date_str = date.strftime('%Y%m%d')
    table_name = f"gencast_{date_str}"
    local_file = f"data/gencast_{date_str}.csv"
    dataset = "gencast_export_data"
    
    # Skip if file already exists
    if os.path.exists(local_file):
        logger.info(f"SKIPPED: {date_str} - file already exists")
        return "skipped"
    
    try:
        # Initialize clients
        bq_client = bigquery.Client(credentials=credentials, project=project_id)
        storage_client = storage.Client(credentials=credentials, project=project_id)
        bucket = storage_client.bucket(bucket_name)
        
        # Step 1: Run BigQuery query and save to temporary table
        logger.info(f"Running BigQuery query for {date_str}")
        query = f"""
        SELECT
            f.time AS forecast_time,
            ST_Y(t.geography) AS latitude,
            ST_X(t.geography) AS longitude,
            AVG(e.`2m_temperature`) AS temp_2m,
            STDDEV(e.`2m_temperature`) AS temp_2m_stddev,
            COUNT(e.`2m_temperature`) as ensemble_count
        FROM
            `ultra-task-456813-d5.weathernext_gen_forecasts.126478713_1_0` AS t,
            UNNEST(t.forecast) AS f,
            UNNEST(f.ensemble) AS e
        WHERE
            t.init_time = TIMESTAMP('{date.strftime('%Y-%m-%d')} 12:00:00')
        GROUP BY
            forecast_time, latitude, longitude
        ORDER BY 
            forecast_time, latitude, longitude
        """
        
        # Configure job
        job_config = bigquery.QueryJobConfig(
            destination=f"{project_id}.{dataset}.{table_name}",
            write_disposition="WRITE_TRUNCATE",
            maximum_bytes_billed=60 * 1024 * 1024 * 1024  # 60GB limit
        )
        
        # Run query
        query_job = bq_client.query(query, job_config=job_config)
        query_job.result()  # Wait for query to complete
        
        # Step 2: Export to GCS
        logger.info(f"Exporting to GCS for {date_str}")
        destination_uri = f"gs://{bucket_name}/temp/{table_name}/*.csv"
        
        extract_job = bq_client.extract_table(
            f"{project_id}.{dataset}.{table_name}",
            destination_uri,
            location="US"  # Adjust if your location is different
        )
        extract_job.result()  # Wait for export to complete
        
        # Step 3: Download from GCS using gsutil
        logger.info(f"Downloading from GCS for {date_str}")
        gcs_path = f"gs://{bucket_name}/temp/{table_name}/"
        os.makedirs("data", exist_ok=True)
        
        # Use gsutil for parallel download
        cmd = f"gsutil -m cp {gcs_path}* data/"
        subprocess.run(cmd, shell=True, check=True)
        
        # Step 4: Cleanup
        logger.info(f"Cleaning up temporary resources for {date_str}")
        
        # Delete temporary table
        bq_client.delete_table(f"{project_id}.{dataset}.{table_name}", not_found_ok=True)
        
        # Delete GCS files
        blobs = bucket.list_blobs(prefix=f"temp/{table_name}")
        for blob in blobs:
            blob.delete()
            
        # Verify file size
        file_size_mb = os.path.getsize(local_file) / (1024 * 1024)
        logger.info(f"""
========== DAY COMPLETE ==========
Date: {date_str}
File: {local_file}
Size: {file_size_mb:.2f} MB
=================================
""")
        
        return "success"
        
    except Exception as e:
        logger.error(f"""
!!!!!!!! DAY FAILED !!!!!!!!
Date: {date_str}
Error: {str(e)}
!!!!!!!!!!!!!!!!!!!!!!!!!!
""")
        # Log to failed_dates
        with open('data/failed_dates.txt', 'a') as f:
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            f.write(f"FAILED: {date_str} | Time: {current_time} | Error: {str(e)}\n")
        return "failed"

def download_gencast_2024():
    """Download GenCast data for April 2024."""
    # Create data directory
    os.makedirs('data', exist_ok=True)
    
    # Setup logging
    logger = setup_logger('gencast')
    
    # Load credentials
    credentials = service_account.Credentials.from_service_account_file(
        'service_acct.json',
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    
    # Get project details from credentials
    with open('service_acct.json') as f:
        project_id = json.load(f)['project_id']
    
    # Use existing bucket and dataset
    bucket_name = "gencast-export-bucket"
    dataset = "gencast_export_data"
    
    # Process April 2024
    month = 4  # April
    year = 2024
    _, num_days = calendar.monthrange(year, month)
    
    logger.info(f"""
====================================
Starting downloads for April 2024:
- Days: {num_days}
- Using BigQuery → GCS → Local approach
- Using bucket: {bucket_name}
- Using dataset: {dataset}
- Files saved in ./data/
- Debug logs in data/debug_*.log
====================================
""")
    
    # Process each day
    successful = 0
    skipped = 0
    failed = 0
    
    for day in range(1, num_days + 1):
        date = datetime(year, month, day)
        result = process_day(date, project_id, bucket_name, credentials, logger)
        
        if result == "success":
            successful += 1
        elif result == "skipped":
            skipped += 1
        else:
            failed += 1
            
        logger.info(f"""
Progress Update:
- Completed: {day}/{num_days} ({(day/num_days)*100:.1f}%)
- Successful: {successful}
- Skipped: {skipped}
- Failed: {failed}
""")
    
    logger.info(f"""
====================================
Download Complete!
- Total days: {num_days}
- Successful: {successful}
- Skipped: {skipped}
- Failed: {failed}
====================================
""")

if __name__ == "__main__":
    try:
        download_gencast_2024()
    except KeyboardInterrupt:
        print("\nDownload interrupted - progress saved")
        print("Run script again to continue from last successful download") 