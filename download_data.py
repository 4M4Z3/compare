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
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import shutil
from typing import List, Tuple
from functools import partial

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

def download_chunk(blob, temp_dir: str, logger) -> Tuple[str, int]:
    """Download a single chunk from GCS."""
    try:
        chunk_path = os.path.join(temp_dir, os.path.basename(blob.name))
        blob.download_to_filename(chunk_path)
        return chunk_path, 0
    except Exception as e:
        logger.error(f"Error downloading chunk {blob.name}: {str(e)}")
        return "", 1

def process_day(date, project_id, bucket_name, credentials, logger):
    """Process one day of data using BigQuery → GCS → Local approach."""
    print(f"\n{'='*50}")
    print(f"STARTING PROCESS FOR DATE: {date.strftime('%Y-%m-%d')}")
    print(f"{'='*50}")
    
    date_str = date.strftime('%Y%m%d')
    table_name = f"gencast_{date_str}"
    final_file = f"data/gencast_{date_str}.csv"
    temp_dir = f"data/temp_{date_str}"
    dataset = "gencast_export_data"
    
    # Skip if file already exists
    if os.path.exists(final_file):
        print(f"File already exists for {date_str} - SKIPPING")
        logger.info(f"SKIPPED: {date_str} - file already exists")
        return "skipped"
    
    try:
        print("\n1️⃣ Initializing clients...")
        # Initialize clients
        bq_client = bigquery.Client(credentials=credentials, project=project_id)
        storage_client = storage.Client(credentials=credentials, project=project_id)
        bucket = storage_client.bucket(bucket_name)
        print("✅ Clients initialized successfully")
        
        # Create temp directory for chunks
        os.makedirs(temp_dir, exist_ok=True)
        print(f"📁 Created temporary directory: {temp_dir}")
        
        # Step 1: Run BigQuery query and save to temporary table
        print("\n2️⃣ Running BigQuery query...")
        print(f"📊 Querying data for {date.strftime('%Y-%m-%d')}")
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
        print("✅ BigQuery query completed successfully")
        
        # Step 2: Export to GCS
        print("\n3️⃣ Exporting to Google Cloud Storage...")
        logger.info(f"Exporting to GCS for {date_str}")
        destination_uri = f"gs://{bucket_name}/temp/{table_name}/*.csv"
        
        extract_job = bq_client.extract_table(
            f"{project_id}.{dataset}.{table_name}",
            destination_uri,
            location="US"  # Adjust if your location is different
        )
        extract_job.result()  # Wait for export to complete
        print("✅ Export to GCS completed successfully")
        
        # Step 3: Download chunks from GCS using parallel downloads
        print("\n4️⃣ Downloading chunks from GCS...")
        logger.info(f"Downloading chunks from GCS for {date_str}")
        prefix = f"temp/{table_name}/"
        blobs = list(bucket.list_blobs(prefix=prefix))
        
        if not blobs:
            raise Exception(f"No chunks found in GCS with prefix: {prefix}")
            
        print(f"📦 Found {len(blobs)} chunks to download")
        
        # Download chunks in parallel using ThreadPoolExecutor
        chunk_paths = []
        failed_downloads = 0
        
        with ThreadPoolExecutor(max_workers=min(10, len(blobs))) as executor:
            download_func = partial(download_chunk, temp_dir=temp_dir, logger=logger)
            futures = [executor.submit(download_func, blob) for blob in blobs]
            
            # Process results as they complete
            for i, future in enumerate(futures, 1):
                chunk_path, failed = future.result()
                if failed:
                    failed_downloads += 1
                else:
                    chunk_paths.append(chunk_path)
                    print(f"📥 Downloaded chunk {i}/{len(blobs)}: {os.path.basename(chunk_path)}")
        
        if failed_downloads > 0:
            raise Exception(f"Failed to download {failed_downloads} chunks")
            
        print("✅ All chunks downloaded successfully")
        
        # Step 4: Combine chunks
        print("\n5️⃣ Combining chunks...")
        logger.info(f"Combining chunks for {date_str}")
        chunk_files = sorted([f for f in os.listdir(temp_dir) if f.endswith('.csv')])
        
        if not chunk_files:
            raise Exception("No chunks downloaded")
            
        print(f"📦 Found {len(chunk_files)} chunks to combine")
            
        # Write header from first chunk
        print("📝 Reading header from first chunk...")
        with open(os.path.join(temp_dir, chunk_files[0]), 'r') as first_chunk:
            header = first_chunk.readline()
            
        print(f"📝 Creating final file: {final_file}")
        
        # Process chunks in parallel
        def process_chunk(chunk_file: str, is_first: bool = False) -> Tuple[int, List[str]]:
            chunk_path = os.path.join(temp_dir, chunk_file)
            chunk_rows = []
            row_count = 0
            
            with open(chunk_path, 'r') as infile:
                # Skip header if not first chunk
                if not is_first:
                    next(infile)
                # Read all lines
                for line in infile:
                    chunk_rows.append(line)
                    row_count += 1
                    
            return row_count, chunk_rows
        
        total_rows = 0
        with open(final_file, 'w') as outfile:
            # Write header
            outfile.write(header)
            
            # Process chunks in parallel
            with ThreadPoolExecutor(max_workers=min(10, len(chunk_files))) as executor:
                futures = []
                for i, chunk_file in enumerate(chunk_files):
                    futures.append(executor.submit(process_chunk, chunk_file, i == 0))
                
                # Write results in order
                for i, future in enumerate(futures, 1):
                    rows, chunk_data = future.result()
                    outfile.writelines(chunk_data)
                    total_rows += rows
                    print(f"   ✓ Added {rows:,} rows from chunk {i}")
                        
        print(f"✅ Successfully combined all chunks. Total rows: {total_rows:,}")
        
        # Step 5: Cleanup
        print("\n6️⃣ Cleaning up...")
        logger.info(f"Cleaning up temporary resources for {date_str}")
        
        # Delete temporary table
        print("🗑️ Deleting temporary BigQuery table...")
        bq_client.delete_table(f"{project_id}.{dataset}.{table_name}", not_found_ok=True)
        
        # Delete GCS files
        print("🗑️ Deleting temporary files from GCS...")
        for blob in blobs:
            blob.delete()
            
        # Delete temp directory with chunks
        print("🗑️ Deleting temporary directory...")
        shutil.rmtree(temp_dir)
            
        # Verify final file size
        file_size_mb = os.path.getsize(final_file) / (1024 * 1024)
        print(f"\n{'='*50}")
        print(f"✨ PROCESS COMPLETE FOR {date.strftime('%Y-%m-%d')} ✨")
        print(f"📊 Final Statistics:")
        print(f"   - Output file: {final_file}")
        print(f"   - File size: {file_size_mb:.2f} MB")
        print(f"   - Chunks combined: {len(chunk_files)}")
        print(f"   - Total rows: {total_rows:,}")
        print(f"{'='*50}\n")
        
        logger.info(f"""
========== DAY COMPLETE ==========
Date: {date_str}
File: {final_file}
Final Size: {file_size_mb:.2f} MB
Chunks Combined: {len(chunk_files)}
Total Rows: {total_rows:,}
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