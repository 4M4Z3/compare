from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, timedelta
import os
from multiprocessing import Pool, cpu_count
import calendar
import logging
import sys
from time import time

def setup_logger(month_name):
    """Setup logger for a specific month process."""
    logger = logging.getLogger(f"gencast_{month_name}")
    logger.setLevel(logging.DEBUG)
    
    # Create handlers
    c_handler = logging.StreamHandler(sys.stdout)
    f_handler = logging.FileHandler(f'data/debug_{month_name.lower()}.log')
    c_handler.setLevel(logging.INFO)
    f_handler.setLevel(logging.DEBUG)
    
    # Create formatters and add it to handlers
    c_format = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
    f_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    c_handler.setFormatter(c_format)
    f_handler.setFormatter(f_format)
    
    # Add handlers to the logger
    logger.addHandler(c_handler)
    logger.addHandler(f_handler)
    
    return logger

def download_month(month_tuple):
    """Download all days for a specific month."""
    month, year = month_tuple
    month_name = calendar.month_name[month]
    logger = setup_logger(month_name)
    
    logger.info(f"=== Starting process for {month_name} {year} ===")
    
    try:
        # Setup
        logger.debug("Initializing service account credentials")
        credentials = service_account.Credentials.from_service_account_file(
            'service_acct.json',
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        
        logger.debug("Creating BigQuery client")
        client = bigquery.Client(credentials=credentials)
        
        # Create data directory
        logger.debug("Creating data directory if it doesn't exist")
        os.makedirs('data', exist_ok=True)
        
        # Get number of days in month
        _, num_days = calendar.monthrange(year, month)
        
        # Generate dates for this month
        start_date = datetime(year, month, 1)
        end_date = datetime(year, month, num_days)
        current_date = start_date
        
        logger.info(f"Will process {num_days} days for {month_name}")
        completed_days = 0
        
        while current_date <= end_date:
            date_str = current_date.strftime('%Y-%m-%d')
            filename = f"data/gencast_{current_date.strftime('%Y%m%d')}.csv"
            
            # Skip if file already exists
            if os.path.exists(filename):
                logger.info(f"SKIPPED: {date_str} - file already exists ({completed_days + 1}/{num_days})")
                current_date += timedelta(days=1)
                completed_days += 1
                continue
                
            logger.info(f"START: Processing {date_str} ({completed_days + 1}/{num_days})")
            start_time = time()
            
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
                # Run query
                logger.debug(f"Running BigQuery job for {date_str}")
                job = client.query(query)
                
                logger.debug("Waiting for query results")
                results = job.result()
                
                logger.debug("Converting results to dataframe")
                df = results.to_dataframe()
                
                logger.debug(f"Saving results to {filename}")
                df.to_csv(filename, index=False)
                
                # Calculate processing time and stats
                processing_time = time() - start_time
                gb_processed = job.total_bytes_processed / 1024 / 1024 / 1024
                
                # Log completion with prominent formatting
                logger.info(f"""
========== DAY COMPLETE ==========
Date: {date_str} ({completed_days + 1}/{num_days})
Time taken: {processing_time:.1f} seconds
Data processed: {gb_processed:.2f} GB
Rows saved: {df.shape[0]:,}
File: {filename}
=================================
""")
                
                completed_days += 1
                
            except Exception as e:
                logger.error(f"""
!!!!!!!! DAY FAILED !!!!!!!!
Date: {date_str}
Error: {str(e)}
!!!!!!!!!!!!!!!!!!!!!!!!!!
""")
                # Create error log
                with open('data/error_log.txt', 'a') as f:
                    f.write(f"{date_str}: {str(e)}\n")
            
            current_date += timedelta(days=1)
        
        # Log month completion
        logger.info(f"""
************************************************
MONTH COMPLETE: {month_name} {year}
Total days processed: {completed_days}/{num_days}
************************************************
""")
        return f"Completed {month_name} {year}"
        
    except Exception as e:
        logger.error(f"""
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
MONTH FAILED: {month_name} {year}
Error: {str(e)}
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
""")
        return f"Failed {month_name} {year}: {str(e)}"

def download_gencast_2024_parallel():
    # Setup main logger
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('data/main_process.log')
        ]
    )
    logger = logging.getLogger('main')
    
    # Generate list of months to process (March to December)
    months_to_process = [(month, 2024) for month in range(3, 13)]
    
    # Calculate total data size
    total_months = len(months_to_process)
    total_gb = total_months * 30 * 47  # approximate GB per month
    
    logger.info(f"""
====================================
Starting parallel downloads for:
- Months: {total_months}
- Daily data: ~47GB
- Total data: ~{total_gb}GB
- Parallel processes: {min(cpu_count(), total_months)}
====================================
""")
    
    # Create pool of workers
    with Pool(min(cpu_count(), total_months)) as pool:
        try:
            # Map months to worker processes
            results = pool.map_async(download_month, months_to_process)
            
            # Wait for all processes to complete
            completed_results = results.get()
            
            # Print results
            for result in completed_results:
                logger.info(result)
                
        except KeyboardInterrupt:
            logger.warning("""
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
INTERRUPTED - Progress saved
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
""")
            pool.terminate()
            pool.join()
            return
        
        except Exception as e:
            logger.error(f"""
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
PARALLEL PROCESSING ERROR
Error: {str(e)}
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
""")
            pool.terminate()
            pool.join()
            return

if __name__ == "__main__":
    print("""
====================================
GenCast 2024 Parallel Download
====================================
- Starting from March 2024
- Files saved in ./data/
- Debug logs in data/debug_*.log
- Press Ctrl+C to stop (progress saved)
====================================
""")
    
    try:
        download_gencast_2024_parallel()
        print("\nAll downloads complete!")
    except KeyboardInterrupt:
        print("\nDownload interrupted - progress saved")
        print("Run script again to continue from last successful download") 