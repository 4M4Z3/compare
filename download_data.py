from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, timedelta
import os
from multiprocessing import Pool, cpu_count, Manager, Lock
from concurrent.futures import ThreadPoolExecutor, as_completed
import calendar
import logging
import sys
from time import time, sleep
import threading
from google.api_core import retry

# Global counter for tracking total progress
class GlobalProgress:
    def __init__(self, total_days):
        self.manager = Manager()
        self.lock = self.manager.Lock()
        self.completed = self.manager.Value('i', 0)
        self.successful = self.manager.Value('i', 0)
        self.skipped = self.manager.Value('i', 0)
        self.failed = self.manager.Value('i', 0)
        self.total_days = total_days
        
    def update(self, status):
        with self.lock:
            self.completed.value += 1
            if status == "success":
                self.successful.value += 1
            elif status == "skipped":
                self.skipped.value += 1
            elif status == "failed":
                self.failed.value += 1
                
            # Calculate percentage
            percentage = (self.completed.value / self.total_days) * 100
            
            # Print progress bar and stats
            bar_length = 50
            filled_length = int(bar_length * self.completed.value // self.total_days)
            bar = '=' * filled_length + '-' * (bar_length - filled_length)
            
            print(f"""
=== OVERALL PROGRESS [{bar}] {percentage:.1f}% ===
Completed: {self.completed.value}/{self.total_days}
✓ Successful: {self.successful.value}
↷ Skipped: {self.skipped.value}
✗ Failed: {self.failed.value}
==========================================
""")

# Global progress tracker
global_progress = None

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

# Custom retry decorator for BigQuery operations
def retry_if_internal_error(exception):
    """Return True if we should retry (in this case when it's an internal error), False otherwise"""
    return isinstance(exception, Exception) and "500" in str(exception)

@retry.Retry(predicate=retry_if_internal_error, initial=1.0, maximum=60.0, multiplier=2.0, deadline=600.0)
def execute_query(client, query, job_config, logger, thread_name):
    """Execute BigQuery query with retry logic"""
    logger.debug(f"[{thread_name}] Running BigQuery job")
    job = client.query(query, job_config=job_config)
    logger.debug(f"[{thread_name}] Waiting for query results")
    return job.result()

def download_day(args):
    """Download data for a specific day."""
    date, client, logger = args
    date_str = date.strftime('%Y-%m-%d')
    filename = f"data/gencast_{date.strftime('%Y%m%d')}.csv"
    thread_name = threading.current_thread().name
    
    # Skip if file already exists
    if os.path.exists(filename):
        logger.debug(f"[{thread_name}] SKIPPED: {date_str} - file already exists")
        global_progress.update("skipped")
        return "skipped", date_str
        
    logger.debug(f"[{thread_name}] START: Processing {date_str}")
    start_time = time()
    
    # Set maximum bytes limit (60GB)
    job_config = bigquery.QueryJobConfig(
        maximum_bytes_billed=60 * 1024 * 1024 * 1024  # 60GB in bytes
    )
    
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
        # Run query with retry logic
        results = execute_query(client, query, job_config, logger, thread_name)
        
        logger.debug(f"[{thread_name}] Converting results to dataframe")
        df = results.to_dataframe()
        
        logger.debug(f"[{thread_name}] Saving results to {filename}")
        df.to_csv(filename, index=False)
        
        # Log file size
        file_size_gb = os.path.getsize(filename) / (1024 * 1024 * 1024)
        
        # Calculate processing time and stats
        processing_time = time() - start_time
        gb_processed = results.total_bytes_processed / 1024 / 1024 / 1024
        
        # Log completion with prominent formatting
        logger.debug(f"""
[{thread_name}] ========== DAY COMPLETE ==========
Date: {date_str}
Time taken: {processing_time:.1f} seconds
Data scanned: {gb_processed:.2f} GB
Result file size: {file_size_gb:.2f} GB
Rows saved: {df.shape[0]:,}
File: {filename}
=================================
""")
        global_progress.update("success")
        return "success", date_str
        
    except Exception as e:
        error_msg = f"""
[{thread_name}] !!!!!!!! DAY FAILED !!!!!!!!
Date: {date_str}
Error: {str(e)}
!!!!!!!!!!!!!!!!!!!!!!!!!!
"""
        logger.error(error_msg)
        
        # Save failed date with timestamp and error
        with open('data/failed_dates.txt', 'a') as f:
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            f.write(f"FAILED: {date_str} | Time: {current_time} | Error: {str(e)}\n")
            
        global_progress.update("failed")
        return "failed", date_str

def download_month(month_tuple):
    """Download all days for a specific month using thread pool."""
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
        
        # Get number of days in month
        _, num_days = calendar.monthrange(year, month)
        
        # Generate dates for this month
        dates = [datetime(year, month, day) for day in range(1, num_days + 1)]
        
        # Prepare arguments for each day
        day_args = [(date, client, logger) for date in dates]
        
        logger.info(f"Starting thread pool with {min(num_days, 10)} threads for {num_days} days")
        
        # Create thread pool and process days
        with ThreadPoolExecutor(max_workers=min(10, num_days)) as executor:
            # Submit all days to thread pool
            future_to_date = {executor.submit(download_day, args): args[0] for args in day_args}
            
            # Track completion
            month_completed = 0
            month_success = 0
            month_skipped = 0
            month_failed = 0
            
            # Process completed tasks as they finish
            for future in as_completed(future_to_date):
                date = future_to_date[future]
                try:
                    status, date_str = future.result()
                    month_completed += 1
                    
                    if status == "success":
                        month_success += 1
                    elif status == "skipped":
                        month_skipped += 1
                    else:
                        month_failed += 1
                        
                    # Log month progress (debug level to avoid cluttering main output)
                    logger.debug(f"""
{month_name} Progress:
- Completed: {month_completed}/{num_days} ({(month_completed/num_days)*100:.1f}%)
- Successful: {month_success}
- Skipped: {month_skipped}
- Failed: {month_failed}
""")
                except Exception as e:
                    logger.error(f"Error processing {date.strftime('%Y-%m-%d')}: {str(e)}")
                    month_failed += 1
        
        # Log month completion
        logger.info(f"""
************************************************
MONTH COMPLETE: {month_name} {year}
Total days: {num_days}
Successful: {month_success}
Skipped: {month_skipped}
Failed: {month_failed}
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
    # Create data directory first
    os.makedirs('data', exist_ok=True)
    
    # Create or clear the failed_dates.txt file
    with open('data/failed_dates.txt', 'w') as f:
        f.write("=== Failed Downloads Log ===\n")
        f.write("Format: FAILED: YYYY-MM-DD | Time: YYYY-MM-DD HH:MM:SS | Error: error_message\n")
        f.write("===============================================\n\n")
    
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
    
    # Generate list of months to process (March only)
    months_to_process = [(3, 2024)]  # Just March
    
    # Calculate total days
    total_days = calendar.monthrange(2024, 3)[1]  # Days in March
    
    # Initialize global progress tracker
    global global_progress
    global_progress = GlobalProgress(total_days)
    
    # Calculate total data size
    total_months = len(months_to_process)
    total_gb = total_months * 30 * 47  # approximate GB per month
    
    num_processes = min(cpu_count(), total_months)
    
    logger.info(f"""
====================================
Starting parallel downloads for:
- Total Days: {total_days}
- Month: March 2024
- Daily data: ~47GB
- Total data: ~{total_gb}GB
- Parallel processes: {num_processes}
- Threads per process: 10
====================================
""")
    
    # Create pool of workers
    with Pool(num_processes) as pool:
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
- Using multiprocessing + multithreading
- Press Ctrl+C to stop (progress saved)
====================================
""")
    
    try:
        download_gencast_2024_parallel()
        print("\nAll downloads complete!")
    except KeyboardInterrupt:
        print("\nDownload interrupted - progress saved")
        print("Run script again to continue from last successful download") 