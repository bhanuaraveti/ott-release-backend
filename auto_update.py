#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Automated movie data updater with error handling and backup management.
This script runs daily via cron to fetch the latest movie data.
"""

import os
import sys
import json
import shutil
import subprocess
from datetime import datetime, timedelta
import logging

# Setup logging
LOG_DIR = os.path.join(os.path.dirname(__file__), 'logs')
os.makedirs(LOG_DIR, exist_ok=True)

log_file = os.path.join(LOG_DIR, f'update_{datetime.now().strftime("%Y%m%d")}.log')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

# Paths
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, 'data')
DATA_FILE = os.path.join(DATA_DIR, 'movies.json')
SCRAPER_SCRIPT = os.path.join(SCRIPT_DIR, 'scrapper.py')
VENV_PYTHON = '/Users/aravetibhanu/projects/virtualenvs/python3.10/bin/python'

# Backup retention period (30 days)
BACKUP_RETENTION_DAYS = 30


def cleanup_old_backups():
    """Remove backup files older than BACKUP_RETENTION_DAYS."""
    try:
        logger.info(f"Cleaning up backups older than {BACKUP_RETENTION_DAYS} days...")
        cutoff_date = datetime.now() - timedelta(days=BACKUP_RETENTION_DAYS)
        
        deleted_count = 0
        for filename in os.listdir(DATA_DIR):
            if filename.startswith('movies_backup_') and filename.endswith('.json'):
                filepath = os.path.join(DATA_DIR, filename)
                file_time = datetime.fromtimestamp(os.path.getmtime(filepath))
                
                if file_time < cutoff_date:
                    os.remove(filepath)
                    deleted_count += 1
                    logger.info(f"Deleted old backup: {filename}")
        
        logger.info(f"Cleanup complete. Deleted {deleted_count} old backup(s).")
    except Exception as e:
        logger.error(f"Error during backup cleanup: {e}")


def get_latest_backup():
    """Get the most recent backup file."""
    try:
        backups = [f for f in os.listdir(DATA_DIR) 
                  if f.startswith('movies_backup_') and f.endswith('.json')]
        
        if not backups:
            return None
        
        # Sort by filename (timestamp is in filename)
        backups.sort(reverse=True)
        return os.path.join(DATA_DIR, backups[0])
    except Exception as e:
        logger.error(f"Error getting latest backup: {e}")
        return None


def restore_from_backup():
    """Restore data from the latest backup if scraping fails."""
    try:
        latest_backup = get_latest_backup()
        if not latest_backup:
            logger.error("No backup files found for restoration!")
            return False
        
        logger.info(f"Restoring from backup: {latest_backup}")
        shutil.copy2(latest_backup, DATA_FILE)
        logger.info("Successfully restored from backup.")
        return True
    except Exception as e:
        logger.error(f"Error restoring from backup: {e}")
        return False


def create_safety_backup():
    """Create a safety backup before running the scraper."""
    try:
        if os.path.exists(DATA_FILE):
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            safety_backup = os.path.join(DATA_DIR, f"movies_safety_{timestamp}.json")
            shutil.copy2(DATA_FILE, safety_backup)
            logger.info(f"Created safety backup: {safety_backup}")
            return safety_backup
        return None
    except Exception as e:
        logger.error(f"Error creating safety backup: {e}")
        return None


def validate_data_file():
    """Validate that the data file is valid JSON and has movies."""
    try:
        with open(DATA_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        if not isinstance(data, list):
            logger.error("Data file is not a list!")
            return False
        
        if len(data) == 0:
            logger.error("Data file is empty!")
            return False
        
        # Check if movies have required fields
        required_fields = ['name', 'platform', 'available_on', 'type']
        sample_movie = data[0]
        for field in required_fields:
            if field not in sample_movie:
                logger.error(f"Missing required field: {field}")
                return False
        
        logger.info(f"Data validation passed. {len(data)} movies found.")
        return True
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in data file: {e}")
        return False
    except Exception as e:
        logger.error(f"Error validating data file: {e}")
        return False


def run_scraper():
    """Run the scraper script and return success status."""
    try:
        logger.info("Starting scraper...")
        
        # Run the scraper
        result = subprocess.run(
            [VENV_PYTHON, SCRAPER_SCRIPT],
            cwd=SCRIPT_DIR,
            capture_output=True,
            text=True,
            timeout=300  # 5 minute timeout
        )
        
        # Log output
        if result.stdout:
            logger.info(f"Scraper output:\n{result.stdout}")
        if result.stderr:
            logger.warning(f"Scraper stderr:\n{result.stderr}")
        
        if result.returncode != 0:
            logger.error(f"Scraper exited with code {result.returncode}")
            return False
        
        logger.info("Scraper completed successfully.")
        return True
    except subprocess.TimeoutExpired:
        logger.error("Scraper timed out after 5 minutes!")
        return False
    except Exception as e:
        logger.error(f"Error running scraper: {e}")
        return False


def main():
    """Main execution function."""
    logger.info("=" * 60)
    logger.info("Starting automated movie data update")
    logger.info("=" * 60)
    
    # Step 1: Create safety backup
    safety_backup = create_safety_backup()
    
    # Step 2: Run the scraper
    scraper_success = run_scraper()
    
    # Step 3: Validate the data
    if scraper_success:
        data_valid = validate_data_file()
        
        if not data_valid:
            logger.error("Data validation failed after scraping!")
            scraper_success = False
    
    # Step 4: Restore from backup if scraping failed
    if not scraper_success:
        logger.warning("Scraping or validation failed. Attempting to restore from backup...")
        if restore_from_backup():
            logger.info("Successfully restored from backup.")
        else:
            logger.error("Failed to restore from backup!")
            sys.exit(1)
    else:
        logger.info("Update completed successfully!")
        # Remove safety backup if update was successful
        if safety_backup and os.path.exists(safety_backup):
            os.remove(safety_backup)
            logger.info("Removed safety backup (update successful).")
    
    # Step 5: Cleanup old backups
    cleanup_old_backups()
    
    # Step 6: Cleanup old log files (keep 30 days)
    try:
        cutoff_date = datetime.now() - timedelta(days=BACKUP_RETENTION_DAYS)
        for filename in os.listdir(LOG_DIR):
            if filename.startswith('update_') and filename.endswith('.log'):
                filepath = os.path.join(LOG_DIR, filename)
                file_time = datetime.fromtimestamp(os.path.getmtime(filepath))
                if file_time < cutoff_date:
                    os.remove(filepath)
                    logger.info(f"Deleted old log: {filename}")
    except Exception as e:
        logger.error(f"Error cleaning up old logs: {e}")
    
    logger.info("=" * 60)
    logger.info("Automated update completed")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
