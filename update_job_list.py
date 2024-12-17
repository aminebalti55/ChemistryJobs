import sys
import os

# Add the project directory to the Python path
project_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_dir)

from job_scraper import initialize_db, update_jobs
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s: %(message)s',
    handlers=[
        logging.StreamHandler(),  # Prints to console
        logging.FileHandler('job_update.log')  # Optional: log to file
    ]
)

def main():
    try:
        logging.info("Starting job update process...")
        
        # Initialize database
        initialize_db()
        logging.info("Database initialized successfully.")
        
        # Run job update
        update_jobs()
        logging.info("Job update completed successfully.")
    
    except Exception as e:
        logging.error(f"Error during job update: {e}", exc_info=True)

if __name__ == "__main__":
    main()