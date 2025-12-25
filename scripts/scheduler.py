import schedule
import time
import subprocess
import logging
import os
import yaml
from datetime import datetime

LOCK_FILE = "pipeline.lock"

# Load config
with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

RUN_TIME = config.get("pipeline_run_time", "02:00")  # default 2 AM

# Logging
logging.basicConfig(
    filename="logs/scheduler_activity.log",
    level=logging.INFO,
    format="%(asctime)s - SCHEDULER - %(message)s"
)

def is_pipeline_running():
    return os.path.exists(LOCK_FILE)

def create_lock():
    with open(LOCK_FILE, "w") as f:
        f.write(str(os.getpid()))

def remove_lock():
    if os.path.exists(LOCK_FILE):
        os.remove(LOCK_FILE)

def run_pipeline():
    if is_pipeline_running():
        logging.warning("Pipeline already running. Skipping execution.")
        return

    try:
        logging.info("Pipeline execution started")
        create_lock()

        # CHANGE THIS FILE NAME IF YOUR PIPELINE FILE IS DIFFERENT
        result = subprocess.run(
            ["python", "scripts/pipeline_orchestrator.py"],
            capture_output=True,
            text=True
        )

        if result.returncode == 0:
            logging.info("Pipeline execution SUCCESS")

            # Run cleanup after success
            subprocess.run(["python", "scripts/cleanup_old_data.py"])
            logging.info("Cleanup executed after pipeline success")

        else:
            logging.error("Pipeline FAILED")
            logging.error(result.stderr)

    except Exception as e:
        logging.error(f"Scheduler error: {str(e)}")

    finally:
        remove_lock()

def start_scheduler():
    logging.info("Scheduler started")
    schedule.every().day.at(RUN_TIME).do(run_pipeline)

    while True:
        schedule.run_pending()
        time.sleep(30)

if __name__ == "__main__":
    start_scheduler()
