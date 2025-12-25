import os
import time
from datetime import datetime, timedelta
import yaml
import logging

# Load config
with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

RETENTION_DAYS = config.get("retention_days", 7)

TARGET_DIRS = [
    "data/raw",
    "data/staging",
    "logs"
]

PRESERVE_KEYWORDS = ["summary", "report", "metadata"]

TODAY = datetime.now().date()

# Logging
logging.basicConfig(
    filename="logs/scheduler_activity.log",
    level=logging.INFO,
    format="%(asctime)s - CLEANUP - %(message)s"
)

def should_preserve(filename):
    return any(word in filename.lower() for word in PRESERVE_KEYWORDS)

def cleanup():
    cutoff_date = datetime.now() - timedelta(days=RETENTION_DAYS)

    for directory in TARGET_DIRS:
        if not os.path.exists(directory):
            continue

        for file in os.listdir(directory):
            file_path = os.path.join(directory, file)

            if not os.path.isfile(file_path):
                continue

            # Preserve important files
            if should_preserve(file):
                continue

            file_time = datetime.fromtimestamp(os.path.getmtime(file_path))

            # Preserve today's files
            if file_time.date() == TODAY:
                continue

            if file_time < cutoff_date:
                os.remove(file_path)
                logging.info(f"Deleted old file: {file_path}")

if __name__ == "__main__":
    cleanup()
    logging.info("Cleanup completed successfully")
