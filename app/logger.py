# logger.py

import logging
import os
from datetime import datetime

# Create a logs folder if it doesn't exist
LOG_DIR = "logs"
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

# Date-based log file naming
timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

# File paths
success_log_path = os.path.join(LOG_DIR, f"success_{timestamp}.log")
fail_log_path = os.path.join(LOG_DIR, f"fail_{timestamp}.log")

# Success logger
success_logger = logging.getLogger("success_logger")
success_handler = logging.FileHandler(success_log_path)
success_handler.setFormatter(logging.Formatter("%(asctime)s - %(message)s"))
success_logger.addHandler(success_handler)
success_logger.setLevel(logging.INFO)

# Fail logger
fail_logger = logging.getLogger("fail_logger")
fail_handler = logging.FileHandler(fail_log_path)
fail_handler.setFormatter(logging.Formatter("%(asctime)s - %(message)s"))
fail_logger.addHandler(fail_handler)
fail_logger.setLevel(logging.ERROR)

# Expose paths for upload later
def get_log_paths():
    return {
        "success": success_log_path,
        "fail": fail_log_path
    }
