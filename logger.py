"""
logger.py
---------
Logging setup using Python's logging module.
Every run writes a timestamped log file.

Usage:
    from logger import get_logger
    logger = get_logger(__name__)
    logger.info("Your message here")
"""

import os
import logging
from datetime import datetime

# -------------------------------------------------------------------
# Configuration
# -------------------------------------------------------------------
LOG_DIR    = "./logs"                                        # folder for log files
LOG_LEVEL  = logging.DEBUG                                   # log everything
TIMESTAMP  = datetime.now().strftime("%Y%m%d_%H%M%S")
LOG_FILE   = os.path.join(LOG_DIR, f"run_{TIMESTAMP}.log")  # e.g. run_20240101_103000.log


def setup_logger(
    name      : str  = "celonis_logger",
    log_file  : str  = None,
    log_level : int  = LOG_LEVEL,
    log_dir   : str  = LOG_DIR
) -> logging.Logger:
    """
    Set up and return a logger that writes to both terminal and a log file.

    Args:
        name      : Name of the logger.
        log_file  : Path to the log file. Auto-generated if not provided.
        log_level : Logging level. Default is DEBUG (logs everything).
        log_dir   : Directory to save log files. Default is './logs'.

    Returns:
        logging.Logger : Configured logger object.
    """

    # Step 1 – Create log directory if it doesn't exist
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # Step 2 – Use provided log file or generate timestamped one
    log_file = log_file or LOG_FILE

    # Step 3 – Create logger
    logger = logging.getLogger(name)
    logger.setLevel(log_level)

    # Avoid adding duplicate handlers if logger already exists
    if logger.handlers:
        return logger

    # Step 4 – Define log format
    formatter = logging.Formatter(
        fmt   = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt = "%Y-%m-%d %H:%M:%S"
    )

    # Step 5 – File handler — writes logs to file
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(log_level)
    file_handler.setFormatter(formatter)

    # Step 6 – Console handler — prints logs to terminal
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)   # only INFO and above in terminal
    console_handler.setFormatter(formatter)

    # Step 7 – Add both handlers to logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger


def get_logger(name: str = "celonis_logger") -> logging.Logger:
    """
    Get an existing logger or create a new one.

    Args:
        name : Name of the logger. Use __name__ for module-level logging.

    Returns:
        logging.Logger : Configured logger object.
    """
    return setup_logger(name)


# -------------------------------------------------------------------
# Test the logger
# -------------------------------------------------------------------
def main():
    logger = get_logger("test_logger")

    print(f"\nLog file created at: {LOG_FILE}\n")

    # Test all log levels
    logger.debug("DEBUG   — Detailed info for diagnosing problems")
    logger.info("INFO    — General info, script is running normally")
    logger.warning("WARNING — Something unexpected but not critical")
    logger.error("ERROR   — Something failed, needs attention")
    logger.critical("CRITICAL — Serious error, script may stop")

    print(f"\nAll logs written to: '{LOG_FILE}'")


if __name__ == "__main__":
    main()