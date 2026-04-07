"""
config.py
---------
Central configuration file that holds all credentials and settings.
Import this file in any script to access credentials and settings.

Usage:
    from config import config
"""

import os
from dotenv import load_dotenv

# Load credentials from .env
load_dotenv()


config = {

    # -------------------------------------------------------------------
    # Celonis Credentials
    # -------------------------------------------------------------------
    "BASE_URL"  : os.getenv("BASE_URL"),
    "API_TOKEN" : os.getenv("API_TOKEN"),
    "KEY_TYPE"  : os.getenv("KEY_TYPE", "USER_KEY"),   # default USER_KEY

    # -------------------------------------------------------------------
    # Data Pool & Model Settings
    # -------------------------------------------------------------------
    "MY_POOL"   : os.getenv("MY_POOL",  "02a. Catalog Extension - SAP ECC"),
    "MY_MODEL"  : os.getenv("MY_MODEL", "perspective_custom_ProcurementSAPECC"),
    "MY_TABLE"  : os.getenv("MY_TABLE", "o_celonis_GoodsReceipt"),

    # -------------------------------------------------------------------
    # Knowledge Model Settings
    # -------------------------------------------------------------------
    "MY_SPACE"   : os.getenv("MY_SPACE",   "Your Space Name"),
    "MY_PACKAGE" : os.getenv("MY_PACKAGE", "Your Package Name"),
    "MY_KM"      : os.getenv("MY_KM",      "Your Knowledge Model"),

    # -------------------------------------------------------------------
    # Export Settings
    # -------------------------------------------------------------------
    "OUTPUT_DIR"  : os.getenv("OUTPUT_DIR", "./output"),    # folder for CSV files
    "ROW_LIMIT"   : int(os.getenv("ROW_LIMIT", 10)),        # default row limit

}


def validate_config():
    """
    Validate that all required credentials are set.
    Call this at the start of any script to catch missing values early.
    """
    required = ["BASE_URL", "API_TOKEN"]
    missing  = [key for key in required if not config.get(key)]

    if missing:
        raise EnvironmentError(
            f"[ERROR] Missing required config values: {missing}\n"
            f"  Fix : Add them to your .env file:\n"
            + "\n".join(f"        {key}=your_value" for key in missing)
        )

    print("  Config validation passed!\n")


def print_config():
    """Print current config values — masks the API token for security."""
    print("=" * 50)
    print("  Current Configuration")
    print("=" * 50)
    for key, value in config.items():
        # Mask API token for security
        if "TOKEN" in key and value:
            display = value[:6] + "****" + value[-4:]
        else:
            display = value
        print(f"  {key:<15} : {display}")
    print("=" * 50 + "\n")


if __name__ == "__main__":
    validate_config()
    print_config()