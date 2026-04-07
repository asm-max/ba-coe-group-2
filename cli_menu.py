"""
cli_menu.py
-----------
A simple CLI menu that asks the user what to do
and runs the right function.

Integrates:
    - reusable_connection.py  : Celonis connection
    - config.py               : Credentials and settings
    - export_helper.py        : Export query results to CSV
    - fetch_KPIs.py           : Fetch KPI values
    - logger.py               : Logging every run

Usage:
    python3 cli_menu.py
"""

import os
import sys
from dotenv import load_dotenv

# -----------------------------------------------------------------------
# Add current folder to path so all local files can be imported
# -----------------------------------------------------------------------
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from logger             import get_logger
from config             import config, validate_config
from reusable_connection import get_connection
from export_helper      import export_to_csv, run_query_and_export
from fetch_kpis         import fetch_and_save_kpi_values

import pycelonis.pql as pql

# Load credentials
load_dotenv()

# Set up logger
logger = get_logger("cli_menu")


# -----------------------------------------------------------------------
# Menu Functions
# -----------------------------------------------------------------------

def run_fetch_kpis(celonis):
    """Option 1 — Fetch KPI values and export to CSV."""
    logger.info("User selected: Fetch KPI values")
    try:
        fetch_and_save_kpi_values(celonis)
        logger.info("KPI fetch completed successfully")
    except Exception as e:
        logger.error(f"KPI fetch failed: {e}")
        print(f"  [ERROR] {e}")


def run_list_pools(celonis):
    """Option 2 — List all Data Pools and Models."""
    logger.info("User selected: List Data Pools and Models")
    try:
        pools = list(celonis.data_integration.get_data_pools())
        if not pools:
            print("  No data pools found.")
            logger.warning("No data pools found")
            return

        print(f"\n  Found {len(pools)} Data Pool(s):\n")
        for i, pool in enumerate(pools, start=1):
            print(f"  [{i}] Pool: {pool.name}")
            logger.info(f"Pool found: {pool.name}")
            try:
                models = pool.get_data_models()
                if not models:
                    print(f"        └─ No models found")
                for model in models:
                    print(f"        └─ Model: {model.name}")
                    logger.info(f"  Model found: {model.name}")
            except Exception as e:
                logger.error(f"Could not fetch models for pool '{pool.name}': {e}")
                print(f"        └─ [ERROR] Could not fetch models: {e}")
            print()

    except Exception as e:
        logger.error(f"Could not list pools: {e}")
        print(f"  [ERROR] {e}")


def run_fetch_table(celonis):
    """Option 3 — Fetch first 10 rows from a table."""
    logger.info("User selected: Fetch table data")
    try:
        pool_name  = input("  Enter Pool name  : ").strip()
        model_name = input("  Enter Model name : ").strip()
        table_name = input("  Enter Table name : ").strip()

        logger.info(f"Fetching table '{table_name}' from '{pool_name}' / '{model_name}'")

        # Find pool
        pools = list(celonis.data_integration.get_data_pools())
        pool  = next((p for p in pools if p.name == pool_name), None)
        if pool is None:
            logger.error(f"Pool '{pool_name}' not found")
            print(f"  [ERROR] Pool '{pool_name}' not found.")
            return

        # Find model
        models = pool.get_data_models()
        model  = next((m for m in models if m.name == model_name), None)
        if model is None:
            logger.error(f"Model '{model_name}' not found")
            print(f"  [ERROR] Model '{model_name}' not found.")
            return

        # Find table
        tables = model.get_tables()
        table  = next((t for t in tables if t.name == table_name), None)
        if table is None:
            logger.error(f"Table '{table_name}' not found")
            print(f"  [ERROR] Table '{table_name}' not found.")
            print(f"  Available: {[t.name for t in tables]}")
            return

        # Build and run query
        all_columns = table.get_columns()
        query       = pql.PQL(limit=10)
        for col in all_columns:
            query += pql.PQLColumn(
                name=col.name,
                query=f'"{table_name}"."{col.name}"'
            )

        df = pql.DataFrame.from_pql(query, data_model=model).to_pandas()
        print(f"\n  First 10 rows of '{table_name}':\n")
        print(df.to_string(index=False))
        logger.info(f"Fetched {len(df)} rows from '{table_name}'")

    except Exception as e:
        logger.error(f"Fetch table failed: {e}")
        print(f"  [ERROR] {e}")


def run_export_table(celonis):
    """Option 4 — Export full table to CSV."""
    logger.info("User selected: Export table to CSV")
    try:
        pool_name  = input("  Enter Pool name  : ").strip()
        model_name = input("  Enter Model name : ").strip()
        table_name = input("  Enter Table name : ").strip()

        logger.info(f"Exporting table '{table_name}'")

        # Find pool
        pools = list(celonis.data_integration.get_data_pools())
        pool  = next((p for p in pools if p.name == pool_name), None)
        if pool is None:
            logger.error(f"Pool '{pool_name}' not found")
            print(f"  [ERROR] Pool '{pool_name}' not found.")
            return

        # Find model
        models = pool.get_data_models()
        model  = next((m for m in models if m.name == model_name), None)
        if model is None:
            logger.error(f"Model '{model_name}' not found")
            print(f"  [ERROR] Model '{model_name}' not found.")
            return

        # Find table
        tables = model.get_tables()
        table  = next((t for t in tables if t.name == table_name), None)
        if table is None:
            logger.error(f"Table '{table_name}' not found")
            print(f"  [ERROR] Table '{table_name}' not found.")
            return

        # Build and run query
        all_columns = table.get_columns()
        query       = pql.PQL()
        for col in all_columns:
            query += pql.PQLColumn(
                name=col.name,
                query=f'"{table_name}"."{col.name}"'
            )

        filepath = run_query_and_export(
            model,
            query,
            prefix=table_name
        )

        if filepath:
            logger.info(f"Exported '{table_name}' to '{filepath}'")
        else:
            logger.warning(f"Export returned no file for '{table_name}'")

    except Exception as e:
        logger.error(f"Export table failed: {e}")
        print(f"  [ERROR] {e}")


def run_show_config():
    """Option 5 — Show current config settings."""
    logger.info("User selected: Show config")
    print("\n--- Current Configuration ---\n")
    for key, value in config.items():
        if "TOKEN" in key and value:
            display = value[:6] + "****" + value[-4:]
        else:
            display = value
        print(f"  {key:<15} : {display}")
        logger.debug(f"Config — {key}: {display}")


# -----------------------------------------------------------------------
# CLI Menu
# -----------------------------------------------------------------------

def print_menu():
    """Print the main menu."""
    print("\n" + "=" * 50)
    print("         Celonis CLI Menu")
    print("=" * 50)
    print("  [1] Fetch KPI values → export to CSV")
    print("  [2] List all Data Pools and Models")
    print("  [3] Fetch first 10 rows from a Table")
    print("  [4] Export full Table to CSV")
    print("  [5] Show current config settings")
    print("  [0] Exit")
    print("=" * 50)


def main():
    print("\n" + "=" * 50)
    print("    Welcome to Celonis CLI")
    print("=" * 50 + "\n")

    logger.info("=" * 40)
    logger.info("Celonis CLI started")
    logger.info("=" * 40)

    # Step 1 – Validate config
    try:
        validate_config()
    except EnvironmentError as e:
        logger.critical(f"Config validation failed: {e}")
        print(f"\n{e}")
        sys.exit(1)

    # Step 2 – Connect once
    celonis = get_connection()
    if celonis is None:
        logger.critical("Could not connect to Celonis. Exiting.")
        print("  Could not connect. Exiting.")
        sys.exit(1)

    logger.info("Connection established successfully")

    # Step 3 – Menu loop
    menu_options = {
        "1": ("Fetch KPI values → export to CSV",   lambda: run_fetch_kpis(celonis)),
        "2": ("List all Data Pools and Models",      lambda: run_list_pools(celonis)),
        "3": ("Fetch first 10 rows from a Table",    lambda: run_fetch_table(celonis)),
        "4": ("Export full Table to CSV",            lambda: run_export_table(celonis)),
        "5": ("Show current config settings",        run_show_config),
    }

    while True:
        print_menu()
        choice = input("\n  Enter your choice: ").strip()

        if choice == "0":
            logger.info("User exited CLI menu")
            print("\n  Goodbye! 👋")
            print("=" * 50 + "\n")
            sys.exit(0)

        elif choice in menu_options:
            label, func = menu_options[choice]
            logger.info(f"Running option [{choice}]: {label}")
            print(f"\n  Running: {label}...\n")
            func()
            input("\n  Press Enter to return to menu...")

        else:
            logger.warning(f"Invalid menu choice: '{choice}'")
            print("\n  [ERROR] Invalid choice. Please enter 0-5.")


if __name__ == "__main__":
    main()