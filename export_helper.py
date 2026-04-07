"""
export_helper.py
----------------
A helper that exports any query result to CSV with a timestamped filename.
Can be imported into any script to export query results.

Usage:
    from export_helper import export_to_csv, run_query_and_export
"""

import os
import pandas as pd
from datetime import datetime
import pycelonis.pql as pql
from pycelonis.pql.saola_connector import KnowledgeModelSaolaConnector
from config import config, validate_config
from reusable_connection import get_connection


# -------------------------------------------------------------------
# Output directory from config
# -------------------------------------------------------------------
OUTPUT_DIR = config["OUTPUT_DIR"]


def get_timestamped_filename(prefix: str = "export") -> str:
    """
    Generate a timestamped filename for the CSV export.

    Args:
        prefix : Prefix for the filename. Default is 'export'.

    Returns:
        str : Timestamped filename e.g. 'export_20240101_103000.csv'
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"{prefix}_{timestamp}.csv"


def ensure_output_dir():
    """
    Create the output directory if it does not exist.
    """
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        print(f"  Created output directory: '{OUTPUT_DIR}'")


def export_to_csv(
    df        : pd.DataFrame,
    prefix    : str = "export",
    output_dir: str = None
) -> str:
    """
    Export a pandas DataFrame to a CSV file with a timestamped filename.

    Args:
        df         : DataFrame to export.
        prefix     : Prefix for the filename e.g. 'kpi', 'cases', 'activities'.
        output_dir : Folder to save the file. Defaults to config OUTPUT_DIR.

    Returns:
        str : Full path of the saved CSV file.
    """
    # Use provided output_dir or fall back to config
    output_dir = output_dir or OUTPUT_DIR

    # Ensure the output directory exists
    ensure_output_dir()

    # Generate timestamped filename
    filename  = get_timestamped_filename(prefix)
    filepath  = os.path.join(output_dir, filename)

    # Validate DataFrame
    if df is None or df.empty:
        print(f"  [WARNING] DataFrame is empty. Nothing to export.")
        return None

    # Export to CSV
    try:
        df.to_csv(filepath, index=False, encoding="utf-8")
        print(f"  ✅ Exported {len(df)} row(s) and {len(df.columns)} column(s)")
        print(f"     Saved to : '{filepath}'")
        return filepath

    except Exception as e:
        print(f"  [ERROR] Could not export to CSV: {e}")
        return None


def run_query_and_export(
    model,
    query     : pql.PQL,
    prefix    : str = "export",
    output_dir: str = None
) -> str:
    """
    Run a PQL query against a data model and export results to CSV.

    Args:
        model      : Celonis data model object.
        query      : PQL query object to execute.
        prefix     : Prefix for the output filename.
        output_dir : Folder to save the file. Defaults to config OUTPUT_DIR.

    Returns:
        str : Full path of the saved CSV file, or None on failure.
    """
    print(f"  Running query...")
    try:
        df = pql.DataFrame.from_pql(query, data_model=model).to_pandas()
        print(f"  Query returned {len(df)} row(s)\n")
        return export_to_csv(df, prefix=prefix, output_dir=output_dir)

    except Exception as e:
        print(f"  [ERROR] Query failed: {e}")
        return None


def run_km_query_and_export(
    model,
    km,
    query     : pql.PQL,
    prefix    : str = "export",
    output_dir: str = None
) -> str:
    """
    Run a PQL query using a Knowledge Model connector and export to CSV.

    Args:
        model      : Celonis data model object.
        km         : Celonis knowledge model object.
        query      : PQL query object to execute.
        prefix     : Prefix for the output filename.
        output_dir : Folder to save the file. Defaults to config OUTPUT_DIR.

    Returns:
        str : Full path of the saved CSV file, or None on failure.
    """
    print(f"  Running knowledge model query...")
    try:
        connector = KnowledgeModelSaolaConnector(model, km)
        df = pql.DataFrame.from_pql(
            query,
            saola_connector=connector
        ).to_pandas()
        print(f"  Query returned {len(df)} row(s)\n")
        return export_to_csv(df, prefix=prefix, output_dir=output_dir)

    except Exception as e:
        print(f"  [ERROR] Knowledge model query failed: {e}")
        return None


def main():
    """Test the export helper with a sample query."""
    print("=" * 50)
    print("  Testing Export Helper")
    print("=" * 50 + "\n")

    try:
        # Step 1 – Validate config
        validate_config()

        # Step 2 – Connect
        celonis = get_connection()

        # Step 3 – Get pool and model
        pools = list(celonis.data_integration.get_data_pools())
        pool  = next((p for p in pools if p.name == config["MY_POOL"]), None)
        if pool is None:
            print(f"  [ERROR] Pool '{config['MY_POOL']}' not found.")
            return

        models = pool.get_data_models()
        model  = next((m for m in models if m.name == config["MY_MODEL"]), None)
        if model is None:
            print(f"  [ERROR] Model '{config['MY_MODEL']}' not found.")
            return

        # Step 4 – Get table columns
        tables = model.get_tables()
        table  = next((t for t in tables if t.name == config["MY_TABLE"]), None)
        if table is None:
            print(f"  [ERROR] Table '{config['MY_TABLE']}' not found.")
            return

        # Step 5 – Build PQL query
        all_columns = table.get_columns()
        query       = pql.PQL(limit=config["ROW_LIMIT"])
        for col in all_columns:
            query += pql.PQLColumn(
                name=col.name,
                query=f'"{config["MY_TABLE"]}"."{col.name}"'
            )

        # Step 6 – Run query and export
        filepath = run_query_and_export(
            model,
            query,
            prefix="test_export"
        )

        if filepath:
            print(f"\n  Test passed! File saved to: '{filepath}'")
        else:
            print(f"\n  Test failed! No file was saved.")

        print("\n" + "=" * 50)
        print("  Export helper test complete!")
        print("=" * 50)

    except Exception as e:
        print(f"\n  [ERROR] {e}")


if __name__ == "__main__":
    main()