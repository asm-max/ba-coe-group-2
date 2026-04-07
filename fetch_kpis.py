

"""
main.py
-------
Task: Fetch KPI values from a Knowledge Model (OCDM) and save to a CSV file.
      Includes robust error handling for failed KPI exports.
"""

import os
import csv
from datetime import datetime
from dotenv import load_dotenv
from pycelonis import get_celonis
import pycelonis.pql as pql
from pycelonis.pql.saola_connector import KnowledgeModelSaolaConnector

# Load credentials from .env
load_dotenv()

BASE_URL  = os.getenv("BASE_URL")
API_TOKEN = os.getenv("API_TOKEN")

# Your specific targets
MY_POOL    = "02a. Catalog Extension - SAP ECC"
MY_MODEL   = "perspective_custom_ProcurementSAPECC"
MY_SPACE   = "[Procurement] Catalog 3.0 Marketplace Assets"
MY_PACKAGE = "SAP - Procurement Starter Kit (Catalog 3)"
MY_KM      = "OCPM KM"

# Output file — timestamped so each run creates a new file
TIMESTAMP   = datetime.now().strftime("%Y%m%d_%H%M%S")
OUTPUT_FILE = f"kpi_output_{TIMESTAMP}.csv"


def get_celonis_connection():
    """Connect to Celonis and return the client."""
    print("Connecting to Celonis...")
    celonis = get_celonis(
        base_url=BASE_URL,
        api_token=API_TOKEN,
        key_type="USER_KEY",
        permissions=False
    )
    print("Connected successfully!\n")
    return celonis


def get_data_model(celonis, pool_name: str, model_name: str):
    """Find and return a specific data model."""
    pools = celonis.data_integration.get_data_pools()
    pool  = next((p for p in pools if p.name == pool_name), None)
    if pool is None:
        print(f"[ERROR] Pool '{pool_name}' not found.")
        return None

    models = pool.get_data_models()
    model  = next((m for m in models if m.name == model_name), None)
    if model is None:
        print(f"[ERROR] Model '{model_name}' not found.")
        return None

    print(f"Found data model : {model.name}\n")
    return model


def get_knowledge_model(celonis, space_name: str, package_name: str, km_name: str):
    """Find and return a Knowledge Model from Studio."""
    spaces = celonis.studio.get_spaces()
    space  = next((s for s in spaces if s.name == space_name), None)
    if space is None:
        print(f"[ERROR] Space '{space_name}' not found.")
        print(f"Available spaces: {[s.name for s in spaces]}")
        return None

    packages = space.get_packages()
    package  = next((p for p in packages if p.name == package_name), None)
    if package is None:
        print(f"[ERROR] Package '{package_name}' not found.")
        return None

    kms = package.get_knowledge_models()
    km  = next((k for k in kms if k.name == km_name), None)
    if km is None:
        print(f"[ERROR] Knowledge Model '{km_name}' not found.")
        return None

    print(f"Found knowledge model: {km.name}\n")
    return km


def export_single_kpi(kpi, data_model, km):
    """
    Try to export a single KPI value.
    Returns (value, status) tuple.

    Common failure reasons:
      - KPI uses KM variables that can't be resolved standalone
      - KPI has filters referencing other KPIs
      - KPI PQL has syntax unsupported in standalone export
    """

    # Strategy 1 – Export using KPI display_name and pql directly
    try:
        query = pql.PQL()
        query += pql.PQLColumn(
            name=kpi.display_name,
            query=kpi.pql
        )
        connector = KnowledgeModelSaolaConnector(data_model, km)
        df = pql.DataFrame.from_pql(
            query,
            saola_connector=connector
        ).to_pandas()

        if not df.empty:
            return str(df.iloc[0, 0]), "Success"
        return "N/A", "Empty result"

    except Exception as e1:
        # Strategy 2 – Try with KPI id instead of display_name
        try:
            query = pql.PQL()
            query += pql.PQLColumn(
                name=kpi.id,
                query=kpi.pql
            )
            connector = KnowledgeModelSaolaConnector(data_model, km)
            df = pql.DataFrame.from_pql(
                query,
                saola_connector=connector
            ).to_pandas()

            if not df.empty:
                return str(df.iloc[0, 0]), "Success (fallback id)"
            return "N/A", "Empty result"

        except Exception as e2:
            # Both strategies failed — log the PQL and reason
            reason = str(e2)

            # Classify the error for better reporting
            if "variable" in reason.lower():
                status = "Skipped — KPI uses unresolved variables"
            elif "permission" in reason.lower():
                status = "Skipped — Permission denied"
            elif "syntax" in reason.lower():
                status = "Skipped — PQL syntax error"
            elif "filter" in reason.lower():
                status = "Skipped — KPI has unsupported filter"
            else:
                status = f"Failed — {reason[:100]}"   # trim long errors

            return "N/A", status


def fetch_and_save_kpi_values(celonis):
    """Fetch KPI values from Knowledge Model and save to CSV."""
    print("=" * 50)
    print("Fetching KPI values from Knowledge Model")
    print("=" * 50 + "\n")

    # Step 1 – Get data model and knowledge model
    data_model = get_data_model(celonis, MY_POOL, MY_MODEL)
    if data_model is None:
        return

    km = get_knowledge_model(celonis, MY_SPACE, MY_PACKAGE, MY_KM)
    if km is None:
        return

    # Step 2 – Get all KPIs
    kpis = km.get_kpis()
    if not kpis:
        print("  No KPIs found in this knowledge model.")
        return

    print(f"  Found {len(kpis)} KPI(s). Fetching values...\n")
    print("-" * 50)

    # Step 3 – Export each KPI and collect results
    results   = []
    success   = 0
    skipped   = 0
    failed    = 0

    for kpi in kpis:
        value, status = export_single_kpi(kpi, data_model, km)

        # Track counts
        if status == "Success" or "fallback" in status:
            success += 1
            icon = "✅"
        elif "Skipped" in status:
            skipped += 1
            icon = "⚠️ "
        else:
            failed += 1
            icon = "❌"

        print(f"  {icon} {kpi.display_name:<40} : {value}")
        if status not in ("Success",):
            print(f"       └─ {status}")

        results.append({
            "KPI ID"       : kpi.id,
            "KPI Name"     : kpi.display_name,
            "PQL"          : kpi.pql,
            "Value"        : value,
            "Status"       : status,
            "Fetched At"   : datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })

    # Step 4 – Print summary
    print("\n" + "-" * 50)
    print(f"  Summary:")
    print(f"    ✅ Successful : {success}")
    print(f"    ⚠️  Skipped   : {skipped}")
    print(f"    ❌ Failed     : {failed}")
    print("-" * 50)

    # Step 5 – Save to CSV
    print(f"\nSaving results to '{OUTPUT_FILE}'...")
    with open(OUTPUT_FILE, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["KPI ID", "KPI Name", "PQL", "Value", "Status", "Fetched At"]
        )
        writer.writeheader()
        writer.writerows(results)

    print(f"✅ Saved {len(results)} KPI(s) to '{OUTPUT_FILE}'")
    print("\n" + "=" * 50)
    print("KPI fetch complete!")
    print("=" * 50)


def main():
    celonis = get_celonis_connection()
    fetch_and_save_kpi_values(celonis)


if __name__ == "__main__":
    main()