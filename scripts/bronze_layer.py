"""
================================================================================
BRONZE LAYER — RAW DATA INGESTION
================================================================================
Purpose  : Load raw source files AS-IS into the Bronze zone.
           No cleaning. No transformation. Just raw data + metadata.

Layer    : Bronze (Layer 1 of 3 in Medallion Architecture)
Reads    : Raw CSV files from CRM and ERP source systems
Writes   : bronze_output/ folder (one subfolder per source file)

Metadata added to every row:
    - rawdata   : exact original line from source file
    - path_name : which source file this row came from
    - load_date : when this data was loaded (YYYY-MM-DD)

Rule     : Bronze = untouched raw data. Acts as audit trail.
           If anything goes wrong in Silver/Gold, we can always
           reprocess from Bronze.

Author   : Anto Rithish
Project  : Data Warehouse — Medallion Architecture (RDD)
================================================================================
"""

import shutil
import os, sys

# Tell Spark which Python to use for workers — must match driver Python
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from datetime import datetime

# ─────────────────────────────────────────────────────────────────────────────
# SPARK SESSION SETUP
# ─────────────────────────────────────────────────────────────────────────────
# local[2]   → run on 2 CPU cores on this machine
# 1g memory  → 1 GB RAM allocated to the driver
# shuffle=4  → use 4 partitions for shuffle operations (good for local mode)
# ─────────────────────────────────────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("Bronze_Layer") \
    .master("local[2]") \
    .config("spark.driver.memory", "1g") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

# Suppress INFO and WARN logs — only show ERROR level messages
spark.sparkContext.setLogLevel("ERROR")

# sc = SparkContext — the entry point for all RDD operations
sc = spark.sparkContext

# ─────────────────────────────────────────────────────────────────────────────
# LOAD DATE
# Used as metadata to track when this batch was ingested
# Format: YYYY-MM-DD (e.g. 2026-04-10)
# ─────────────────────────────────────────────────────────────────────────────
LOAD_DATE = datetime.now().strftime("%Y-%m-%d")

print("=" * 60)
print("🥉 BRONZE LAYER — RAW INGESTION")
print(f"Load Date: {LOAD_DATE}")
print("=" * 60)


# ─────────────────────────────────────────────────────────────────────────────
# FUNCTION: file_extract
# ─────────────────────────────────────────────────────────────────────────────
# What it does:
#   1. Reads a CSV file using sc.textFile() → each line becomes one RDD item
#   2. zipWithIndex() → attaches row number to each line (0, 1, 2, ...)
#   3. filter(index > 0) → skips row 0 (the header row)
#   4. map(x[0]) → keeps only the raw line, drops the index number
#   5. map(dict) → wraps each raw line in a dict with 3 metadata fields
#
# Parameters:
#   path      → full file path of the CSV
#   pathname  → friendly name for this source (e.g. "customer_info")
#
# Returns:
#   RDD of dicts — each dict has rawdata, path_name, load_date
#
# Example output row:
#   {
#       "rawdata"   : "11000,AW00011000, Jon,Yang ,M,M,2025-10-06",
#       "path_name" : "customer_info",
#       "load_date" : "2026-04-10"
#   }
# ─────────────────────────────────────────────────────────────────────────────
def file_extract(path, pathname):
    return sc.textFile(path, 4) \
        .zipWithIndex() \
        .filter(lambda x: x[1] > 0) \
        .map(lambda x: x[0]) \
        .map(lambda line: {
            "rawdata": line,
            "path_name": pathname,
            "load_date": LOAD_DATE
        })


# ─────────────────────────────────────────────────────────────────────────────
# SOURCE FILE PATHS
# ─────────────────────────────────────────────────────────────────────────────
# Two source systems:
#   CRM → internal customer relationship system (3 files)
#   ERP → external enterprise resource planning system (3 files)
#
# Files:
#   cust_info.csv   → customer master data (name, gender, marital status)
#   prd_info.csv    → product master data  (name, cost, category, dates)
#   sales_details   → transaction records  (orders, quantity, price)
#   CUST_AZ12.csv   → customer birth dates and gender from ERP
#   LOC_A101.CSV    → customer country/location from ERP
#   PX_CAT_G1V2.csv → product categories and subcategories from ERP
# ─────────────────────────────────────────────────────────────────────────────
files = [
    r"C:\Users\antor\OneDrive\Desktop\data warehouse project\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_crm\cust_info.csv",
    r"C:\Users\antor\OneDrive\Desktop\data warehouse project\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_crm\prd_info.csv",
    r"C:\Users\antor\OneDrive\Desktop\data warehouse project\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_crm\sales_details.csv",
    r"C:\Users\antor\OneDrive\Desktop\data warehouse project\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv",
    r"C:\Users\antor\OneDrive\Desktop\data warehouse project\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_erp\LOC_A101.CSV",
    r"C:\Users\antor\OneDrive\Desktop\data warehouse project\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv"
]

# Friendly names used for:
#   1. The "path_name" metadata field in each row
#   2. The output subfolder name under bronze_output/
file_names = [
    "customer_info",  # → bronze_output/customer_info/
    "product_info",  # → bronze_output/product_info/
    "sales_details",  # → bronze_output/sales_details/
    "customer_birth_date",  # → bronze_output/customer_birth_date/
    "customer_location",  # → bronze_output/customer_location/
    "category_products"  # → bronze_output/category_products/
]

# ─────────────────────────────────────────────────────────────────────────────
# BASE OUTPUT PATH
# All bronze output subfolders will be created inside this directory
# ─────────────────────────────────────────────────────────────────────────────
base_output = r"C:\Users\antor\OneDrive\Desktop\data warehouse project\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\bronze_output"

# ─────────────────────────────────────────────────────────────────────────────
# INGEST LOOP
# Iterates over each (file_path, file_name) pair and:
#   1. Extracts the raw file into a Bronze RDD
#   2. Prints row count and sample for validation
#   3. Deletes existing output folder (idempotent re-run support)
#   4. Saves the Bronze RDD to disk as text files
# ─────────────────────────────────────────────────────────────────────────────
for path, name in zip(files, file_names):

    print(f"\n{'─' * 60}")
    print(f"📥  Ingesting : {name}")
    print(f"{'─' * 60}")

    # Step 1: Extract raw data + attach metadata
    rdd = file_extract(path, name)

    # Step 2: Validate — print row count and first raw line
    print("count  :", rdd.count())
    print("sample :", rdd.first()["rawdata"])

    # Step 3: Build the unique output path for this file
    #         Example: bronze_output/customer_info/
    output_path = os.path.join(base_output, name)

    # Step 4: Delete existing output folder if it exists
    #         This makes the script safe to re-run (idempotent)
    if os.path.exists(output_path):
        shutil.rmtree(output_path)
        print(f"deleted : existing folder cleared → {output_path}")

    # Step 5: Save the Bronze RDD to disk
    #         Spark creates part-00000, part-00001 ... files inside the folder
    rdd.saveAsTextFile(output_path)
    print(f"saved   : ✅ {output_path}")

# ─────────────────────────────────────────────────────────────────────────────
# BRONZE COMPLETE
# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "=" * 60)
print("🥉 BRONZE LAYER — COMPLETE")
print("=" * 60)
print(f"""
  Output location : {base_output}

  Folders created :
    bronze_output/customer_info/
    bronze_output/product_info/
    bronze_output/sales_details/
    bronze_output/customer_birth_date/
    bronze_output/customer_location/
    bronze_output/category_products/

  Next step → Run silver_layer.py
""")

spark.stop()
