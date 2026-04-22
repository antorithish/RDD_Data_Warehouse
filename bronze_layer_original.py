import shutil
import os, sys
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from datetime import datetime

# -------------------------
# Spark Setup
# -------------------------
spark = SparkSession.builder \
    .appName("Bronze_Layer") \
    .master("local[2]") \
    .config("spark.driver.memory", "1g") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
sc = spark.sparkContext

# -------------------------
# Load Date
# -------------------------
LOAD_DATE = datetime.now().strftime("%Y-%m-%d")

print("=" * 60)
print("🥉 BRONZE LAYER — RAW INGESTION")
print(f"Load Date: {LOAD_DATE}")
print("=" * 60)


# -------------------------
# Function
# -------------------------
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


# -------------------------
# File Paths (FIXED) prd_info.csv
# -------------------------
files = [
    r"C:\Users\antor\OneDrive\Desktop\data warehouse project\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_crm\cust_info.csv",
    r"C:\Users\antor\OneDrive\Desktop\data warehouse project\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_crm\prd_info.csv",
    r"C:\Users\antor\OneDrive\Desktop\data warehouse project\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_crm\sales_details.csv",
    r"C:\Users\antor\OneDrive\Desktop\data warehouse project\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv",
    r"C:\Users\antor\OneDrive\Desktop\data warehouse project\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_erp\LOC_A101.CSV",
    r"C:\Users\antor\OneDrive\Desktop\data warehouse project\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv"
]

file_names = [
    "customer_info",
    "product_info",
    "sales_details",
    "customer_birth_date",
    "customer_location",
    "category_products"
]

# -------------------------
# Combine ALL files
# -------------------------

base_output = r"C:\Users\antor\OneDrive\Desktop\data warehouse project\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\bronze_output"

for path, name in zip(files, file_names):

    rdd = file_extract(path, name)

    print("count:", rdd.count())
    print("sample:", rdd.first()["rawdata"])

    # create unique path per file
    output_path = os.path.join(base_output, name)

    # delete if exists
    if os.path.exists(output_path):
        shutil.rmtree(output_path)

    # save
    rdd.saveAsTextFile(output_path)
