"""
================================================================================
🥈 SILVER LAYER — CLEANING, STANDARDIZATION, AND VALIDATION
================================================================================
Purpose  : Read Bronze data, clean it, standardize it, and save structured data.
Layer    : Silver (Layer 2 of 3 in Medallion Architecture)
Reads    : bronze_output/
Writes   : silver_layer_output/

What this layer does:
    - Cleans raw strings from Bronze
    - Standardizes IDs, names, dates, and categories
    - Converts invalid or messy values into clean values
    - Tracks invalid records using Spark Accumulators
    - Filters out unusable rows
    - Saves clean structured datasets for Gold layer

Rule:
    Bronze = raw
    Silver  = cleaned and standardized
================================================================================
"""

# =============================================================================
# IMPORTS
# =============================================================================
import os
import sys
from pyspark.sql import SparkSession
from datetime import datetime
from ast import literal_eval
import shutil

# =============================================================================
# SPARK SETUP
# =============================================================================
# Make sure Spark workers use the same Python interpreter as this script.
# This avoids Python version mismatch issues in worker processes.
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# Create Spark session for the Silver layer job.
# local[2] means Spark will use 2 local CPU threads.
spark = SparkSession.builder \
    .appName("Silver_Layer") \
    .master("local[2]") \
    .config("spark.driver.memory", "1g") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

# Reduce noise in terminal output.
spark.sparkContext.setLogLevel("ERROR")
sc = spark.sparkContext

print("=" * 70)
print("🥈 SILVER LAYER — DATA CLEANING STARTED")
print("=" * 70)

# =============================================================================
# SECTION 1: customer_info
# =============================================================================
print("\n" + "=" * 30)
print("customer_info")
print("=" * 30)

# Bronze customer_info contains raw rows stored as Python dictionary strings.
# literal_eval() converts each saved string back into a Python dictionary.
file_path_info = r"C:\Users\antor\OneDrive\Desktop\data warehouse project\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\bronze_output\customer_info\*"
load_customer_info = sc.textFile(file_path_info, 4) \
                       .map(lambda line: literal_eval(line))

# Accumulator to count invalid customer names.
accum_customer_name = sc.accumulator(0)


def customer_id_raw(id_raw):
    """
    Convert customer id into an integer only if it is numeric.
    Example:
        "11000" -> 11000
        "ABC"   -> None
    """
    if id_raw and str(id_raw).isdigit():
        return int(id_raw)
    return None


def customer_key_raw(key):
    """
    Validate customer key format.

    Expected pattern:
        AW00011000

    Rules:
        - not empty
        - uppercase
        - length exactly 10
        - starts with 'AW'
        - remaining characters are digits
    """
    if key is None or str(key).strip() == "":
        return None

    key = str(key).strip().upper()

    if len(key) != 10:
        return None
    if not key.startswith("AW"):
        return None
    if not key[2:].isdigit():
        return None

    return key


def firstname_lastname(first_name, last_name):
    """
    Combine first name and last name into one full name.

    If both values are missing, count that case in the accumulator.
    """
    if first_name and last_name:
        return first_name + " " + last_name
    elif first_name:
        return first_name
    elif last_name:
        return last_name

    accum_customer_name.add(1)
    return None


def martial_status(martial):
    """
    Convert one-letter marital status into a readable value.
    Example:
        M -> Married
        S -> Single
    """
    if martial:
        if martial.upper() == "M":
            return "Married"
        elif martial.upper() == "S":
            return "Single"
    return None


def gender_status(gender):
    """
    Convert one-letter gender into a readable value.
    Example:
        M -> Male
        F -> Female
    """
    if gender:
        if gender.upper() == "M":
            return "Male"
        elif gender.upper() == "F":
            return "Female"
    return None


def new_customer_create_date(customer_data):
    """
    Validate date string and standardize it to YYYY-MM-DD.
    If the date is invalid, return None.
    """
    if customer_data and len(customer_data) == 10:
        try:
            date_obj = datetime.strptime(customer_data, "%Y-%m-%d")
            return date_obj.strftime("%Y-%m-%d")
        except ValueError:
            return None
    return None


def customer_detail(row):
    """
    Convert one raw Bronze customer_info row into a cleaned dictionary.

    Input row structure:
        {
            "rawdata": "11000,AW00011000,Jon,Yang,M,M,2025-10-06",
            "path_name": "customer_info",
            "load_date": "2026-04-10"
        }
    """
    raw = row.get("rawdata")
    if raw is None:
        return None

    split_raw = raw.split(",")
    if len(split_raw) < 7:
        return None

    customer_id = split_raw[0].strip()
    customer_key = split_raw[1].strip()
    customer_firstname = split_raw[2].strip()
    customer_lastname = split_raw[3].strip()
    customer_gender = split_raw[4].strip()
    customer_martial_status = split_raw[5].strip()
    customer_create_date = split_raw[6].strip()

    return {
        "customer_id_cleaned": customer_id_raw(customer_id),
        "customer_key_cleaned": customer_key_raw(customer_key),
        "customer_name_cleaned": firstname_lastname(customer_firstname, customer_lastname),
        "customer_martial_cleaned": martial_status(customer_martial_status),
        "customer_gender_cleaned": gender_status(customer_gender),
        "customer_date": new_customer_create_date(customer_create_date)
    }


def not_none(x):
    """
    Keep only rows that have a valid customer key.
    This removes unusable rows from the Silver output.
    """
    return x is not None and x["customer_key_cleaned"] is not None


silver_customer_info = load_customer_info.map(customer_detail).filter(not_none)

print("Sample customer_info rows:")
print(silver_customer_info.take(5))

print("\n" + "=" * 30)
print("ACCUMULATOR RESULTS")
print("=" * 30)
print("Invalid customer name count :", accum_customer_name.value)

# =============================================================================
# SECTION 2: customer_birth
# =============================================================================
print("\n" + "=" * 30)
print("customer_birth")
print("=" * 30)

# Bronze customer_birth_date file contains raw birth date and gender rows.
file_path_birth = r"C:\Users\antor\OneDrive\Desktop\data warehouse project\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\bronze_output\customer_birth_date\*"
load_customer_birth_info = sc.textFile(file_path_birth, 4) \
                             .map(lambda line: literal_eval(line))

# Accumulators for birth dataset quality checks.
accumalator_customer_key = sc.accumulator(0)
accumnaltor_birthdate = sc.accumulator(0)
accumaltor_gender = sc.accumulator(0)
accumaltor_age = sc.accumulator(0)


def raw_customer_key(customer_key):
    """
    Clean customer key from ERP birth file.

    Expected raw pattern:
        NASAW00011000

    Logic:
        - strip the 'NAS' prefix
        - keep only the AW... key part
        - validate the cleaned key
    """
    if customer_key is None or customer_key.strip() == "":
        accumalator_customer_key.add(1)
        return None

    customer_key = customer_key.strip().upper()

    if len(customer_key) == 13 and customer_key.startswith("NAS"):
        cleaned_key = customer_key[3:]
        if len(cleaned_key) == 10 and cleaned_key.startswith("AW") and cleaned_key[2:].isdigit():
            return cleaned_key

    accumalator_customer_key.add(1)
    return None


def raw_birth_date(birth_date):
    """
    Validate birth_date format and normalize it to YYYY-MM-DD.
    """
    if not birth_date:
        accumnaltor_birthdate.add(1)
        return None

    if len(birth_date) != 10 or birth_date[4] != "-" or birth_date[7] != "-":
        accumnaltor_birthdate.add(1)
        return None

    try:
        date_obj = datetime.strptime(birth_date, "%Y-%m-%d")
        return date_obj.strftime("%Y-%m-%d")
    except ValueError:
        accumnaltor_birthdate.add(1)
        return None


def raw_gender(gender):
    """
    Standardize gender text.
    Example:
        male   -> Male
        female -> Female
    """
    if gender.lower() == "male":
        return "Male"
    elif gender.lower() == "female":
        return "Female"
    else:
        accumaltor_gender.add(1)
        return None


def raw_age(birth_age):
    """
    Calculate age from birth_date.
    The input is the standardized birth date string.
    """
    if birth_age is None:
        accumaltor_age.add(1)
        return None

    try:
        date_obj = datetime.strptime(birth_age, "%Y-%m-%d")
        today = datetime.today()
        age = today.year - date_obj.year
        if (today.month, today.day) < (date_obj.month, date_obj.day):
            age -= 1
        return age
    except:
        accumaltor_age.add(1)
        return None


def customer_birthday(details):
    """
    Convert one raw birth row into a cleaned dictionary.
    """
    if details is None:
        return None

    raw = details.get("rawdata")
    if raw is None:
        return None

    split_row = raw.split(",")
    if len(split_row) < 3:
        return None

    customer_key = split_row[0].strip()
    birth_date = split_row[1].strip()
    gender = split_row[2].strip()

    clean_birth = raw_birth_date(birth_date)

    return {
        "customer_key": raw_customer_key(customer_key),
        "birth_date": clean_birth,
        "gender": raw_gender(gender),
        "age": raw_age(clean_birth)
    }


def not_none1(x):
    """
    Keep only rows with a valid customer key.
    """
    return x is not None and x["customer_key"] is not None


silver_layer = load_customer_birth_info.map(customer_birthday).filter(not_none1)

print("Sample customer_birth rows:")
print(silver_layer.take(5))

print("\n" + "=" * 30)
print("ACCUMULATOR RESULTS")
print("=" * 30)
print("Invalid Customer Key   :", accumalator_customer_key.value)
print("Invalid Birth Date     :", accumnaltor_birthdate.value)
print("Invalid Gender         :", accumaltor_gender.value)
print("Invalid Age            :", accumaltor_age.value)

# =============================================================================
# SECTION 3: customer_location
# =============================================================================
print("\n" + "=" * 30)
print("customer_location")
print("=" * 30)

# Bronze location file contains customer key and country/state information.
file_path_loc = r"C:\Users\antor\OneDrive\Desktop\data warehouse project\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\bronze_output\customer_location\*"
load_customer_location_info = sc.textFile(file_path_loc, 4) \
                                .map(lambda line: literal_eval(line))

# Accumulators for invalid location records.
accumalator_customer_key_location = sc.accumulator(0)
accumalator_customer_location_state = sc.accumulator(0)


def raw_customer_location_key(customer_location_key):
    """
    Standardize location key into AW00011000 style.

    Input may look like:
        AW-00011000

    Output becomes:
        AW00011000
    """
    if not customer_location_key:
        accumalator_customer_key_location.add(1)
        return None

    customer_location_key = customer_location_key.strip().upper()

    if len(customer_location_key) == 11 and customer_location_key.startswith("AW-"):
        numeric_part = customer_location_key[3:]
        if numeric_part.isdigit():
            return customer_location_key.replace("-", "")

    accumalator_customer_key_location.add(1)
    return None


def raw_customer_location_state(customer_location_state):
    """
    Standardize location values into a controlled country name list.
    This keeps reporting consistent later in Gold layer.
    """
    if not customer_location_state:
        accumalator_customer_location_state.add(1)
        return None

    val = customer_location_state.strip()

    if val in ["US", "USA", "United States"]:
        return "United States"
    elif val == "United Kingdom":
        return "United Kingdom"
    elif val == "Australia":
        return "Australia"
    elif val == "Canada":
        return "Canada"
    elif val == "France":
        return "France"
    elif val == "Germany":
        return "Germany"
    elif val == "DE":
        return "Germany"
    else:
        accumalator_customer_location_state.add(1)
        return None


def customer_location(location_details):
    """
    Convert one raw location row into a cleaned dictionary.
    """
    if location_details is None:
        return None

    raw_location = location_details.get("rawdata")
    if raw_location is None:
        return None

    split_row_location = raw_location.split(",")
    if len(split_row_location) < 2:
        return None

    customer_location_key = split_row_location[0].strip()
    customer_location_state = split_row_location[1].strip()

    return {
        "customer_location_key": raw_customer_location_key(customer_location_key),
        "customer_location_state": raw_customer_location_state(customer_location_state)
    }


def not_none2(x):
    """
    Keep only rows with a valid location key.
    """
    return x is not None and x["customer_location_key"] is not None


silver_layer_location = load_customer_location_info.map(customer_location).filter(not_none2)

print("Sample customer_location rows:")
print(silver_layer_location.take(5))

print("\n" + "=" * 30)
print("ACCUMULATOR RESULTS")
print("=" * 30)
print("Invalid Customer_location_key :", accumalator_customer_key_location.value)
print("Invalid_location_state        :", accumalator_customer_location_state.value)

# =============================================================================
# SECTION 4: category_products
# =============================================================================
print("\n" + "=" * 30)
print("category_products")
print("=" * 30)

# Bronze category file contains product category mapping data.
file_path_cat = r"C:\Users\antor\OneDrive\Desktop\data warehouse project\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\bronze_output\category_products\*"
load_customer_cat_product = sc.textFile(file_path_cat, 4) \
                              .map(lambda line: literal_eval(line))

# Track invalid category rows.
accumalator_category_key = sc.accumulator(0)
accumalator_category = sc.accumulator(0)
accumalator_subcat = sc.accumulator(0)
accumalator_maintance = sc.accumulator(0)


def customer_id_key(customer_product_key):
    """
    Validate the category-product key format.
    This keeps only keys that match the expected structure.
    """
    if customer_product_key:
        if len(customer_product_key) == 5:
            if customer_product_key[2] == "_":
                return customer_product_key
            else:
                accumalator_category_key.add(1)
                return None
        else:
            accumalator_category_key.add(1)
            return None
    else:
        accumalator_category_key.add(1)
        return None


def customer_category_cat(customer_category):
    """
    Clean category value.
    """
    if customer_category is None or customer_category == "":
        accumalator_category.add(1)
        return None
    else:
        return customer_category


def sub_cat(customer_sub_category):
    """
    Clean sub-category value.
    """
    if customer_sub_category is None or customer_sub_category == "":
        accumalator_subcat.add(1)
        return None
    else:
        return customer_sub_category


def customer_maintance(customer_category_maintance):
    """
    Standardize maintenance flag to Yes/No.
    """
    if customer_category_maintance == "Yes":
        return "Yes"
    elif customer_category_maintance == "No":
        return "No"
    else:
        accumalator_maintance.add(1)
        return None


def category_products(category_product):
    """
    Convert one raw category row into a cleaned dictionary.
    """
    if category_product is None:
        return None

    raw_category = category_product.get("rawdata")
    if raw_category is None:
        return None

    split_row_category = raw_category.split(",")
    if len(split_row_category) < 4:
        return None

    customer_product_key = split_row_category[0].strip()
    customer_category = split_row_category[1].strip()
    customer_sub_category = split_row_category[2].strip()
    customer_category_maintance = split_row_category[3].strip()

    return {
        "customer_product_key1": customer_id_key(customer_product_key),
        "customer_category1": customer_category_cat(customer_category),
        "customer_sub_category1": sub_cat(customer_sub_category),
        "customer_category_maintance1": customer_maintance(customer_category_maintance)
    }


def not_none3(x):
    """
    Keep only rows with a valid product/category key.
    """
    return x is not None and x["customer_product_key1"] is not None


silver_layer_category = load_customer_cat_product.map(category_products).filter(not_none3)

print("Sample category_products rows:")
print(silver_layer_category.take(5))

print("\n" + "=" * 30)
print("ACCUMULATOR RESULTS")
print("=" * 30)
print("Invalid Customer_cat_key  :", accumalator_category_key.value)
print("Invalid_category          :", accumalator_category.value)
print("Invalid_sub_category      :", accumalator_subcat.value)
print("Invalid_maintance         :", accumalator_maintance.value)

# =============================================================================
# SECTION 5: product_details
# =============================================================================
print("\n" + "=" * 30)
print("product_details")
print("=" * 30)

# Bronze product file contains product master details.
file_path_product_info = r"C:\Users\antor\OneDrive\Desktop\data warehouse project\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\bronze_output\product_info\*"
load_customer_prod = sc.textFile(file_path_product_info, 4) \
                       .map(lambda line: literal_eval(line))

# Track invalid product values.
accum_product_id = sc.accumulator(0)
accum_cat_id = sc.accumulator(0)
accum_prod_key = sc.accumulator(0)
accum_product_nm = sc.accumulator(0)
accum_product_cost = sc.accumulator(0)
accum_product_line = sc.accumulator(0)
accum_product_start_date = sc.accumulator(0)


def to_float(x):
    """
    Safely convert text to float.
    Returns None if conversion fails.
    """
    try:
        return float(x)
    except:
        return None


def raw_product_id(product_id):
    """
    Validate product id.
    """
    if len(product_id) == 3 and product_id != "":
        return product_id
    else:
        accum_product_id.add(1)
        return "N/A"


def raw_cat_id(cat_id):
    """
    Standardize category id.
    """
    if cat_id and cat_id != "":
        split_part = cat_id[0:5]
        replace = split_part.replace("-", "_")
        return replace
    else:
        accum_cat_id.add(1)
        return "N/A"


def raw_prod_key(prod_key):
    """
    Extract product key portion from raw composite value.
    """
    if prod_key and prod_key != "":
        split_prod_key = prod_key[6:len(prod_key)]
        return split_prod_key
    else:
        accum_prod_key.add(1)
        return "N/A"


def raw_product_nm(product_nm):
    """
    Clean product name.
    """
    if product_nm and product_nm != "":
        return product_nm
    else:
        accum_product_nm.add(1)
        return "N/A"


def raw_product_cost(product_cost):
    """
    Clean product cost and convert it to numeric value.
    Invalid values become 0.
    """
    product_cost1 = to_float(product_cost)
    if product_cost1 is None or product_cost1 < 0:
        accum_product_cost.add(1)
        return 0
    else:
        return product_cost1


def raw_product_line(product_line):
    """
    Standardize product line codes.
        M -> Mountain
        R -> Road
        S -> Other Sales
        T -> Touring
    """
    if product_line:
        if product_line.upper() == "M":
            return "Mountain"
        elif product_line.upper() == "R":
            return "Road"
        elif product_line.upper() == "S":
            return "Other Sales"
        elif product_line.upper() == "T":
            return "Touring"
        else:
            accum_product_line.add(1)
            return "N/A"
    else:
        return "N/A"


def raw_product_start_date(product_start_date):
    """
    Validate and standardize product start date.
    """
    if product_start_date and len(product_start_date) == 10:
        try:
            date_obj_1 = datetime.strptime(product_start_date, "%Y-%m-%d")
            return date_obj_1.strftime("%Y-%m-%d")
        except ValueError:
            accum_product_start_date.add(1)
            return "N/A"
    accum_product_start_date.add(1)
    return "N/A"


def customer_product_info(product_info):
    """
    Convert one raw product row into a cleaned dictionary.
    """
    if product_info is None:
        return None

    raw_product = product_info.get("rawdata")
    if raw_product is None:
        return None

    split_row_product = raw_product.split(",")
    if len(split_row_product) < 6:
        return None

    product_id = split_row_product[0].strip()
    cat_id = split_row_product[1].strip()
    prod_key = split_row_product[1].strip()
    product_nm = split_row_product[2].strip()
    product_cost = split_row_product[3].strip()
    product_line = split_row_product[4].strip()
    product_start_date = split_row_product[5].strip()

    return {
        "product_id": raw_product_id(product_id),
        "cat_id": raw_cat_id(cat_id),
        "prod_key": raw_prod_key(prod_key),
        "product_nm": raw_product_nm(product_nm),
        "product_cost": raw_product_cost(product_cost),
        "product_line": raw_product_line(product_line),
        "product_start_date": raw_product_start_date(product_start_date)
    }


def not_none4(x):
    """
    Keep only valid product rows.
    """
    return x is not None and x["product_id"] not in [None, "N/A"]


silver_layer_product_info = load_customer_prod.map(customer_product_info).filter(not_none4)

print("Sample product_details rows:")
print(silver_layer_product_info.take(5))

print("\n" + "=" * 30)
print("ACCUMULATOR RESULTS")
print("=" * 30)
print("Invalid accum_product_id         :", accum_product_id.value)
print("Invalid accum_cat_id             :", accum_cat_id.value)
print("Invalid accum_prod_key           :", accum_prod_key.value)
print("Invalid accum_product_nm         :", accum_product_nm.value)
print("Invalid accum_product_cost       :", accum_product_cost.value)
print("Invalid accum_product_line       :", accum_product_line.value)
print("Invalid accum_product_start_date :", accum_product_start_date.value)

# =============================================================================
# SECTION 6: sales_details
# =============================================================================
print("\n" + "=" * 30)
print("sales_details")
print("=" * 30)

# Bronze sales file contains order-level transaction details.
file_path_sales_info = r"C:\Users\antor\OneDrive\Desktop\data warehouse project\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\bronze_output\sales_details\*"
load_customer_sales = sc.textFile(file_path_sales_info, 4) \
                        .map(lambda line: literal_eval(line))

# Track invalid sales values.
accum_sls_ord_num = sc.accumulator(0)
accum_sls_prd_key = sc.accumulator(0)
accum_sls_cust_id = sc.accumulator(0)
accum_sls_order_date = sc.accumulator(0)
accum_sls_due_date = sc.accumulator(0)


def sales_order_num(sls_ord_num):
    """
    Validate sales order number format.
    Expected:
        starts with SO
        length 7
        alphanumeric
    """
    if sls_ord_num and sls_ord_num.strip():
        if len(sls_ord_num) == 7:
            if sls_ord_num.startswith("SO"):
                if sls_ord_num.isalnum():
                    return sls_ord_num
                else:
                    accum_sls_ord_num.add(1)
                    return "N/A"
            else:
                accum_sls_ord_num.add(1)
                return "N/A"
        else:
            accum_sls_ord_num.add(1)
            return "N/A"
    accum_sls_ord_num.add(1)
    return "N/A"


def sales_product_key(sls_prd_key):
    """
    Validate sales product key format.
    """
    if sls_prd_key:
        if len(sls_prd_key) > 2:
            if sls_prd_key[2] == "-":
                return sls_prd_key
            else:
                accum_sls_prd_key.add(1)
                return "N/A"
        else:
            accum_sls_prd_key.add(1)
            return "N/A"
    else:
        accum_sls_prd_key.add(1)
        return "N/A"


def sales_customer_id(sls_cust_id):
    """
    Validate customer id in sales data and convert it to integer.
    """
    if sls_cust_id:
        if len(sls_cust_id) == 5:
            return int(sls_cust_id)
        else:
            accum_sls_cust_id.add(1)
            return None
    else:
        accum_sls_cust_id.add(1)
        return None


def sales_order_date(sls_order_date):
    """
    Convert order date from YYYYMMDD into YYYY-MM-DD.
    """
    if sls_order_date:
        if len(sls_order_date) == 8:
            try:
                sub = sls_order_date[0:4] + "-" + sls_order_date[4:6] + "-" + sls_order_date[6:8]
                convert_time = datetime.strptime(sub, "%Y-%m-%d")
                return convert_time.strftime("%Y-%m-%d")
            except ValueError:
                accum_sls_order_date.add(1)
                return "N/A"
    else:
        return "N/A"


def sales_due_date(sls_due_date):
    """
    Convert due date from YYYYMMDD into YYYY-MM-DD.
    """
    if sls_due_date:
        if len(sls_due_date) == 8:
            try:
                sub = sls_due_date[0:4] + "-" + sls_due_date[4:6] + "-" + sls_due_date[6:8]
                convert_time1 = datetime.strptime(sub, "%Y-%m-%d")
                return convert_time1.strftime("%Y-%m-%d")
            except ValueError:
                accum_sls_due_date.add(1)
                return "N/A"
        else:
            return "N/A"
    else:
        return "N/A"


def clean_sales(sales_sls_price, sales_quantity, sales_price):
    """
    Make sure sales price, quantity, and total sales are consistent.
    If values are missing or invalid, replace them with safe defaults.
    """
    sales_sls_price = to_float(sales_sls_price)
    sales_quantity = to_float(sales_quantity)
    sales_price = to_float(sales_price)

    if sales_quantity is None or sales_quantity <= 0:
        sales_quantity = 1
    if sales_sls_price is None or sales_sls_price < 0:
        sales_sls_price = 0
    if sales_price is None or sales_price <= 0:
        sales_price = sales_sls_price / sales_quantity

    if round(sales_sls_price, 2) != round(sales_quantity * sales_price, 2):
        sales_sls_price = sales_quantity * sales_price

    return sales_sls_price, sales_quantity, sales_price


def sales_deatils(sls):
    """
    Convert one raw sales row into a cleaned dictionary.
    """
    if sls is None:
        return None

    raw_sales = sls.get("rawdata")
    if raw_sales is None:
        return None

    split_row_sales = raw_sales.split(",")
    if len(split_row_sales) < 9:
        return None

    sls_ord_num = split_row_sales[0].strip()
    sls_prd_key = split_row_sales[1].strip()
    sls_cust_id = split_row_sales[2].strip()
    sls_order_date = split_row_sales[3].strip()
    sls_due_date = split_row_sales[5].strip()
    sales_sls_price = split_row_sales[6].strip()
    sales_quantity = split_row_sales[7].strip()
    sales_price = split_row_sales[8].strip()

    fixed_sls_price, fixed_qty, fixed_price = clean_sales(sales_sls_price, sales_quantity, sales_price)

    return {
        "sls_ord_num": sales_order_num(sls_ord_num),
        "sls_prd_key": sales_product_key(sls_prd_key),
        "sls_cust_id": sales_customer_id(sls_cust_id),
        "sls_order_date": sales_order_date(sls_order_date),
        "sls_due_date": sales_due_date(sls_due_date),
        "sales_sls_price": fixed_sls_price,
        "sales_quantity": fixed_qty,
        "sales_price": fixed_price
    }


def not_none5(x):
    """
    Keep only sales rows with a valid order number.
    """
    return x is not None and x["sls_ord_num"] not in [None, "N/A"]


silver_layer_sales_info = load_customer_sales.map(sales_deatils).filter(not_none5)

print("Sample sales_details rows:")
print(silver_layer_sales_info.take(5))

print("\n" + "=" * 30)
print("ACCUMULATOR RESULTS")
print("=" * 30)
print("Invalid accum_sls_ord_num     :", accum_sls_ord_num.value)
print("Invalid accum_sls_prd_key     :", accum_sls_prd_key.value)
print("Invalid accum_sls_cust_id     :", accum_sls_cust_id.value)
print("Invalid accum_sls_order_date  :", accum_sls_order_date.value)
print("Invalid accum_sls_due_date    :", accum_sls_due_date.value)

# =============================================================================
# OUTPUT BASE PATH
# =============================================================================
# All cleaned Silver output folders will be created here.
base_output_path = r"C:\Users\antor\OneDrive\Desktop\data warehouse project\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\silver_layer_output"

if not os.path.exists(base_output_path):
    os.makedirs(base_output_path)

# =============================================================================
# SAVE CLEANED OUTPUTS
# =============================================================================
print("\n" + "=" * 70)
print("💾 SAVING SILVER OUTPUTS")
print("=" * 70)

# 1. Customer Info
cust_path = base_output_path + r"\customer_info"
if os.path.exists(cust_path):
    shutil.rmtree(cust_path)
silver_customer_info.saveAsTextFile(cust_path)
print("Saved customer_info ->", cust_path)

# 2. Customer Birth
birth_path = base_output_path + r"\customer_birth"
if os.path.exists(birth_path):
    shutil.rmtree(birth_path)
silver_layer.saveAsTextFile(birth_path)
print("Saved customer_birth ->", birth_path)

# 3. Customer Location
loc_path = base_output_path + r"\customer_location"
if os.path.exists(loc_path):
    shutil.rmtree(loc_path)
silver_layer_location.saveAsTextFile(loc_path)
print("Saved customer_location ->", loc_path)

# 4. Category Products
cat_path = base_output_path + r"\category_products"
if os.path.exists(cat_path):
    shutil.rmtree(cat_path)
silver_layer_category.saveAsTextFile(cat_path)
print("Saved category_products ->", cat_path)

# 5. Product Info
prod_path = base_output_path + r"\product_info"
if os.path.exists(prod_path):
    shutil.rmtree(prod_path)
silver_layer_product_info.saveAsTextFile(prod_path)
print("Saved product_info ->", prod_path)

# 6. Sales Details
sales_path = base_output_path + r"\sales_details"
if os.path.exists(sales_path):
    shutil.rmtree(sales_path)
silver_layer_sales_info.saveAsTextFile(sales_path)
print("Saved sales_details ->", sales_path)

print("\n" + "=" * 70)
print("🥈 SILVER LAYER — COMPLETE")
print("=" * 70)
print("All cleaned datasets saved successfully in:")
print(base_output_path)

spark.stop()
