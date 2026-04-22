import os, sys
from pyspark.sql import SparkSession
from datetime import datetime
from ast import literal_eval  
import shutil

# -------------------------
# Spark Setup
# -------------------------
# Make sure Spark workers use the same Python interpreter as the driver.
# This avoids worker-side Python mismatch issues when Spark runs transformations.
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# Create the Spark session for Gold layer processing.
# local[2] means Spark will use 2 CPU threads locally.
# driver.memory and shuffle.partitions are tuned for local development.
spark = SparkSession.builder \
    .appName("Silver_Layer") \
    .master("local[2]") \
    .config("spark.driver.memory", "1g") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

# Keep terminal output clean and focused on errors only.
spark.sparkContext.setLogLevel("ERROR")
sc = spark.sparkContext

# =============================================================================
# SECTION 1: CUSTOMER DIMENSION
# =============================================================================
print("=" * 30)
print("customer_info_file")
print("=" * 30)

# Read the cleaned Silver customer_info output.
# The saved rows are text representations of dictionaries, so literal_eval() converts
# each line back into a Python dictionary.
# keyBy() creates the join key using customer_key_cleaned.
file_path_info1 = r"C:\Users\antor\OneDrive\Desktop\data warehouse project\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\silver_output\customer_info\*"
raw_rdd_customer_info = sc.textFile(file_path_info1, 4)\
                          .map((lambda x: literal_eval(x)))\
                          .keyBy(lambda x: x["customer_key_cleaned"])

# Print one sample row so the structure can be checked before joining.
print(raw_rdd_customer_info.take(1))

print("=" * 30)
print("birthday_file")
print("=" * 30)

# Read the cleaned Silver customer_birth dataset.
# This file contains birth date, gender, and age details.
file_path_info2 = r"C:\Users\antor\OneDrive\Desktop\data warehouse project\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\silver_output\customer_birth\*"
raw_rdd_customer_birth = sc.textFile(file_path_info2, 4)\
                     .map((lambda x: literal_eval(x)))\
                     .keyBy(lambda x: x["customer_key"])

# Validate structure of birth dataset.
print(raw_rdd_customer_birth.take(1))                     

print("=" * 30)
print("customer_location")
print("=" * 30)

# Read the cleaned Silver customer_location dataset.
# This file contains the customer's location/country information.
file_path_info3 = r"C:\Users\antor\OneDrive\Desktop\data warehouse project\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\silver_output\customer_location\*"
raw_rdd_customer_location = sc.textFile(file_path_info3, 4)\
                     .map(lambda x: literal_eval(x))\
                     .keyBy(lambda x: x["customer_location_key"])

# Validate structure of location dataset.
print(raw_rdd_customer_location.take(1)) 

# Join customer_info with birth data first.
# Result structure after first join:
# (key, (info, birth))
joined_info_birth = raw_rdd_customer_info.leftOuterJoin(raw_rdd_customer_birth)

# Join the result with customer_location.
# Final structure:
# (key, ((info, birth), loc))
joined_info_birth = joined_info_birth.leftOuterJoin(raw_rdd_customer_location)

# Print one joined record to validate the nested structure.
print(joined_info_birth.take(1))


def resolve_gender(info_gender, birth_gender):
    """
    Pick the best available gender value.

    Priority order:
        1. customer_info gender
        2. birth file gender
        3. None if both are missing

    This helper is used so the Gold layer keeps the most complete value
    without losing data when one source is missing.
    """
    if info_gender: 
        return info_gender
    elif birth_gender:
        return birth_gender
    else: 
        return None


def build_dim_customer(row):
    """
    Build one final customer dimension row from the joined Silver datasets.

    Input structure:
        key, ((info, birth), loc)

    Output structure:
        {
            customer_key,
            customer_id,
            customer_name,
            marital_status,
            gender,
            customer_since,
            birth_date,
            age,
            country
        }
    """
    # Unpack the nested joined tuple into readable variables.
    key, ((info, birth), loc) = row

    # Safely extract values from optional joined datasets.
    birth_gender = birth["gender"] if birth else None
    birth_date = birth["birth_date"] if birth else None
    birth_age = birth["age"] if birth else None

    # If location exists, use the standardized country/state value.
    # Otherwise mark it as Unknown.
    country = None
    if loc:
        country = loc["customer_location_state"] if loc["customer_location_state"] else "Unknown"

    # Resolve gender using the helper function above.
    final_gender = resolve_gender(info["customer_gender_cleaned"], birth_gender)

    # Build the final customer dimension row.
    return {
        "customer_key": key,
        "customer_id": info["customer_id_cleaned"],
        "customer_name": info["customer_name_cleaned"],
        "marital_status": info["customer_martial_cleaned"],
        "gender": final_gender,
        "customer_since": info["customer_date"],
        "birth_date": birth_date,
        "age": birth_age,
        "country": country
    }


# Add a unique sequential number to each customer row.
# This acts like a surrogate key / row number for the dimension table.
gold_dim_customer = joined_info_birth.map(build_dim_customer)\
                                     .zipWithIndex()\
                                     .map(lambda x: {**x[0], "unique_number_customer": x[1] + 1})
                                         
# Show sample output and count for validation.
print("Sample gold_dim_customer:")
for row_1 in gold_dim_customer.take(5): 
        print("the clean_data are :", row_1)
print(f"Total customer records : {gold_dim_customer.count()}") 

# =============================================================================
# SECTION 2: PRODUCT DIMENSION
# =============================================================================
print("=" *30)
print("customer_product_file")
print("=" *30)

# Read cleaned product_info dataset from Silver layer.
# Keying is done using cat_id so it can join with category_products.
file_path_info4 = r"C:\Users\antor\OneDrive\Desktop\data warehouse project\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\silver_output\product_info\*"
raw_rdd_product_info = sc.textFile(file_path_info4, 4)\
                          .map((lambda x: literal_eval(x)))\
                          .keyBy(lambda x: x["cat_id"])

print("=" *30)
print("category_file")
print("=" *30)

# Read cleaned category_products dataset from Silver layer.
# Keying is done using customer_product_key1 so it matches product-related rows.
file_path_info5 = r"C:\Users\antor\OneDrive\Desktop\data warehouse project\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\silver_output\category_products\*"
raw_rdd_customer_category = sc.textFile(file_path_info5, 4)\
                     .map((lambda x: literal_eval(x)))\
                     .keyBy(lambda x: x["customer_product_key1"])

# Join product details with category information.
joined_info_product = raw_rdd_product_info.leftOuterJoin(raw_rdd_customer_category)


def all_product(joined_info_product):
    """
    Build one final product dimension row.

    Input structure:
        key, (product, category)

    Output includes:
        - product_id
        - prod_key
        - product_nm
        - product_key
        - category
        - sub_category
        - maintance
        - product_cost
        - product_line
        - product_start_date
    """
    key, (product, category) = joined_info_product  
    customer_category = category["customer_category1"] if category else "N/A"
    sub_category = category["customer_sub_category1"] if category else "N/A"
    maintance = category["customer_category_maintance1"] if category else "N/A"

    return {
        "product_id": product["product_id"],
        "prod_key": product["prod_key"],
        "product_nm": product["product_nm"],
        "product_key": key,
        "category": customer_category,
        "sub_category": sub_category,
        "maintance": maintance,
        "product_cost": product["product_cost"],
        "product_line": product["product_line"],
        "product_start_date": product["product_start_date"],
         
    }


# Add a unique sequential number to each product row.
# This becomes a stable surrogate key for the product dimension.
gold_dim_product = joined_info_product.map(all_product)\
                                     .zipWithIndex()\
                                     .map(lambda x: {**x[0], "unique_number": x[1] + 1})
                                         
# Show sample output and count for validation.
print("Sample gold_dim_customer:")
for row_2 in gold_dim_product.take(5): 
        print("the clean_data are :", row_2)
print(f"Total customer records : {gold_dim_product.count()}")

# ========================
# OUTPUT BASE PATH
# ========================
# Base folder where all Gold layer outputs will be stored.
base_output_path = r"C:\Users\antor\OneDrive\Desktop\data warehouse project\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\gold_layer_output"

# Create base folder if it does not exist.
if not os.path.exists(base_output_path):
    os.makedirs(base_output_path)

# ========================
# SAVE EACH DIMENSION
# ========================

# Save customer dimension.
cust_path = base_output_path + r"\gold_dim_customer"
if os.path.exists(cust_path):
    shutil.rmtree(cust_path)
gold_dim_customer.saveAsTextFile(cust_path)

# Save product dimension.
product_path = base_output_path + r"\gold_dim_product"
if os.path.exists(product_path):
    shutil.rmtree(product_path)
gold_dim_product.saveAsTextFile(product_path)

# =============================================================================
# SECTION 3: FACT TABLE — SALES
# =============================================================================
print("=" * 30)
print("sales_file")
print("=" * 30)

# Read the Silver sales_details dataset.
# Keying by sls_cust_id so later it can connect to customer dimension.
file_path_info6 = r"C:\Users\antor\OneDrive\Desktop\data warehouse project\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\silver_output\sales_details\*"
raw_rdd_sales_info = sc.textFile(file_path_info6, 4)\
                          .map((lambda x: literal_eval(x)))\
                          .keyBy(lambda x: x["sls_cust_id"])
                              
# Show one sample sales row.
print(raw_rdd_sales_info.take(1))

print("=" * 30)
print("sales_prd_file")
print("=" * 30)

# Read sales again, this time keyed by product key.
# This is used to join sales with product dimension.
file_path_info7 = r"C:\Users\antor\OneDrive\Desktop\data warehouse project\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\silver_output\sales_details\*"
raw_rdd_sales_prd_info = sc.textFile(file_path_info7, 4)\
                          .map((lambda x: literal_eval(x)))\
                          .keyBy(lambda x: x["sls_prd_key"])
                              
# Show one sample sales-product keyed row.
print(raw_rdd_sales_prd_info.take(1))

print("=" * 30)
print("silver_customer_info")
print("=" * 30)

# Load gold_dim_customer back from disk so it can be joined into the fact table.
# Keying is done using customer_id.
file_path_info8 = r"C:\Users\antor\OneDrive\Desktop\data warehouse project\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\gold_layer_output\gold_dim_customer\*"
raw_rdd_silver_customer = sc.textFile(file_path_info8, 4)\
                     .map((lambda x: literal_eval(x)))\
                     .keyBy(lambda x: x["customer_id" ])
                     
# Validate customer dimension structure.
print(raw_rdd_silver_customer.take(1))

print("=" * 30)
print("silver_product_file")
print("=" * 30)

# Load gold_dim_product back from disk so it can be joined into the fact table.
# Keying is done using prod_key.
file_path_info9 = r"C:\Users\antor\OneDrive\Desktop\data warehouse project\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\gold_layer_output\gold_dim_product\*"
raw_rdd_silver_product = sc.textFile(file_path_info9, 4)\
                     .map((lambda x: literal_eval(x)))\
                     .keyBy(lambda x: x["prod_key"])

# Validate product dimension structure.
print(raw_rdd_silver_product.take(1))

# Join sales rows with product dimension first.
# This gives sales + product details in one row.
joined_sales_with_cust = raw_rdd_sales_prd_info.leftOuterJoin(raw_rdd_silver_product)


def rekey(x):
    """
    Rebuild the join key so that sales can later be joined to customer dimension.

    Input structure:
        (product_key, (sales, product))

    Output structure:
        (customer_id, original_tuple)

    This allows the next join to happen on customer id.
    """
    old_key = x[0]  
    sales_prod = x[1] 
    sales = x[1][0]  
    new_key = sales["sls_cust_id"]  

    return (new_key, x)


# Re-key the sales-product join output by customer id.
customer_rekey = joined_sales_with_cust.map(rekey)

# Example structure after rekey:
# (21768, ('BK-R93R-62', (sales, product)))
joined_all = customer_rekey.leftOuterJoin(raw_rdd_silver_customer)

# Example final structure:
# (21768, (('BK-R93R-62', (sales, product)), customer))
print(joined_all.take(1))
# looks like: (21768, (('BK-R93R-62', (sales, product)), customer))


def all_sales(key):
    """
    Build one final fact_sales row.

    Input structure:
        cust_id, ((prod_id, (sales, prod)), customer)

    Output:
        {
            order_number,
            customer_unique_num,
            product_unique_num,
            order_date,
            order_shipping_date,
            sales_amount,
            sales_quantity,
            price
        }
    """
    cust_id, ((prod_id, (sales, prod)), customer) = key
    product_unique_number = prod["unique_number"] if prod else "N/A"
    customer_unique_number = customer["unique_number_customer"] if customer else "N/A"
    return {
        "order_number": sales["sls_ord_num"],
        "customer_unique_num": customer_unique_number,
        "product_unique_num": product_unique_number,
        "order_date": sales["sls_order_date"],
        "order_shipping_date": sales["sls_due_date"],
        "sales_amount": sales["sales_sls_price"],
        "sales_quantity": sales["sales_quantity"],
        "price": sales["sales_price"]
    }


def filtering(data):
    """
    Keep only valid sales rows that still have an order number.
    """
    return data["order_number"] is not None  


# Build the final fact_sales table.
gold_fact_sales = joined_all.map(all_sales).filter(filtering)

# Print sample fact rows and total record count.
for row in gold_fact_sales.take(10):
    print(row)
print(f"Total sales records: {gold_fact_sales.count()}")

# Save fact table to Gold output.
gold_fact_path = base_output_path + r"\gold_fact_sales"
if os.path.exists(gold_fact_path):
    shutil.rmtree(gold_fact_path)
gold_fact_sales.saveAsTextFile(gold_fact_path)

# =============================================================================
# SECTION 4: REPORT — REVENUE BY COUNTRY
# =============================================================================
print("=" * 30)
print("revenue by each country")
print("=" * 30)

# Reload fact table from disk so it can be joined with customer dimension again.
# Here the grouping target is customer country.
file_path_info10 = r"C:\Users\antor\OneDrive\Desktop\data warehouse project\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\gold_layer_output\gold_fact_sales\*"
raw_rdd_sales_join_info = sc.textFile(file_path_info10, 4)\
                       .map((lambda x: literal_eval(x)))\
                       .keyBy(lambda x: x["customer_unique_num"])
                        
# Reload customer dimension for country lookup.
file_path_info8 = r"C:\Users\antor\OneDrive\Desktop\data warehouse project\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\gold_layer_output\gold_dim_customer\*"
raw_rdd_customer_join_info = sc.textFile(file_path_info8, 4)\
                          .map((lambda x: literal_eval(x)))\
                          .keyBy(lambda x: x["unique_number_customer"])
                          
# Join fact rows with customer dimension rows.
join = raw_rdd_sales_join_info.leftOuterJoin(raw_rdd_customer_join_info)
print(join.take(1))


def mappin(metadata):
    """
    Convert the joined fact + customer row into a cleaner business reporting shape.
    This extracts country, customer profile fields, and sales price.
    """
    customer_unique_id, (sales, customer) = metadata
    customer_name = customer["customer_name"] if customer else "N/A"
    customer_gender = customer["gender"] if customer else "N/A"
    customer_age = customer["age"] if customer else "N/A"
    customer_country = customer["country"] if customer else "N/A"

    return {
        "customer_unique_NUM": customer_unique_id,
        "customer_name": customer_name,
        "customer_gender": customer_gender,
        "customer_age": customer_age,
        "customer_country": customer_country,
        "sales_price": sales["price"]
    }


def map_country_sales(row):
    """
    Convert the report row into a (country, sales_price) pair.
    This is the key step that allows reduceByKey() to group by country.
    """
    return (row["customer_country"], row["sales_price"])


def merge_lists(a, b):
    """
    Aggregate sales values for the same country.
    Since the mapped value is a number, this function is used to add values together.
    """
    return a + b


# Group sales by country.
country_sales = join.map(mappin) \
    .map(map_country_sales) \
    .reduceByKey(merge_lists)

print(country_sales.take(10))

# =============================================================================
# SECTION 5: REPORT — MONTHLY TREND
# =============================================================================
print("=" * 30)
print("revenue by each month")
print("=" * 30)


# Safe parse helper so a bad row does not crash the job.
def safe_parse(x):
    try:
        return literal_eval(x)
    except:
        return None


# Read fact table again for monthly analysis.
raw_rdd = sc.textFile(file_path_info10, 4) \
            .map(safe_parse) \
            .filter(lambda x: x is not None)


def monthly_trend(row):
    """
    Extract the order month from the order_date field.

    Input:
        row dictionary from fact_sales

    Output:
        {
            "month": <Month Name>,
            "month_num": <Month Number>,
            "price": <Sales Price>
        }
    """
    order_date = row.get("order_date")
    if not order_date:
        return None
    if isinstance(order_date, str):
        date_obj = datetime.strptime(order_date, "%Y-%m-%d")
    else:
        date_obj = order_date
    month_name = date_obj.strftime("%B")
    return {
        "month": month_name,
        "month_num": date_obj.month,  # for sorting
        "price": float(row.get("price", 0))
           }


def to_month_pair(row):
    """
    Convert monthly record into key-value format for grouping.
    Key   = month name
    Value = sales + order count
    """
    return (
        row["month"],
        {
            "sales": row["price"],
            "orders": 1,
            "month_num": row["month_num"]
        }
    )


def reduce_month(a, b):
    """
    Combine monthly aggregates:
        - sales are added
        - orders are counted
        - month_num is preserved for sorting
    """
    return {
        "sales": a["sales"] + b["sales"],
        "orders": a["orders"] + b["orders"],
        "month_num": a["month_num"]  # keep one
           }


# Build month-wise aggregation.
monthly_data = raw_rdd \
    .map(monthly_trend) \
    .filter(lambda x: x is not None) \
    .map(to_month_pair) \
    .reduceByKey(reduce_month)

 
def format_output(x):
    """
    Convert the reduced monthly result into final readable report format.
    """
    return {
        "month": x[0],
        "total_sales": round(x[1]["sales"], 2),
        "total_orders": x[1]["orders"],
        "month_num": x[1]["month_num"]
            }


# Sort by month number so output stays in calendar order.
final_result = monthly_data.map(format_output) \
                           .sortBy(lambda x: x["month_num"])

print(final_result.take(10))


# =============================================================================
# SECTION 6: REPORT — TOP CUSTOMERS
# =============================================================================
def top_customer(data_1):
    """
    Extract customer-level sales details from the joined report input.

    Input:
        key_1, (sale_1, cust_1)

    Output:
        (customer_id, {customer profile + sales + orders})
    """
    key_1, (sale_1, cust_1) = data_1

    customer_name1 = cust_1["customer_name"] if cust_1 else "N/A"
    customer_id1 = key_1 if key_1 else "N/A"
    country1 = cust_1["country"] if cust_1 else "N/A"
    gender1 = cust_1["gender"] if cust_1 else "N/A"
    sales1 = float(sale_1["price"]) if sale_1 else 0

    return (
        customer_id1,
        {
            "customer_name": customer_name1,
            "country": country1,
            "gender": gender1,
            "sales": sales1,
            "orders": 1
        })


def reduce_top_customer(a, b):
    """
    Merge multiple sales rows for the same customer.
    This adds total sales and total order count while keeping profile fields.
    """
    return {
        "customer_name": a["customer_name"],
        "country": a["country"],
        "gender": a["gender"],
        "sales": a["sales"] + b["sales"],
        "orders": a["orders"] + b["orders"]
    }

    
def format_top_customers(format):
    """
    Convert the reduced customer record into final reporting format.
    """
    (key_2, format_top) = format 
    return {
             "customer_id": key_2,
             "country": format_top["country"],
             "gender": format_top["gender"],
             "Total_sales": round(format_top["sales"], 2),
             "Total_orders":round(format_top["orders"], 2),
             "avg_order_value": round(format_top["sales"] / format_top["orders"], 2)
           }

    
def sorting(srt_id):
    """
    Return the field used for ranking customers.
    Sorting is done by Total_sales in descending order later.
    """
    return srt_id["Total_sales"]
    
    
# Build top customer report:
# 1. extract customer sales
# 2. aggregate by customer id
# 3. format the result
# 4. sort by total sales descending
top_customers = join\
                .map(top_customer)\
                .reduceByKey(reduce_top_customer)\
                .map(format_top_customers)\
                .sortBy(sorting, ascending=False)

print(top_customers.take(5))
