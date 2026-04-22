import os, sys
from pyspark.sql import SparkSession
from datetime import datetime
from ast import literal_eval  
import shutil

# -------------------------
# Spark Setup
# -------------------------
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

spark = SparkSession.builder \
    .appName("Silver_Layer") \
    .master("local[2]") \
    .config("spark.driver.memory", "1g") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
sc = spark.sparkContext
print("=" * 30)
print("customer_info_file")
print("=" * 30)
file_path_info1 = r"C:\Users\antor\OneDrive\Desktop\data warehouse project\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\silver_output\customer_info\*"
raw_rdd_customer_info = sc.textFile(file_path_info1, 4)\
                          .map((lambda x: literal_eval(x)))\
                          .keyBy(lambda x: x["customer_key_cleaned"])\
                              
print(raw_rdd_customer_info.take(1))
print("=" * 30)
print("birthday_file")
print("=" * 30)
file_path_info2 = r"C:\Users\antor\OneDrive\Desktop\data warehouse project\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\silver_output\customer_birth\*"
raw_rdd_customer_birth = sc.textFile(file_path_info2, 4)\
                     .map((lambda x: literal_eval(x)))\
                     .keyBy(lambda x: x["customer_key"])\
                         
print(raw_rdd_customer_birth.take(1))                     
print("=" * 30)
print("customer_location")
print("=" * 30)
file_path_info3 = r"C:\Users\antor\OneDrive\Desktop\data warehouse project\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\silver_output\customer_location\*"
raw_rdd_customer_location = sc.textFile(file_path_info3, 4)\
                     .map(lambda x: literal_eval(x))\
                     .keyBy(lambda x: x["customer_location_key"])\
                                  
print(raw_rdd_customer_location.take(1)) 

joined_info_birth = raw_rdd_customer_info.leftOuterJoin(raw_rdd_customer_birth)
joined_info_birth = joined_info_birth.leftOuterJoin(raw_rdd_customer_location)
print(joined_info_birth.take(1))


def resolve_gender(info_gender, birth_gender):
    if info_gender: 
        return info_gender
    elif birth_gender:
        return birth_gender
    else: 
        return None


def build_dim_customer(row):
    key, ((info, birth), loc) = row
    birth_gender = birth["gender"] if birth else None
    birth_date = birth["birth_date"] if birth else None
    birth_age = birth["age"] if birth else None
    country = None
    if loc:
        country = loc["customer_location_state"] if loc["customer_location_state"] else "Unknown"
    final_gender = resolve_gender(info["customer_gender_cleaned"], birth_gender)
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


gold_dim_customer = joined_info_birth.map(build_dim_customer)\
                                     .zipWithIndex()\
                                     .map(lambda x: {**x[0], "unique_number_customer": x[1] + 1})\
                                         
print("Sample gold_dim_customer:")
for row_1 in gold_dim_customer.take(5): 
        print("the clean_data are :", row_1)
print(f"Total customer records : {gold_dim_customer.count()}") 

print("=" *30)
print("customer_product_file")
print("=" * 30)
file_path_info4 = r"C:\Users\antor\OneDrive\Desktop\data warehouse project\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\silver_output\product_info\*"
raw_rdd_product_info = sc.textFile(file_path_info4, 4)\
                          .map((lambda x: literal_eval(x)))\
                          .keyBy(lambda x: x["cat_id"])

print("=" * 30)
print("category_file")
print("=" * 30)
file_path_info5 = r"C:\Users\antor\OneDrive\Desktop\data warehouse project\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\silver_output\category_products\*"
raw_rdd_customer_category = sc.textFile(file_path_info5, 4)\
                     .map((lambda x: literal_eval(x)))\
                     .keyBy(lambda x: x["customer_product_key1"])

joined_info_product = raw_rdd_product_info.leftOuterJoin(raw_rdd_customer_category)


def all_product(joined_info_product):
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


gold_dim_product = joined_info_product.map(all_product)\
                                     .zipWithIndex()\
                                     .map(lambda x: {**x[0], "unique_number": x[1] + 1})\
                                         
print("Sample gold_dim_customer:")
for row_2 in gold_dim_product.take(5): 
        print("the clean_data are :", row_2)
print(f"Total customer records : {gold_dim_product.count()}")

# ========================
# OUTPUT BASE PATH
# ========================
base_output_path = r"C:\Users\antor\OneDrive\Desktop\data warehouse project\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\gold_layer_output"

# create base folder if not exists
if not os.path.exists(base_output_path):
    os.makedirs(base_output_path)

# ========================
# SAVE EACH RDD
# ========================

# 1. Customer Info
cust_path = base_output_path + r"\gold_dim_customer"
if os.path.exists(cust_path):
    shutil.rmtree(cust_path)
gold_dim_customer.saveAsTextFile(cust_path)

# 2. Customer product
product_path = base_output_path + r"\gold_dim_product"
if os.path.exists(product_path):
    shutil.rmtree(product_path)
gold_dim_product.saveAsTextFile(product_path)

print("=" * 30)
print("sales_file")
print("=" * 30)
file_path_info6 = r"C:\Users\antor\OneDrive\Desktop\data warehouse project\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\silver_output\sales_details\*"
raw_rdd_sales_info = sc.textFile(file_path_info6, 4)\
                          .map((lambda x: literal_eval(x)))\
                          .keyBy(lambda x: x["sls_cust_id"])
                              
print(raw_rdd_sales_info.take(1))

print("=" * 30)
print("sales_prd_file")
print("=" * 30)
file_path_info7 = r"C:\Users\antor\OneDrive\Desktop\data warehouse project\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\silver_output\sales_details\*"
raw_rdd_sales_prd_info = sc.textFile(file_path_info7, 4)\
                          .map((lambda x: literal_eval(x)))\
                          .keyBy(lambda x: x["sls_prd_key"])
                              
print(raw_rdd_sales_prd_info.take(1))

print("=" * 30)
print("silver_customer_info")
print("=" * 30)
file_path_info8 = r"C:\Users\antor\OneDrive\Desktop\data warehouse project\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\gold_layer_output\gold_dim_customer\*"
raw_rdd_silver_customer = sc.textFile(file_path_info8, 4)\
                     .map((lambda x: literal_eval(x)))\
                     .keyBy(lambda x: x["customer_id" ])
                     
print(raw_rdd_silver_customer.take(1))
print("=" * 30)
print("silver_product_file")
print("=" * 30)
file_path_info9 = r"C:\Users\antor\OneDrive\Desktop\data warehouse project\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\gold_layer_output\gold_dim_product\*"
raw_rdd_silver_product = sc.textFile(file_path_info9, 4)\
                     .map((lambda x: literal_eval(x)))\
                     .keyBy(lambda x: x["prod_key"])
print(raw_rdd_silver_product.take(1))

joined_sales_with_cust = raw_rdd_sales_prd_info.leftOuterJoin(raw_rdd_silver_product)


def rekey(x):
    old_key = x[0]  
    sales_prod = x[1] 
    sales = x[1][0]  
    new_key = sales["sls_cust_id"]  

    return (new_key, x)


customer_rekey = joined_sales_with_cust.map(rekey)
# looks like: (21768, ('BK-R93R-62', (sales, product)))
joined_all = customer_rekey.leftOuterJoin(raw_rdd_silver_customer)
# looks like: (21768, (('BK-R93R-62', (sales, product)), customer))
print(joined_all.take(1))
# looks like: (21768, (('BK-R93R-62', (sales, product)), customer))


def all_sales(key):
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
    return data["order_number"] is not None  


gold_fact_sales = joined_all.map(all_sales).filter(filtering)
for row in gold_fact_sales.take(10):
    print(row)
print(f"Total sales records: {gold_fact_sales.count()}")

# ========================
# OUTPUT BASE PATH
# ========================
base_output_path = r"C:\Users\antor\OneDrive\Desktop\data warehouse project\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\gold_layer_output"

# create base folder if not exists
if not os.path.exists(base_output_path):
    os.makedirs(base_output_path)

# ========================
# SAVE EACH RDD
# ========================

# 1. Customer Info
gold_fact_path = base_output_path + r"\gold_fact_sales"
if os.path.exists(gold_fact_path):
    shutil.rmtree(gold_fact_path)
gold_fact_sales.saveAsTextFile(gold_fact_path)

# ─────────────────────────────────────────────────────────
# GOLD TABLE 4: report_revenue_by_country
# ─────────────────────────────────────────────────────────

print("=" * 30)
print("revenue by each country")
print("=" * 30)
file_path_info10 = r"C:\Users\antor\OneDrive\Desktop\data warehouse project\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\gold_layer_output\gold_fact_sales\*"
raw_rdd_sales_join_info = sc.textFile(file_path_info10, 4)\
                       .map((lambda x: literal_eval(x)))\
                       .keyBy(lambda x: x["customer_unique_num"])
                        
file_path_info8 = r"C:\Users\antor\OneDrive\Desktop\data warehouse project\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\gold_layer_output\gold_dim_customer\*"
raw_rdd_customer_join_info = sc.textFile(file_path_info8, 4)\
                          .map((lambda x: literal_eval(x)))\
                          .keyBy(lambda x: x["unique_number_customer"])
                          
join = raw_rdd_sales_join_info.leftOuterJoin(raw_rdd_customer_join_info)
print(join.take(1))


def mappin(metadata):
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
    return (row["customer_country"], row["sales_price"])


def merge_lists(a, b):
    return a + b


country_sales = join.map(mappin) \
    .map(map_country_sales) \
    .reduceByKey(merge_lists)
print(country_sales.take(10))

# ─────────────────────────────────────────────────────────
# GOLD TABLE 7: report_monthly_trend
# ─────────────────────────────────────────────────────────

print("=" * 30)
print("revenue by each month")
print("=" * 30)


def safe_parse(x):
    try:
        return literal_eval(x)
    except:
        return None


raw_rdd = sc.textFile(file_path_info10, 4) \
            .map(safe_parse) \
            .filter(lambda x: x is not None)


def monthly_trend(row):
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
    return (
        row["month"],
        {
            "sales": row["price"],
            "orders": 1,
            "month_num": row["month_num"]
        }
    )


def reduce_month(a, b):
    return {
        "sales": a["sales"] + b["sales"],
        "orders": a["orders"] + b["orders"],
        "month_num": a["month_num"]  # keep one
           }


monthly_data = raw_rdd \
    .map(monthly_trend) \
    .filter(lambda x: x is not None) \
    .map(to_month_pair) \
    .reduceByKey(reduce_month)

 
def format_output(x):
    return {
        "month": x[0],
        "total_sales": round(x[1]["sales"], 2),
        "total_orders": x[1]["orders"],
        "month_num": x[1]["month_num"]
            }


final_result = monthly_data.map(format_output) \
                           .sortBy(lambda x: x["month_num"])

print(final_result.take(10))


# ─────────────────────────────────────────────────────────
# GOLD TABLE 8: report_top_customers
# ─────────────────────────────────────────────────────────
def top_customer(data_1):
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
    return {
        "customer_name": a["customer_name"],
        "country": a["country"],
        "gender": a["gender"],
        "sales": a["sales"] + b["sales"],
        "orders": a["orders"] + b["orders"]
    }

    
def format_top_customers(format):
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
    return srt_id["Total_sales"]
    
    
top_customers = join\
                .map(top_customer)\
                .reduceByKey(reduce_top_customer)\
                .map(format_top_customers)\
                .sortBy(sorting, ascending=False)

print(top_customers.take(5))
