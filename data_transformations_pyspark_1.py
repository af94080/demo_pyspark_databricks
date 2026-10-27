# Databricks notebook source
# 1. 
 
# import time
# import pandas as pd
from pyspark.sql import DataFrame, Window, functions as F
 
CATALOG = spark.sql("SELECT current_catalog()").first()[0]
SCHEMA = "transform_demo"
 
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"USE {CATALOG}.{SCHEMA}")
print(f"Working in {CATALOG}.{SCHEMA}")
 
N_CUST, N_PROD, N_ORD = 20_000, 500, 1_000_000
 
 
def pick(values, seed):
    """Random choice from a Python list, expressed as a pure Spark column (no UDF)."""
    arr = F.array(*[F.lit(v) for v in values])
    return F.element_at(arr, (F.floor(F.rand(seed) * len(values)) + 1).cast("int"))
 
 
def bench(label: str, df: DataFrame) -> float:
    """Force full execution without storing anything, using the 'noop' sink, and time it."""
    t0 = time.perf_counter()
    df.write.format("noop").mode("overwrite").save()
    dt = time.perf_counter() - t0
    print(f"{label:<45}{dt:7.2f}s")
    return dt

# QA : the schema exists, the session points at it, and your helper functions work    

# COMMAND ----------

# 1.2 QA checks on previous cell
# check pick() on a small DataFrame
# expected output: no nulls, and a combination of a, b and c, with no nulls

test = (spark.range(10)
        .withColumn("choice", pick(["a", "b", "c"], seed=1)))
display(test)

# COMMAND ----------

#1.3 QA for bench(): 
# should print a label and a time under a few seconds. 
# shows that F and the noop write work

bench("sanity check", spark.range(1_000_000).withColumn("x", F.col("id") * 2))

# COMMAND ----------

# 1.4 QA check constants
print(N_CUST, N_PROD, N_ORD)   # 20000 500 1000000

# COMMAND ----------

#2. Ingest Bronze Data
#   Creates 5 dataframes: customers_raw, dups, orders_base, orders_raw, products_raw

domains = ["Gmail.com", "yahoo.com", "Outlook.com", "company.io"]
raw_countries = ["US", "usa ", "United States", "CA", "Canada", "UK", "GB", "DE", "Germany", "IN", "india"]
categories = ["Electronics", "Home", "Toys", "Books", "Fashion", "Sports"]
 
customers_raw = (
    spark.range(1, N_CUST + 1).withColumnRenamed("id", "customer_id")
    .withColumn("full_name", F.concat(F.lit("Customer_"), F.col("customer_id").cast("string")))
    .withColumn("email", F.concat(F.lit("  User"), F.col("customer_id").cast("string"),
                                  F.lit("@"), pick(domains, 1), F.lit(" ")))
    .withColumn("country", pick(raw_countries, 2))
    .withColumn("signup_date", F.date_add(F.lit("2022-01-01").cast("date"),
                                          F.floor(F.rand(3) * 1000).cast("int")))
)
 
products_raw = (
    spark.range(1, N_PROD + 1).withColumnRenamed("id", "product_id")
    .withColumn("product_name", F.concat(F.lit("Product_"), F.col("product_id").cast("string")))
    .withColumn("category", pick(categories, 4))
    .withColumn("base_price", F.round(5 + F.rand(5) * 195, 2))
)
 
orders_base = (
    spark.range(1, N_ORD + 1).withColumnRenamed("id", "order_id")
    # skew: ~20% of orders belong to customer 1
    .withColumn("customer_id", F.when(F.rand(7) < 0.2, F.lit(1))
                                .otherwise((F.rand(8) * N_CUST).cast("int") + 1))
    .withColumn("product_id", (F.rand(9) * N_PROD).cast("int") + 1)
    # ~1% invalid quantities (0)
    .withColumn("quantity", F.when(F.rand(11) < 0.01, F.lit(0)).otherwise((F.rand(10) * 5).cast("int") + 1))
    .withColumn("unit_price", F.round(5 + F.rand(12) * 195, 2))
    # ~70% null discounts
    .withColumn("discount", F.when(F.rand(13) < 0.3, F.round(F.rand(14) * 0.3, 2)))
    .withColumn("status", pick(["completed", "COMPLETED ", " shipped", "Cancelled", "returned", "pending"], 15))
    .withColumn("order_ts", (F.lit("2024-01-01").cast("timestamp").cast("long")
                             + (F.rand(16) * 365 * 86400).cast("long")).cast("timestamp"))
    .withColumn("ingested_at", F.current_timestamp())
)
 
# 2% of orders are re-delivered an hour later (duplicates with a newer ingested_at)
dups = (orders_base.sample(fraction=0.02, seed=3)
        .withColumn("ingested_at", F.col("ingested_at") + F.expr("INTERVAL 1 HOUR")))
orders_raw = orders_base.unionByName(dups)
 
customers_raw.write.mode("overwrite").saveAsTable("bronze_customers")
products_raw.write.mode("overwrite").saveAsTable("bronze_products")
orders_raw.write.mode("overwrite").saveAsTable("bronze_orders")
 
print("bronze_orders rows:", spark.table("bronze_orders").count())


# COMMAND ----------

#2.2 QA checks for the previous cell

print("customers:", spark.table("bronze_customers").count())   # 20,000
print("products :", spark.table("bronze_products").count())    # 500
print("orders   :", spark.table("bronze_orders").count())      # 1,020,140
print("dups     :", dups.count())                              # 20,140 (2% of 1M)

# check the schema looks right: 

spark.table("bronze_orders").printSchema()

# COMMAND ----------

CATALOG = spark.sql("SELECT current_catalog()").first()[0]
SCHEMA = "transform_demo"

spark.sql(f"USE {CATALOG}.{SCHEMA}")
print(f"Working in {CATALOG}.{SCHEMA}")

# COMMAND ----------

# 2.3 duplicate check 

display(spark.sql("""
    SELECT COUNT(*) AS total_rows,
           COUNT(DISTINCT order_id) AS distinct_orders,
           COUNT(*) - COUNT(DISTINCT order_id) AS duplicate_rows
    FROM bronze_orders
"""))
 
# total_rows	distinct_orders	duplicate_rows
# 1020140	    1000000	        20140


# COMMAND ----------

# 2.4 data quality check for inconsistent values

orders = spark.table("transform_demo.bronze_orders")

# status: 6 raw values, including ones with stray whitespace and mixed case
display(orders.groupBy("status").count().orderBy("status"))

# invalid quantities (~1%)
print("qty = 0:", orders.filter("quantity = 0").count())

# null discounts (~70%)
print("null discount %:", orders.filter("discount IS NULL").count() / orders.count())

# status	count
#  shipped	169952
# COMPLETED 	169535
# Cancelled	170173
# completed	169950
# pending	169906
# returned	170624

# COMMAND ----------

# 2.5 skew check 
display(orders.groupBy("customer_id").count().orderBy(F.desc("count")).limit(3))

# customer_id	count
# 1	            204540
# 2580	        71
# 1495	        68

# COMMAND ----------

# 2.6 check for dirty customer data 

display(spark.table("bronze_customers").select("email", "country").limit(8))
display(spark.table("bronze_customers").groupBy("country").count())

# email	country
#   User17501@Gmail.com 	UK
#   User17502@Outlook.com 	UK
#   User17503@yahoo.com 	Germany
#   User17504@Gmail.com 	usa 
#   User17505@yahoo.com 	United States
#   User17506@yahoo.com 	india
#   User17507@yahoo.com 	US
#   User17508@Gmail.com 	United States

# COMMAND ----------

# 2.7 value ranges sanity check
# 
display(orders.select(
    F.min("order_ts"), F.max("order_ts"),          # all within 2024
    F.min("unit_price"), F.max("unit_price"),      # ~5 to ~200
    F.min("customer_id"), F.max("customer_id"),    # 1 to 20000
    F.min("product_id"), F.max("product_id")       # 1 to 500
))

# COMMAND ----------

#2.8 check : three tables exist in bronze

display(spark.sql("SHOW TABLES"))

# database	tableName	isTemporary
# transform_demo	bronze_customers	false
# transform_demo	bronze_orders	false
# transform_demo	bronze_products	false

# COMMAND ----------

# MAGIC %md
# MAGIC # --------------------------------------
# MAGIC # SILVER LAYER
# MAGIC # --------------------------------------

# COMMAND ----------

 # 3.1 Build Silver lqyer by applying rules to standardize bronze data

COUNTRY_MAP = {  # config lives in plain Python; Spark sees it as a literal map (no join, no UDF)
    "us": "US", "usa": "US", "united states": "US",
    "ca": "CA", "canada": "CA",
    "uk": "GB", "gb": "GB",
    "de": "DE", "germany": "DE",
    "in": "IN", "india": "IN",
}
 
 
def standardize_country(df: DataFrame, col: str = "country") -> DataFrame:
    mapping = F.create_map(*[x for k, v in COUNTRY_MAP.items() for x in (F.lit(k), F.lit(v))])
    return df.withColumn(col, F.coalesce(mapping[F.lower(F.trim(col))], F.lit("UNKNOWN")))
 
 
def clean_email(df: DataFrame) -> DataFrame:
    return (df.withColumn("email", F.lower(F.trim("email")))
              .withColumn("email_domain", F.split("email", "@").getItem(1)))
 
 
def normalize_status(df: DataFrame) -> DataFrame:
    return df.withColumn("status", F.lower(F.trim("status")))
 
 
def dedupe_latest(df: DataFrame, key: str, order_col: str) -> DataFrame:
    """Keep the most recently ingested row per key."""
    w = Window.partitionBy(key).orderBy(F.col(order_col).desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
 
 
def add_amounts(df: DataFrame) -> DataFrame:
    gross = F.col("quantity") * F.col("unit_price")
    return (df.withColumn("order_date", F.to_date("order_ts"))
              .withColumn("gross_amount", F.round(gross, 2))
              .withColumn("net_amount", F.round(gross * (1 - F.coalesce("discount", F.lit(0.0))), 2)))
 
 
def split_valid(df: DataFrame, rule):
    """Quarantine pattern: route bad rows to a side table instead of silently dropping them."""
    return df.filter(rule), df.filter(~rule)
 
 
# --- run the pipeline
silver_customers = spark.table("bronze_customers").transform(standardize_country).transform(clean_email)
silver_products = spark.table("bronze_products")
 
orders_clean = (spark.table("bronze_orders")
                .transform(dedupe_latest, "order_id", "ingested_at")
                .transform(normalize_status)
                .transform(add_amounts)
                .drop("ingested_at"))
 
valid, rejected = split_valid(orders_clean, F.col("quantity") > 0)
 
silver_customers.write.mode("overwrite").saveAsTable("silver_customers")
silver_products.write.mode("overwrite").saveAsTable("silver_products")
rejected.write.mode("overwrite").saveAsTable("quarantine_orders")
 
# Mix SQL and Python: register a view, then let SQL create a *liquid-clustered* Delta table
valid.createOrReplaceTempView("orders_valid_stg")
 

# COMMAND ----------

#3.2 check for silver data

## check rows 

print("raw rows      :", spark.table("bronze_orders").count())
print("silver rows   :", spark.table("silver_orders").count(), "(deduped + valid)")
print("quarantined   :", spark.table("quarantine_orders").count())
display(spark.table("silver_orders").limit(5))



# COMMAND ----------

#3.3 build UDFs, python and pandas. serialize rows 
stress = (spark.range(3_000_000)
          .withColumn("email", F.concat(F.lit("  User"), F.col("id").cast("string"), F.lit("@"), pick(domains, 1))))
 
 
@F.udf("string")
def domain_udf(e):                      # row-at-a-time Python
    return e.strip().lower().split("@")[-1] if e else None
 
 
@F.pandas_udf("string")
def domain_pandas(s: pd.Series) -> pd.Series:   # vectorized over Arrow batches
    return s.str.strip().str.lower().str.split("@").str[-1]
 
 
builtin_expr = F.split(F.lower(F.trim("email")), "@").getItem(1)   # runs natively in the JVM / Photon
 
t_udf = bench("Python UDF", stress.select(domain_udf("email").alias("d")))
t_pandas = bench("pandas UDF (vectorized)", stress.select(domain_pandas("email").alias("d")))
t_builtin = bench("Built-in functions", stress.select(builtin_expr.alias("d")))

# Python UDF                                     19.76s
# pandas UDF (vectorized)                         5.11s
# Built-in functions                              0.35s