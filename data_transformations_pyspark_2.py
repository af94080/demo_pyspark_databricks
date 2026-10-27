# Databricks notebook source
# GOLD LAYER 
# creates two tables in gold layer:
#   gold_monthly_category_revenue
#   gold_customer_rfm
#

# make sure you are in the right schema : transform_demo

from pyspark.sql import functions as F 

CATALOG, SCHEMA = "workspace", "transform_demo"
spark.sql(f"USE {CATALOG}.{SCHEMA}")

print(spark.sql("SELECT current_schema()").first()[0]) # transform_demo

print([r.tableName for r in spark.sql("SHOW TABLES").collect()])






# COMMAND ----------

# MAGIC %sql
# MAGIC -- 
# MAGIC --  4.1 create gold layer : gold_monthly_category_revenue
# MAGIC -- 
# MAGIC CREATE OR REPLACE TABLE gold_monthly_category_revenue AS
# MAGIC WITH base AS (
# MAGIC   SELECT trunc(o.order_date, 'MM')   AS month,
# MAGIC          p.category,
# MAGIC          SUM(o.net_amount)           AS revenue,
# MAGIC          COUNT(DISTINCT o.order_id)  AS orders
# MAGIC   FROM silver_orders o
# MAGIC   JOIN silver_products p USING (product_id)
# MAGIC   WHERE o.status IN ('completed', 'shipped')
# MAGIC   GROUP BY 1, 2
# MAGIC )
# MAGIC SELECT *,
# MAGIC        SUM(revenue) OVER (PARTITION BY category ORDER BY month)                                AS running_revenue,
# MAGIC        ROUND(revenue / LAG(revenue) OVER (PARTITION BY category ORDER BY month) - 1, 4)        AS mom_growth
# MAGIC FROM base;
# MAGIC
# MAGIC SELECT * FROM gold_monthly_category_revenue ORDER BY category, month LIMIT 24;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 
# MAGIC -- 4.2 GOLD LAYER: Top 3 products per category
# MAGIC -- 
# MAGIC SELECT p.category, p.product_name,
# MAGIC        ROUND(SUM(o.net_amount), 2) AS revenue,
# MAGIC        RANK() OVER (PARTITION BY p.category ORDER BY SUM(o.net_amount) DESC) AS rnk
# MAGIC FROM silver_orders o
# MAGIC JOIN silver_products p ON o.product_id = p.product_id
# MAGIC WHERE o.status IN ('completed', 'shipped')
# MAGIC GROUP BY p.category, p.product_name
# MAGIC QUALIFY rnk <= 3
# MAGIC ORDER BY p.category, rnk;

# COMMAND ----------

# 4.3 GOLD : gold_customer_rfm : customer RFM scoring 

rfm = spark.sql("""
    WITH agg AS (
      SELECT customer_id,
             DATEDIFF(DATE'2025-01-01', MAX(order_date)) AS recency_days,
             COUNT(*)                                    AS frequency,
             SUM(net_amount)                             AS monetary
      FROM silver_orders
      WHERE status IN ('completed', 'shipped')
      GROUP BY customer_id
    )
    SELECT *,
           NTILE(5) OVER (ORDER BY recency_days DESC) AS r_score,
           NTILE(5) OVER (ORDER BY frequency)         AS f_score,
           NTILE(5) OVER (ORDER BY monetary)          AS m_score
    FROM agg
""")

rfm_segments = rfm.withColumn(
    "segment",
    F.when((F.col("r_score") >= 4) & (F.col("f_score") >= 4) & (F.col("m_score") >= 4), "champion")
     .when(F.col("r_score") <= 2, "at_risk")
     .otherwise("regular"))

rfm_segments.write.mode("overwrite").saveAsTable("gold_customer_rfm")
display(rfm_segments.groupBy("segment").agg(F.count("*").alias("customers"),
                                            F.round(F.avg("monetary"), 2).alias("avg_monetary")))

# COMMAND ----------



# COMMAND ----------

