# Databricks notebook source
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("DataAnalysis").getOrCreate()

base_path = "abfss://destination@storageadfkjupstart13.dfs.core.windows.net/publish"

publish_products = spark.read.format("parquet").load(f"{base_path}/publish_products")
publish_products.createOrReplaceTempView("publish_products")

publish_orders = spark.read.format("parquet").load(f"{base_path}/publish_orders")
publish_orders.createOrReplaceTempView("publish_orders")

# COMMAND ----------

# MAGIC %md
# MAGIC # Which color generated the highest revenue each year?

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH color_sales AS (
# MAGIC     SELECT 
# MAGIC         date_format(orders.order_date, 'yyyy') AS year, --We assume the order_date column as the date of sale
# MAGIC         products.color,
# MAGIC         SUM(
# MAGIC           orders.total_line_extended_price - -- OrderQty * (UnitPrice - UnitPriceDiscount)
# MAGIC           (orders.order_qty * products.standard_cost) -- Cost of selling this product (quantity * unit cost)
# MAGIC         ) AS total_net_revenue
# MAGIC     FROM publish_orders AS orders
# MAGIC     INNER JOIN publish_products AS products 
# MAGIC         ON orders.product_id = products.product_id
# MAGIC     GROUP BY
# MAGIC       date_format(orders.order_date, 'yyyy'), --year
# MAGIC       products.color
# MAGIC )
# MAGIC SELECT 
# MAGIC     year,
# MAGIC     color,
# MAGIC     ROUND(total_net_revenue, 3) AS total_net_revenue
# MAGIC FROM color_sales
# MAGIC QUALIFY RANK() OVER (PARTITION BY year ORDER BY total_net_revenue DESC) = 1
# MAGIC ORDER BY year DESC

# COMMAND ----------

# MAGIC %md
# MAGIC # What is the average LeadTimeInBusinessDays by ProductCategoryName?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   products.product_category_name,
# MAGIC   ROUND(AVG(orders.lead_time_in_business_days), 3) AS avg_lead_time_in_business_days
# MAGIC FROM publish_orders AS orders
# MAGIC INNER JOIN publish_products AS products 
# MAGIC   ON orders.product_id = products.product_id
# MAGIC GROUP BY products.product_category_name
# MAGIC ORDER BY avg_lead_time_in_business_days DESC