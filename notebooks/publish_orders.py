# Databricks notebook source
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("publish_orders").getOrCreate()

# COMMAND ----------

base_path = "abfss://destination@storageadfkjupstart13.dfs.core.windows.net/store"
order_details = spark.read.format("parquet").load(f"{base_path}/store_order_detail")
order_headers = spark.read.format("parquet").load(f"{base_path}/store_order_headers")

order_details.createOrReplaceTempView("order_details")
order_headers.createOrReplaceTempView("order_headers")

# COMMAND ----------

publish_orders = spark.sql("""
SELECT
  d.sales_order_id,
  d.sales_order_detail_id,
  d.order_qty,
  d.product_id,
  d.unit_price,
  d.unit_price_discount,
  h.order_date,
  h.ship_date,
  h.online_order_flag,
  h.account_number,
  h.customer_id,
  h.sales_person_id,
  h.freight AS total_order_freight,
  size(
    filter(
      sequence(CAST(order_date AS DATE), CAST(ship_date AS DATE)),
      d -> dayofweek(d) NOT IN (1, 7)
    )
  ) - (
    CASE
      WHEN dayofweek(CAST(order_date AS DATE)) NOT IN (1, 7)
      THEN 1
      ELSE 0
    END
  ) AS lead_time_in_business_days, --the difference between OrderDate and ShipDate, excluding Saturdays and Sundays.
  d.order_qty * (
    CAST(d.unit_price AS float) -
    CAST(d.unit_price_discount AS float)
  ) AS total_line_extended_price --OrderQty * (UnitPrice - UnitPriceDiscount)
FROM order_details AS d
JOIN order_headers AS h
  ON d.sales_order_id = h.sales_order_id
""")

# COMMAND ----------

publish_orders.write.format("parquet") \
  .mode("overwrite") \
  .save("abfss://destination@storageadfkjupstart13.dfs.core.windows.net/publish/publish_orders")