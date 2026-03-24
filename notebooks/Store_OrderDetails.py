# Databricks notebook source
from pyspark.sql.functions import col, cast
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("StoreOrderDetails").getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Order Detail

# COMMAND ----------

order_details = spark.read.format("parquet").load("abfss://destination@storageadfkjupstart13.dfs.core.windows.net/raw/order_details")
order_details.printSchema()

# COMMAND ----------

order_details_snake_case = order_details.select(
    col("SalesOrderID").cast("int").alias("sales_order_id"),
    col("SalesOrderDetailID").cast("int").alias("sales_order_detail_id"),
    col("OrderQty").cast("int").alias("order_qty"),
    col("ProductID").cast("int").alias("product_id"),
    col("UnitPrice").cast("float").alias("unit_price"),
    col("UnitPriceDiscount").cast("string").alias("unit_price_discount")
)
order_details_snake_case.printSchema()

# COMMAND ----------

order_details_snake_case.write.format("parquet") \
  .mode("overwrite") \
  .save("abfss://destination@storageadfkjupstart13.dfs.core.windows.net/store/store_order_detail")