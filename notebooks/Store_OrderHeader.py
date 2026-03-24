# Databricks notebook source
from pyspark.sql.functions import col, cast
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("StoreOrderHeader").getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Order Header

# COMMAND ----------

order_headers = spark.read.format("parquet").load("abfss://destination@storageadfkjupstart13.dfs.core.windows.net/raw/order_headers")
order_headers.printSchema()

# COMMAND ----------

order_headers_lower_case = order_headers.select(
    col("SalesOrderID").cast("int").alias("sales_order_id"),
    col("OrderDate").cast("date").alias("order_date"),
    col("ShipDate").cast("string").alias("ship_date"),
    col("OnlineOrderFlag").cast("boolean").alias("online_order_flag"),
    col("AccountNumber").cast("string").alias("account_number"),
    col("CustomerID").cast("int").alias("customer_id"),
    col("SalesPersonID").cast("int").alias("sales_person_id"),
    col("Freight").cast("float").alias("freight")
)
order_headers_lower_case.printSchema()

# COMMAND ----------

order_headers_lower_case.write.format("parquet") \
  .mode("overwrite") \
  .save("abfss://destination@storageadfkjupstart13.dfs.core.windows.net/store/store_order_headers")