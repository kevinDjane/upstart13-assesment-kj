# Databricks notebook source
from pyspark.sql.functions import col, cast
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("StoreProducts").getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Products

# COMMAND ----------

products = spark.read.format("parquet").load("abfss://destination@storageadfkjupstart13.dfs.core.windows.net/raw/products")
products.printSchema()

# COMMAND ----------

products_snake_case = products.select(
    col("ProductID").cast("int").alias("product_id"),
    col("ProductDesc").cast("string").alias("product_desc"),
    col("ProductNumber").cast("string").alias("product_number"),
    col("MakeFlag").cast("boolean").alias("make_flag"),
    col("Color").cast("string").alias("color"),
    col("SafetyStockLevel").cast("int").alias("safety_stock_level"),
    col("ReorderPoint").cast("int").alias("reorder_point"),
    col("StandardCost").cast("float").alias("standard_cost"),
    col("ListPrice").cast("float").alias("list_price"),
    col("Size").cast("string").alias("size"),
    col("SizeUnitMeasureCode").cast("string").alias("size_unit_measure_code"),
    col("Weight").cast("double").alias("weight"),
    col("WeightUnitMeasureCode").cast("string").alias("weight_unit_measure_code"),
    col("ProductCategoryName").cast("string").alias("product_category_name"),
    col("ProductSubCategoryName").cast("string").alias("product_sub_category_name")
)
products_snake_case.printSchema()

# COMMAND ----------

products_snake_case.write.format("parquet") \
  .mode("overwrite") \
  .save("abfss://destination@storageadfkjupstart13.dfs.core.windows.net/store/store_products")