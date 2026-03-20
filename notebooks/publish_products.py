# Databricks notebook source
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("publish_products").getOrCreate()

# COMMAND ----------

from pyspark.sql.functions import coalesce, nullif, trim, col, lit, when

# COMMAND ----------

store_products = spark.read.format("parquet").load("abfss://destination@storageadfkjupstart13.dfs.core.windows.net/store/store_products")

# COMMAND ----------

publish_products = store_products.withColumns(
    {
        "color": coalesce(nullif(trim(col("color")), lit("")), lit("N/A")),
        "product_category_name":
            when(
                nullif(trim(col("product_category_name")), lit("")).isNull(),
                when(
                    col("product_sub_category_name").isin(
                        ['Gloves', 'Shorts', 'Socks', 'Tights', 'Vests']
                    ),
                    "Clothing"
                )
                .when(
                    col("product_sub_category_name").isin(
                        ['Locks', 'Lights', 'Headsets', 'Helmets', 'Pedals', 'Pumps']
                    ),
                    "Accessories"
                )
                .when(
                    col("product_sub_category_name").isin(['Wheels', 'Saddles']) |
                    col("product_sub_category_name").like("%Frames%"),
                    "Components"
                )
                .otherwise(lit("Other"))
            ).otherwise(col("product_category_name"))
    }
)

# COMMAND ----------

publish_products.write.format("parquet") \
  .mode("overwrite") \
  .save("abfss://destination@storageadfkjupstart13.dfs.core.windows.net/publish/publish_products")