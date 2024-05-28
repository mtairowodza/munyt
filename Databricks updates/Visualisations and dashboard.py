# Databricks notebook source
order_details = spark.read.parquet("dbfs:/FileStore/tables/gold/order_details")

# COMMAND ----------

monthly_sales = spark.read.parquet("dbfs:/FileStore/tables/gold/monthly_sales")

# COMMAND ----------

display(order_details)

# COMMAND ----------

display(monthly_sales)

# COMMAND ----------

# MAGIC %md
# MAGIC some text for my dashboard

# COMMAND ----------


