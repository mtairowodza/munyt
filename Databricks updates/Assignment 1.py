# Databricks notebook source
# MAGIC %md
# MAGIC #Bronze Layer

# COMMAND ----------

# MAGIC %md
# MAGIC ##Read Tables

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

customers_df = spark.read.csv("dbfs:/FileStore/tables/bronze/customers.csv", header=True)

# COMMAND ----------

customers_df.schema

# COMMAND ----------

customers_scema = StructType([
    StructField('CUSTOMER_ID', IntegerType(), False), 
    StructField('FULL_NAME', StringType(), False), 
    StructField('EMAIL_ADDRESS', StringType(), False)
    ])

# COMMAND ----------

customers = spark.read.csv("dbfs:/FileStore/tables/bronze/customers.csv", schema=customers_scema,  header=True)

# COMMAND ----------

customers.display()

# COMMAND ----------

orders_df = spark.read.csv("dbfs:/FileStore/tables/bronze/orders.csv", header=True)

# COMMAND ----------

orders_df.schema

# COMMAND ----------

orders_schema = StructType([
    StructField('ORDER_ID', IntegerType(), False), 
    StructField('ORDER_DATETIME', StringType(), False), 
    StructField('CUSTOMER_ID', IntegerType(), False), 
    StructField('ORDER_STATUS', StringType(), False), 
    StructField('STORE_ID', StringType(), False)
    ])

# COMMAND ----------

orders = spark.read.csv("dbfs:/FileStore/tables/bronze/orders.csv", schema=orders_schema, header=True)

# COMMAND ----------

from pyspark.sql.functions import to_timestamp

orders= orders.select('ORDER_ID', \
              to_timestamp(orders['order_datetime'], "dd-MMM-yy kk.mm.ss.SS").alias('ORDER_TIMESTAMP'), \
              'CUSTOMER_ID', \
              'ORDER_STATUS', \
              'STORE_ID'
             )

# COMMAND ----------

orders = orders.filter(orders['order_status']=="COMPLETE")

# COMMAND ----------

oi_df = spark.read.csv("dbfs:/FileStore/tables/bronze/order_items.csv", header=True)

# COMMAND ----------

oi_df.schema

# COMMAND ----------

oi_schema = StructType([
    StructField('ORDER_ID', IntegerType(), False), 
    StructField('LINE_ITEM_ID', StringType(), False), 
    StructField('PRODUCT_ID', IntegerType(), False), 
    StructField('UNIT_PRICE', DoubleType(), False), 
    StructField('QUANTITY', IntegerType(), False)
    ])

# COMMAND ----------

order_items = spark.read.csv("dbfs:/FileStore/tables/bronze/order_items.csv", schema=oi_schema, header=True)

# COMMAND ----------

stores_df = spark.read.csv("dbfs:/FileStore/tables/bronze/stores.csv", header = True)

# COMMAND ----------

stores_df.schema

# COMMAND ----------

stores_schema = StructType([
    StructField('STORE_ID', StringType(), False), 
    StructField('STORE_NAME', StringType(), False), 
    StructField('WEB_ADDRESS', StringType(), False), 
    StructField('LATITUDE', StringType(), False), 
    StructField('LONGITUDE', StringType(), False)
    ])

# COMMAND ----------

stores = spark.read.csv("dbfs:/FileStore/tables/bronze/stores.csv",schema=stores_schema, header = True)

# COMMAND ----------

products_df = spark.read.csv("dbfs:/FileStore/tables/bronze/products.csv", header=True)

# COMMAND ----------

products_df.schema

# COMMAND ----------

products_schema = StructType([
    StructField('PRODUCT_ID', IntegerType(), False), 
    StructField('PRODUCT_NAME', StringType(), False), 
    StructField('UNIT_PRICE', DoubleType(), False)
    ])

# COMMAND ----------

products = spark.read.csv("dbfs:/FileStore/tables/bronze/products.csv", schema=products_schema, header=True)

# COMMAND ----------

# MAGIC %md
# MAGIC #Silver Layer

# COMMAND ----------

# MAGIC %md
# MAGIC #silver Transformation

# COMMAND ----------

products.show()

# COMMAND ----------

customers.show()

# COMMAND ----------

order_items = order_items.drop('LINE_ITEM_ID')

# COMMAND ----------

order_items.show()

# COMMAND ----------

orders = orders.join(stores, orders['STORE_ID'] == stores['STORE_ID'], 'left').select('ORDER_ID', 'ORDER_TIMESTAMP', 'CUSTOMER_ID', 'STORE_NAME')

# COMMAND ----------

orders.columns

# COMMAND ----------

orders.write.parquet("dbfs:/FileStore/tables/silver/orders", mode ="overwrite")

# COMMAND ----------

order_items.write.parquet("dbfs:/FileStore/tables/silver/order_items", mode ="overwrite")

# COMMAND ----------

customers.write.parquet("dbfs:/FileStore/tables/silver/customers", mode ="overwrite")

# COMMAND ----------

products.write.parquet("dbfs:/FileStore/tables/silver/products", mode ="overwrite")

# COMMAND ----------

# MAGIC %md
# MAGIC #GOLD LAYER

# COMMAND ----------

orders = spark.read.parquet("dbfs:/FileStore/tables/silver/orders")

# COMMAND ----------

order_items = spark.read.parquet("dbfs:/FileStore/tables/silver/order_items")

# COMMAND ----------

products = spark.read.parquet("dbfs:/FileStore/tables/silver/products")

# COMMAND ----------

customers = spark.read.parquet("dbfs:/FileStore/tables/silver/customers")

# COMMAND ----------

orders.columns

# COMMAND ----------

orders =  orders.select('ORDER_ID',\
    to_date('order_timestamp').alias('DATE'),\
         'CUSTOMER_ID',\
              'STORE_NAME')

# COMMAND ----------

order_items = order_items.withColumn('Total_order_amount', order_items['unit_price'] * order_items['quantity'])

# COMMAND ----------

order_details = orders.join(order_items, orders["order_id"] == order_items["order_id"], 'left').select(orders["order_id"], "DATE", "CUSTOMER_ID", "STORE_NAME", "Total_order_amount" ).groupBy("order_id", "date", "customer_id", "store_name").agg(\
    sum('total_order_amount').alias('total_order_amount'))

# COMMAND ----------

order_details= order_details.withColumn('total_order_amount', round('total_order_amount',2))

# COMMAND ----------

order_details.write.parquet("dbfs:/FileStore/tables/gold/order_details", mode ="overwrite")

# COMMAND ----------

sales_with_month = order_details.withColumn('MONTH_YEAR', date_format('DATE','yyyy-MM'))

# COMMAND ----------

monthly_sales = sales_with_month.groupBy('MONTH_YEAR').agg(\
    sum("Total_order_amount").alias("month_sales")).withColumn('month_sales', round('month_sales',2)).orderBy("month_sales", ascending=False)

# COMMAND ----------

monthly_sales.write.parquet("dbfs:/FileStore/tables/gold/monthly_sales", mode ="overwrite")

# COMMAND ----------

store_monthly_sales = sales_with_month.groupBy('MONTH_YEAR', "STORE_NAME").agg(\
    sum("Total_order_amount").alias("monthly-sale-by-store")).withColumn("monthly-sale-by-store", round("monthly-sale-by-store",2)).orderBy("monthly-sale-by-store", ascending=False)

# COMMAND ----------

store_monthly_sales.show()

# COMMAND ----------

store_monthly_sales.write.parquet("dbfs:/FileStore/tables/gold/store_monthly_sales", mode ="overwrite")
