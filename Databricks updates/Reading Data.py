# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Reading Data

# COMMAND ----------

spark.read.csv("/FileStore/countries.csv")

# COMMAND ----------

countries_df = spark.read.csv("/FileStore/countries.csv", header=True)

# COMMAND ----------

countries_df.show()

# COMMAND ----------

countries_df.display()

# COMMAND ----------

countries_df =spark.read.options(header=True).csv("/FileStore/countries.csv")

# COMMAND ----------

display(countries_df)

# COMMAND ----------

countries_df.dtypes

# COMMAND ----------

countries_df.schema

# COMMAND ----------

countries_df.describe()

# COMMAND ----------

countries_df.explain()

# COMMAND ----------

countries_df = spark.read.csv("/FileStore/countries.csv", header=True, inferSchema=True)

# COMMAND ----------

countries_df.display()

# COMMAND ----------

countries_df.schema

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

countries_schema =StructType([
    StructField('COUNTRY_ID', IntegerType(), False), 
    StructField('NAME', StringType(), False), 
    StructField('NATIONALITY', StringType(), False), 
    StructField('COUNTRY_CODE', StringType(), False), 
    StructField('ISO_ALPHA2', StringType(), False), 
    StructField('CAPITAL', StringType(), False), 
    StructField('POPULATION', IntegerType(), False), 
    StructField('AREA_KM2', DoubleType(), False), 
    StructField('REGION_ID', IntegerType(), True), 
    StructField('SUB_REGION_ID', IntegerType(), True), 
    StructField('INTERMEDIATE_REGION_ID', IntegerType(), True),
    StructField('ORGANIZATION_REGION_ID', IntegerType(), True)
    ])

# COMMAND ----------

countries_df = spark.read.csv("/FileStore/countries.csv", header=True, schema=countries_schema)

# COMMAND ----------

countries_df.display()

# COMMAND ----------

countries_df.schema

# COMMAND ----------

#reading in jason
countries_sl_jason = spark.read.json("dbfs:/FileStore/countries_single_line.json")

# COMMAND ----------

countries_sl_jason.display()

# COMMAND ----------

#reading multiline jason

countries_ml_jason = spark.read.options(multiline=True).json("dbfs:/FileStore/countries_multi_line.json")

# COMMAND ----------

countries_ml_jason.display()

# COMMAND ----------

#reading in testfile
countries_txt = spark.read.csv("dbfs:/FileStore/countries.txt", sep='\t', header=True)


# COMMAND ----------

countries_txt.display()

# COMMAND ----------

countries_txt.schema

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # writing Data

# COMMAND ----------

countries_schema =StructType([
    StructField('COUNTRY_ID', IntegerType(), False), 
    StructField('NAME', StringType(), False), 
    StructField('NATIONALITY', StringType(), False), 
    StructField('COUNTRY_CODE', StringType(), False), 
    StructField('ISO_ALPHA2', StringType(), False), 
    StructField('CAPITAL', StringType(), False), 
    StructField('POPULATION', IntegerType(), False), 
    StructField('AREA_KM2', DoubleType(), False), 
    StructField('REGION_ID', IntegerType(), True), 
    StructField('SUB_REGION_ID', IntegerType(), True), 
    StructField('INTERMEDIATE_REGION_ID', IntegerType(), True),
    StructField('ORGANIZATION_REGION_ID', IntegerType(), True)
    ])

# COMMAND ----------

countries_df = spark.read.csv("/FileStore/countries.csv", header=True, schema=countries_schema)

# COMMAND ----------

countries_df.write.csv('/FileStore/tables/countries_out', header=True, mode='overwrite')

# COMMAND ----------

df = spark.read.csv('dbfs:/FileStore/tables/countries_out', header=True)

# COMMAND ----------

df.show()

# COMMAND ----------

df.write.csv('/FileStore/tables/output/countries_out', mode='overwrite')

# COMMAND ----------

df.write.mode('overwrite').csv('/FileStore/tables/output/countries_out')

# COMMAND ----------

# MAGIC %md
# MAGIC ##partitioning

# COMMAND ----------

display(df)

# COMMAND ----------

df = spark.read.csv('dbfs:/FileStore/tables/countries_out', header=True)

# COMMAND ----------

df.write.mode('overwrite').partitionBy('REGION_ID').csv('/FileStore/tables/output/countries_out', header=True)

# COMMAND ----------

df2= spark.read.csv("dbfs:/FileStore/tables/countries_out/REGION_ID=10/part-00000-tid-1987045949540874250-35b215da-372e-4790-a20d-3591e49c3120-82-1.c000.csv", header=True)

# COMMAND ----------

df2.show()

# COMMAND ----------

df2.display()

# COMMAND ----------

#using parquet format
df = spark.read.csv('dbfs:/FileStore/tables/countries_out', header=True)


# COMMAND ----------

df.write.parquet('dbfs:/FileStore/tables/output/countries_parquet')

# COMMAND ----------

display(spark.read.parquet('dbfs:/FileStore/tables/output/countries_parquet'))

# COMMAND ----------

dbutils.fs.rm('dbfs:/FileStore/countries.txt')

# COMMAND ----------

dbutils.fs.rm('dbfs:/FileStore/countries.txt')

# COMMAND ----------

dbutils.fs.rm('dbfs:/FileStore/countries_single_line.json')

# COMMAND ----------

dbutils.fs.rm('dbfs:/FileStore/tables/output', recurse=True)

# COMMAND ----------

dbutils.fs.rm('dbfs:/FileStore/tables/countries_out', recurse=True)

# COMMAND ----------

dbutils.fs.rm('dbfs:/FileStore/.DS_Store')

# COMMAND ----------


