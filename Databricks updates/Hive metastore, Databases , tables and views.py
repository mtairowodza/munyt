# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC #reading data via sql

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.databrickspm.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.databrickspm.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.databrickspm.dfs.core.windows.net", "sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2024-10-12T11:45:10Z&st=2024-05-28T03:45:10Z&spr=https&sig=%2FhEJKCgsbjsLX6asS4rjE8rDkhLt%2BXN%2FW71Nxn3KyLE%3D")

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

countries = spark.read.csv("dbfs:/FileStore/countries.csv", header= True)

# COMMAND ----------

countries.createOrReplaceTempView("countries_tv")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from countries_tv

# COMMAND ----------

spark.sql("""
          select * 
          from countries_tv """).display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #Creatng databases in hive-metastore

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS countries

# COMMAND ----------

# MAGIC %sql
# MAGIC select current_database()

# COMMAND ----------

# MAGIC %sql
# MAGIC use default

# COMMAND ----------

countries = spark.read.csv("dbfs:/FileStore/countries.csv", header=True)

# COMMAND ----------

countries.write.saveAsTable('countries.countries_mt')

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended countries.countries_mt

# COMMAND ----------

# MAGIC %md
# MAGIC ###Creating an empty table in sql

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE countries.countries_mt_empty
# MAGIC (country_id int,
# MAGIC name string,
# MAGIC nationality string,
# MAGIC country_code string,
# MAGIC iso_alpha_2 string,
# MAGIC capital string,
# MAGIC population int,
# MAGIC area_km2 int,
# MAGIC region_id int,
# MAGIC sub_region_id int,
# MAGIC intermediate_region_id int,
# MAGIC organization_region_id int)
# MAGIC USING CSV

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create table countries.countries_copy as select * from countries.countries_mt

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from countries.countries_copy

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists countries.countries_mt;
# MAGIC drop table  if exists countries.countries_copy;
# MAGIC drop table if exists countries.countries_mt_empty;

# COMMAND ----------

# MAGIC %md
# MAGIC ##specifying location for managed table

# COMMAND ----------

countries = spark.read.csv("dbfs:/FileStore/countries.csv", header=True)

# COMMAND ----------

countries.write.saveAsTable('countries.countries_defaul_loc')

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table  if exists countries.countries_defaul_loc;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop database countries

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE countries
# MAGIC LOCATION 'dbfs:/FileStore/tables/managed_data'

# COMMAND ----------

# MAGIC %sql
# MAGIC Create database contriez
# MAGIC location"abfss://delta-lake-demo@databrickspm.dfs.core.windows.net/countries_modified.csv"
# MAGIC

# COMMAND ----------

countries.write.saveAsTable('countries.countries_specified_loc')

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table countries.countries_specified_loc

# COMMAND ----------

# MAGIC %sql
# MAGIC drop database countries

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS countries

# COMMAND ----------

# MAGIC %md
# MAGIC ##Unmanaged tables

# COMMAND ----------

countries = spark.read.csv("dbfs:/FileStore/countries.csv", header=True)

# COMMAND ----------

countries.write.option('path', "abfss://delta-lake-demo@databrickspm.dfs.core.windows.net/countries_modified.csv").saveAsTable('countries.countries_ext_python', mode='overwrite')

# COMMAND ----------

countries.write.option('path', 'fileStore/external/countries').saveAsTable('countries.countries_ext_python', mode='overwrite')

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table countries.countries_ext_python

# COMMAND ----------

# MAGIC %sql
# MAGIC  
# MAGIC CREATE TABLE countries.countries_ext_sql
# MAGIC (country_id int,
# MAGIC name string,
# MAGIC nationality string,
# MAGIC country_code string,
# MAGIC iso_alpha_2 string,
# MAGIC capital string,
# MAGIC population int,
# MAGIC area_km2 int,
# MAGIC region_id int,
# MAGIC sub_region_id int,
# MAGIC intermediate_region_id int,
# MAGIC organization_region_id int)
# MAGIC USING CSV
# MAGIC LOCATION '/FileStore/tables/countries.csv'

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table countries.countries_ext_python

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table countries.countries_ext_sql

# COMMAND ----------

dbutils.fs.rm('/user/hive/warehouse', recurse=True)

# COMMAND ----------

# MAGIC %sql
# MAGIC drop database countries
