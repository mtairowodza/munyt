# Databricks notebook source
countries =  spark.read.csv('dbfs:/FileStore/countries.csv', header=True)

# COMMAND ----------

# MAGIC %sql
# MAGIC create database countries

# COMMAND ----------

# MAGIC %sql
# MAGIC use countries

# COMMAND ----------

countries.write.saveAsTable('countries_mt')

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace view countries.view_region_10
# MAGIC as select * from countries.countries_mt
# MAGIC where region_id = 10

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from countries.view_region_10

# COMMAND ----------

# MAGIC %sql
# MAGIC drop database countries cascade
