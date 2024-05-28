# Databricks notebook source
# MAGIC %md
# MAGIC ## Deleting and updating records

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.databrickspm.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.databrickspm.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.databrickspm.dfs.core.windows.net", "sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2024-08-01T16:29:28Z&st=2024-05-27T08:29:28Z&spr=https&sig=DDnIRwwWf%2BQuKNfAzP%2BXQEtO15%2FR9JcB4iPpi5JlNU0%3D")

# COMMAND ----------

# MAGIC %sql
# MAGIC use delta_lake_db

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended delta_lake_db.countries_managed_delta

# COMMAND ----------

countries = spark.read.csv("abfss://bronze@databrickspm.dfs.core.windows.net/countries.csv", header=True, inferSchema=True)

# COMMAND ----------

countries.display()

# COMMAND ----------

countries.write.format("parquet").saveAsTable('delta_lake_db.countries_managed_pq')

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended delta_lake_db.countries_managed_pq

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from countries_managed_pq

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from countries_managed_delta

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from countries_ext_pq where region_id = 20

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from countries_ext_delta where region_id = 20

# COMMAND ----------

# MAGIC %md
# MAGIC ### python delete table

# COMMAND ----------

# Delete using Python code
# First create a deltaTable object and import libraries and functions
from delta.tables import *
from pyspark.sql.functions import *

deltaTable = DeltaTable.forPath(spark, "dbfs:/user/hive/warehouse/delta_lake_db.db/countries_managed_delta")

# COMMAND ----------

deltaTable.delete("region_id = 50 and population > 200000")

# COMMAND ----------

# MAGIC %md
# MAGIC ### python update table

# COMMAND ----------

deltaTable.update("region_id = '30'", { "country_code": "'XXX'" } )   # predicate using SQL formatted string

# COMMAND ----------


