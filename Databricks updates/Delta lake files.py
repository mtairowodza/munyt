# Databricks notebook source
spark.conf.set("fs.azure.account.auth.type.databrickspm.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.databrickspm.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.databrickspm.dfs.core.windows.net", "sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2024-08-01T16:29:28Z&st=2024-05-27T08:29:28Z&spr=https&sig=DDnIRwwWf%2BQuKNfAzP%2BXQEtO15%2FR9JcB4iPpi5JlNU0%3D")

# COMMAND ----------

countries = spark.read.csv("abfss://bronze@databrickspm.dfs.core.windows.net/countries.csv", header=True)

# COMMAND ----------

countries.display()

# COMMAND ----------

output_path = "abfss://delta-lake-demo@databrickspm.dfs.core.windows.net/countries_delta.csv"
countries.write.format('delta').save(output_path)

# COMMAND ----------

output_path = "abfss://delta-lake-demo@databrickspm.dfs.core.windows.net/countries_parquet"
countries.write.format('parquet').mode("overwrite").save(output_path)

# COMMAND ----------

spark.read.format('delta').load("abfss://delta-lake-demo@databrickspm.dfs.core.windows.net/countries_delta.csv")

# COMMAND ----------

output_path = "abfss://delta-lake-demo@databrickspm.dfs.core.windows.net/countries_delta_paritioned.csv"
countries.write.format('delta').mode("overwrite").partitionBy("region_id").save(output_path)

# COMMAND ----------

# MAGIC %sql
# MAGIC create database delta_lake_db

# COMMAND ----------

countries = spark.read.format('delta').load("abfss://delta-lake-demo@databrickspm.dfs.core.windows.net/countries_delta.csv")

# COMMAND ----------

countries.write.saveAsTable("delta_lake_db.countries_managed_delta")

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended delta_lake_db.countries_managed_delta

# COMMAND ----------

countries.write.option("path", "abfss://delta-lake-demo@databrickspm.dfs.core.windows.net/countries_delta.csv").mode("overwrite").saveAsTable("delta_lake_db.countries_ext_delta")

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended  delta_lake_db.countries_ext_delta
