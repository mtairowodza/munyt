# Databricks notebook source
# MAGIC %md
# MAGIC #Using Access Keys

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.databrickspm.dfs.core.windows.net",
    "muQ66J9aelJHOhWmRatds8F0PadXa3zOELyt3uQR+/nmfm1QyQbjtajkqM6NnC4UgVolkZviVm8L+AStTgTYOQ==")

# COMMAND ----------

countries = spark.read.csv("abfss://bronze@databrickspm.dfs.core.windows.net/countries.csv", header=True)

# COMMAND ----------

regions = spark.read.csv("abfss://bronze@databrickspm.dfs.core.windows.net/country_regions.csv", header=True)

# COMMAND ----------

# MAGIC %md
# MAGIC #Using Shared Access Token

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.databrickspm.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.databrickspm.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.databrickspm.dfs.core.windows.net", "sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2024-08-01T16:29:28Z&st=2024-05-27T08:29:28Z&spr=https&sig=DDnIRwwWf%2BQuKNfAzP%2BXQEtO15%2FR9JcB4iPpi5JlNU0%3D")

# COMMAND ----------

countries = spark.read.csv("abfss://bronze@databrickspm.dfs.core.windows.net/countries.csv", header=True)

# COMMAND ----------

# MAGIC %md
# MAGIC #Mounting ADLS to ADFS

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": "<application-id>",
          "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="<scope-name>",key="<service-credential-key-name>"),
          "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/<directory-id>/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://<container-name>@<storage-account-name>.dfs.core.windows.net/",
  mount_point = "/mnt/<mount-name>",
  extra_configs = configs)

# COMMAND ----------

#check my current mounts

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs
# MAGIC ls

# COMMAND ----------

application_id = "ef8617c7-5c6f-4f46-be6f-2f9ccf7e05f5"
tenant_id = "1f03fbf5-9feb-425a-97b8-2faf8595223f"
secret = "039b1634-b6ab-494e-8312-0c8e5e530ba0"


# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": "ef8617c7-5c6f-4f46-be6f-2f9ccf7e05f5",
          "fs.azure.account.oauth2.client.secret": "039b1634-b6ab-494e-8312-0c8e5e530ba0",
          "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/1f03fbf5-9feb-425a-97b8-2faf8595223f/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://bronze@databrickspm.dfs.core.windows.net/",
  mount_point = "/mnt/bronze",
  extra_configs = configs)

# COMMAND ----------


