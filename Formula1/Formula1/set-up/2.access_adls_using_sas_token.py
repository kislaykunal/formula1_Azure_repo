# Databricks notebook source
# MAGIC %md
# MAGIC ### Access Azure Data Lake using sas token
# MAGIC 1. Set the spark config for sas Token
# MAGIC 2. List files from demo container
# MAGIC 3. Read data from circuit.csv file

# COMMAND ----------

formual1dl_secrets_sas_token = dbutils.secrets.get(scope='formula1-scope',key='formula1dl-demo-sas-token')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1dl59.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.formula1dl59.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.formula1dl59.dfs.core.windows.net",formual1dl_secrets_sas_token)

# COMMAND ----------

dbutils.fs.ls("abfss://demo@formula1dl59.dfs.core.windows.net")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dl59.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dl59.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

