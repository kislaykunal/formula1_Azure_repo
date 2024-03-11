# Databricks notebook source
# MAGIC %md
# MAGIC ### Access Azure Data Lake using access keys
# MAGIC 1. Set the spark config fs.azure.account.key
# MAGIC 2. List files from demo container
# MAGIC 3. Read data from circuit.csv file

# COMMAND ----------

#Now we are using the secret instead of direct key and value
formula1dl_account_key = dbutils.secrets.get(scope='formula1-scope',key='formula1dl-account-key')

# COMMAND ----------

#here we have removed the actual key which is mentioned below
#"CTwYtBNbvk5oWJemiIKe0wjMCqkwzmvoDnzSd1dmdd1An5POQEUyFVa0NcO7CUrNyMQiurX3uPk3+AStjkobWg=="
spark.conf.set("fs.azure.account.key.formula1dl59.dfs.core.windows.net",formula1dl_account_key)

# COMMAND ----------

dbutils.fs.ls("abfss://demo@formula1dl59.dfs.core.windows.net")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dl59.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dl59.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

