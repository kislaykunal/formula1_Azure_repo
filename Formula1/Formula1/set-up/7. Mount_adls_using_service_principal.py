# Databricks notebook source
# MAGIC %md
# MAGIC ### Mount Azure Data Lake using Service Principal
# MAGIC #### Steps to follow
# MAGIC 1. Get client_id, tenant_id and client_secret from key vault
# MAGIC 2. Set Spark Config with App/ Client Id, Directory/ Tenant Id & Secret 
# MAGIC 3. Call file system utilities related to mount  (list all mounts, unmounts)
# MAGIC

# COMMAND ----------

client_id = dbutils.secrets.get(scope='formula1-scope',key= 'formula1-app-client-id')
tenant_id = dbutils.secrets.get(scope='formula1-scope',key= 'formula1-app-tenant-id')
client_secret = dbutils.secrets.get(scope='formula1-scope',key= 'formula1-app-client-secret')

# COMMAND ----------

# can copy code snippet from here :-
# https://docs.databricks.com/dbfs/mounts.html 
# replace <application id to "client_id">
# and scope name to "client_secret"
configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://demo@formula1dl59.dfs.core.windows.net/",
  mount_point = "/mnt/formula1dl59/demo",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/formula1dl59/demo"))

# COMMAND ----------

display(spark.read.csv("/mnt/formula1dl59/demo/circuits.csv"))

# COMMAND ----------

#To know the path of the mount
display(dbutils.fs.mounts())
#first and last two are automatically created we can see them in the output

# COMMAND ----------

#To remove the mount or unmount them 
#dbutils.fs.unmount('mount name')
dbutils.fs.unmount('/mnt/formula1dl59/demo')

# COMMAND ----------

