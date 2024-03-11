# Databricks notebook source
# MAGIC %md
# MAGIC #### Explore DBFS Root
# MAGIC 1. List all the folders in DBFS root
# MAGIC 2. Interact with DBFS File browser
# MAGIC 3. Upload file to DBFS Root
# MAGIC

# COMMAND ----------

# To see the default directory in DBFS Root
display(dbutils.fs.ls('/'))

# COMMAND ----------

# To look the data in the FileStore
display(dbutils.fs.ls('/FileStore'))

# COMMAND ----------

# To see the contents of the file
# We will use Data Frame Reader API ro read the data
display(spark.read.csv('/FileStore/circuits.csv'))

# COMMAND ----------

