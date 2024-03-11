# Databricks notebook source
# MAGIC %md
# MAGIC ####Access dataframes using SQL
# MAGIC ##### Objectives
# MAGIC 1.Create temporary views on dataframes\
# MAGIC 2.Access the view from SQL cell \
# MAGIC 3.Access the view from pyhton cell 

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

race_results_df.createOrReplaceTempView("v_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(1)
# MAGIC from v_race_results
# MAGIC where race_year =2020

# COMMAND ----------

race_results_2019_df=spark.sql("select * from v_race_results where race_year = 2019")

# COMMAND ----------

display(race_results_2019_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Global Temporary Views
# MAGIC 1. Create golbal temporary views on dataframes
# MAGIC 2. Access the view from SQL cell
# MAGIC 3. Access the view from python cell
# MAGIC 4. Access the view from another notebook
# MAGIC

# COMMAND ----------

race_results_df.createOrReplaceGlobalTempView("gv_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in global_temp;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * 
# MAGIC from global_temp.gv_race_results;

# COMMAND ----------

spark.sql("select *\
    from global_temp.gv_race_results").show()

# COMMAND ----------

