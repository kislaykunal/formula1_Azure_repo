# Databricks notebook source
dbutils.widgets.text("p_file_date","2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %run ../includes/configuration

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Find race year for which data is to be processed

# COMMAND ----------

race_results_df = spark.read.format("delta").load(f"{presentation_folder_path}/race_results")\
.filter(f"file_date = '{v_file_date}'")

# COMMAND ----------

race_year_list = df_column_to_list(race_results_df,'race_year')

# COMMAND ----------

from pyspark.sql.functions import col

races_df = spark.read.format("delta").load(f"{presentation_folder_path}/race_results")\
.filter(col("race_year").isin(race_year_list))

# COMMAND ----------

# display(races_df)

# COMMAND ----------

 from pyspark.sql.functions import sum, count, when, col

 constructor_standings_df = races_df\
     .groupBy("Team","race_year")\
     .agg(sum("points").alias("points"),\
        count(when(col("position") == 1,True)).alias("Wins"))

# COMMAND ----------

# display(constructor_standings_df.filter("race_year == 2020"))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import rank,desc

constructor_window_spec = Window.partitionBy("race_year").orderBy(desc("points"),desc("wins"))

final_df = constructor_standings_df.withColumn("rank",rank().over(constructor_window_spec))

# COMMAND ----------

# display(final_df.filter("race_year == 2020"))

# COMMAND ----------

# final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.constructor_standings")

# COMMAND ----------

# overwrite_partition(final_df, 'f1_presentation', 'constructor_standings', 'race_year')

# COMMAND ----------

merge_condition = "tgt.team = src.team AND tgt.race_year = src.race_year"
merge_delta_data(final_df, 'f1_presentation', 'constructor_standings', presentation_folder_path, merge_condition, 'race_year')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_presentation.constructor_standings
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_year,count(1)
# MAGIC from f1_presentation.driver_standings
# MAGIC group by race_year
# MAGIC order by race_year desc

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_presentation.constructor_standings WHERE race_year = 2021;

# COMMAND ----------

