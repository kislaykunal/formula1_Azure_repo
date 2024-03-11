# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest results.json file

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

v_file_date

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read the json file using the spark dataframe reader API
# MAGIC

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, StringType, FloatType, IntegerType

# COMMAND ----------

results_schema = StructType(fields=[
    StructField("resultId",IntegerType(),False),
    StructField("raceId",IntegerType(),True),
    StructField("driverId",IntegerType(),True),
    StructField("constructorId",IntegerType(),True),
    StructField("number",IntegerType(),True),
    StructField("grid",IntegerType(),True),
    StructField("position",IntegerType(),True),
    StructField("positionText",StringType(),True),
    StructField("positionOrder",IntegerType(),True),
    StructField("points",FloatType(),True),
    StructField("laps",IntegerType(),True),
    StructField("time",StringType(),True),
    StructField("milliseconds",IntegerType(),True),
    StructField("fastestLap",IntegerType(),True),
    StructField("rank",IntegerType(),True),
    StructField("fastestLapTime",StringType(),True),
    StructField("fastestLapSpeed",FloatType(),True),
    StructField("statusId",StringType(),True)
])

# COMMAND ----------

results_df= spark.read \
            .schema(results_schema) \
            .json(f"{raw_folder_path}/{v_file_date}/results.json")

# COMMAND ----------

# display(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step-2 Rename and add new columns

# COMMAND ----------

#from pyspark.sql.functions import current_timestamp

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

results_df = add_ingestion_date(results_df)

# COMMAND ----------

results_renamed_df = results_df.withColumnRenamed("resultId","result_id") \
                               .withColumnRenamed("raceId","race_id") \
                               .withColumnRenamed("driverId","driver_id") \
                               .withColumnRenamed("constructorId","constructor_id") \
                               .withColumnRenamed("positionText","position_text") \
                                .withColumnRenamed("fastestLap","fastest_lap") \
                                .withColumnRenamed("fastestLapTime","fastest_lap_time") \
                                .withColumnRenamed("fastestLapSpeed","Fastest_lap_speed") \
                                .withColumnRenamed("positionOrder","position_order")\
                                .withColumn("data_source",lit(v_data_source))\
                                .withColumn("file_date",lit(v_file_date))
                                #.withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

# display(results_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step-4 drop the unwanted column

# COMMAND ----------

results_final_df=results_renamed_df.drop("statusId")

# COMMAND ----------

# MAGIC %md
# MAGIC De-dupe the dataframe

# COMMAND ----------

#display(results_final_df)

# COMMAND ----------

results_deduped_df = results_final_df.dropDuplicates(['race_id', 'driver_id'])

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 4- write the data in parquet form

# COMMAND ----------

# MAGIC %md
# MAGIC ####Method 1

# COMMAND ----------

# for race_id_list in results_final_df.select("race_id").distinct().collect():
#     if (spark._jsparkSession.catalog().tableExists("f1_processed.results")):
#         spark.sql(f"ALTER TABLE f1_processed.results DROP IF EXISTS PARTITION (race_id = {race_id_list.race_id})")

# COMMAND ----------

# results_final_df.write.mode("append").partitionBy("race_id").format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Method 2

# COMMAND ----------

# overwrite_partition(results_final_df,'f1_processed','results','race_id')

# COMMAND ----------

merge_condition = "tgt.result_id = src.result_id AND tgt.race_id = src.race_id"
merge_delta_data(results_deduped_df, 'f1_processed', 'results', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# %sql
# select * from f1_processed.results;

# COMMAND ----------

# %sql
# drop table f1_processed.results;

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id,count(1)
# MAGIC from f1_processed.results
# MAGIC group by race_id 
# MAGIC order by race_id DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(1)
# MAGIC   FROM f1_processed.results;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, driver_id, COUNT(1) 
# MAGIC FROM f1_processed.results
# MAGIC GROUP BY race_id, driver_id
# MAGIC HAVING COUNT(1) > 1
# MAGIC ORDER BY race_id, driver_id DESC;

# COMMAND ----------

# MAGIC %sql SELECT * FROM f1_processed.results WHERE race_id = 540 AND driver_id = 229;

# COMMAND ----------

