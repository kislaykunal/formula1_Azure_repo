# Databricks notebook source
dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC #### ingest pit_stops_file

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# COMMAND ----------

pit_stops_schema = StructType(fields=[
    StructField("raceId",IntegerType(),False),
    StructField("driverId",IntegerType(),True),
    StructField("stop",StringType(),True),
    StructField("lap",IntegerType(),True),
    StructField("time",StringType(),True),
    StructField("duration",StringType(),True),
    StructField("milliseconds",IntegerType(),True),
])

# COMMAND ----------

pit_stops_df = spark.read \
                    .schema(pit_stops_schema) \
                    .option("multiline",True) \
                    .json(f"{raw_folder_path}/{v_file_date}/pit_stops.json")

# COMMAND ----------

display(pit_stops_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### step - 2 Rename columns and add new column
# MAGIC 1. Rename driverId and raceId
# MAGIC 2. Add ingestion_date with current timestamp
# MAGIC

# COMMAND ----------

#from pyspark.sql.functions import current_timestamp

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

pit_stops_date_df = add_ingestion_date(pit_stops_df)

# COMMAND ----------

final_df=pit_stops_date_df.withColumnRenamed("driverId","driver_id",) \
    .withColumnRenamed("raceId","race_id") \
    .withColumn("data_source",lit(v_data_source))\
    .withColumn("file_date", lit(v_file_date))


    #.withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

display(final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step-3 Write the output to processed container in parquet format

# COMMAND ----------

# final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.pit_stops")

# COMMAND ----------

# display(spark.read.parquet(f"{processed_folder_path}/pit_stops"))

# COMMAND ----------

# overwrite_partition(final_df, 'f1_processed', 'pit_stops', 'race_id')

# COMMAND ----------

merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.stop = src.stop AND tgt.race_id = src.race_id"
merge_delta_data(final_df, 'f1_processed', 'pit_stops', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# %sql
# drop table f1_processed.pit_stops;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.pit_stops;

# COMMAND ----------

