# Databricks notebook source
dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingest races.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step- 1 read the csv file using spark dataframe reader API

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1dl59/raw

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

# COMMAND ----------

# here we will create schema
#we have already read the csv file but we will create schema now and then insert in our csv file 
races_schema = StructType(fields=[StructField("raceId",IntegerType(),False),
                                  StructField("year",IntegerType(),True),
                                  StructField("round",IntegerType(),True),
                                  StructField("circuitId",IntegerType(),True),
                                  StructField("name",StringType(),True),
                                  StructField("date",DateType(),True),
                                  StructField("time",StringType(),True),
                                  StructField("url",StringType(),True)                                

])

# COMMAND ----------

races_df = spark.read\
.option("header",True)\
.schema(races_schema)\
.csv(f"{raw_folder_path}/{v_file_date}/races.csv")

# COMMAND ----------

display(races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step-2 Add ingestion date and race_timestamp to the dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,to_timestamp,concat,col,lit

# COMMAND ----------

races_with_ingestion_date_df = add_ingestion_date(races_df)

# COMMAND ----------

races_with_timestamp_df = races_with_ingestion_date_df.withColumn("race_timestamp",to_timestamp(concat(col("date"),lit(" "),col("time")),'yyyy-MM-dd HH:mm:ss'))\
.withColumn("data_source",lit(v_data_source))\
.withColumn("file_date",lit(v_file_date))

# COMMAND ----------

   display(races_with_timestamp_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step-2 Select only the columns required and rename as required

# COMMAND ----------

races_selected_df = races_with_timestamp_df.select(col("raceId").alias("race_id"),col("year").alias("race_year"),col("round"),col("circuitId").alias("circuit_id"),col("name"),col("ingestion_date"),col("race_timestamp"))

# COMMAND ----------

# races_selected_df = races_selected_df.withColumn("data_source",lit(v_data_source))

# COMMAND ----------

display(races_selected_df)

# COMMAND ----------

## WE have already renamed the columns using alias so col function is not required

# races_renamed_df = races_selected_df.withColumnRenamed("raceId","race_Id")\
# .withColumnRenamed("circuitId","circuit_Id")\
# .withColumnRenamed("year","race_year")

# COMMAND ----------

# MAGIC %md 
# MAGIC ###Write the output to processed container in parquet fromat

# COMMAND ----------

# races_selected_df.write.mode("overwrite").parquet("f{processed_folder_path/races")

# COMMAND ----------

# to check wether the file is created or not

# COMMAND ----------

# %fs
# ls /mnt/formula1dl59/processed/races

# COMMAND ----------

# display(spark.read.parquet("/mnt/formula1dl59/processed/races"))

# COMMAND ----------

# use of PartitionBy 
races_selected_df.write.mode("overwrite").partitionBy("race_year").format("delta").saveAsTable("f1_processed.races")

# COMMAND ----------

#  display(spark.read.parquet("/mnt/formula1dl59/processed/races"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.races;

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

