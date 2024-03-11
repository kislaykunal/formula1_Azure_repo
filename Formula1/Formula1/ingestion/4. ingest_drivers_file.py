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
# MAGIC ### Read the json file using the spark dataframe reader API
# MAGIC

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

name_schema = StructType(fields=[StructField("forename", StringType(),True) ,
                                 StructField("surname", StringType(),True)])

# COMMAND ----------

drivers_schema = StructType(fields=[StructField("driverId", IntegerType(),False),
                                    StructField("driverRef", StringType(),True),
                                    StructField("number",IntegerType(),True),
                                    StructField("code", StringType(),True),
                                    StructField("name",name_schema,True),
                                    StructField("dob",DateType(),True),
                                    StructField("nationality",StringType(),True),
                                    StructField("url",StringType(),True)
                                    ])

# COMMAND ----------

drivers_df = spark.read \
    .schema(drivers_schema) \
    .json(f"{raw_folder_path}/{v_file_date}/drivers.json")

# COMMAND ----------

drivers_df.printSchema()

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step-2 Rename columns and ad new columns
# MAGIC 1. driverId renamed to driver_id
# MAGIC 2. driverRed renamed to driver_ref
# MAGIC 3. ingestion date added
# MAGIC 4. name added with concation of forename and surname
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col,concat,current_timestamp, lit

# COMMAND ----------

drivers_date_df = add_ingestion_date(drivers_df)

# COMMAND ----------

drivers_renamed_df = drivers_date_df.withColumnRenamed("driverId","driver_id") \
    .withColumn("name",concat(col("name.forename"),lit(" "),col("name.surname"))) \
    .withColumnRenamed("driverRef","driver_ref")\
    .withColumn("data_source",lit(v_data_source))\
    .withColumn("file_date",lit(v_file_date))

#.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

display(drivers_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step-3 Drop the unwanted columns
# MAGIC 1. name.forename
# MAGIC 2. name.surname (we have concat these two so no need to drop)
# MAGIC 3. url

# COMMAND ----------

drivers_final_df = drivers_renamed_df.drop("url")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write the output to processed container in  parquet format

# COMMAND ----------

drivers_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.drivers")

# COMMAND ----------

# display(spark.read.parquet("/mnt/formula1dl59/processed/drivers"))

# COMMAND ----------

# %fs
# ls /mnt/formula1dl59/processed/drivers

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.drivers;

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

