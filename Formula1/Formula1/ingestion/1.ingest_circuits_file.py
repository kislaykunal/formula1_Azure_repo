# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest circuits.csv file

# COMMAND ----------

dbutils.widgets.help()

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

v_data_source

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

raw_folder_path

# COMMAND ----------

# MAGIC %md
# MAGIC #### step 1 - Read the CSV file using the spark dataframe reader

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, StringType, DoubleType

# COMMAND ----------

circuits_Schema=StructType(fields=[StructField("circuitId", IntegerType(),False),
                                   StructField("circuitRef", StringType(),True),
                                   StructField("name", StringType(),True),
                                   StructField("location", StringType(),True),
                                   StructField("country", StringType(),True),
                                   StructField("lat", DoubleType(),True),
                                   StructField("long", DoubleType(),True),
                                   StructField("alt", IntegerType(),True),
                                   StructField("url", StringType(),True),

                                   
])

# COMMAND ----------

circuits_df=spark.read\
.option("header",True)\
.schema(circuits_Schema)\
.csv(f"{raw_folder_path}/{v_file_date}/circuits.csv")

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 Select only the required columns

# COMMAND ----------

#Method 4 
# By using a function col so we will import it to use it
from pyspark.sql.functions import col


# COMMAND ----------

circuits_selected_df = circuits_df.select(col('circuitId'),col('circuitRef'),col('name'),col('location'),col('country'),col('lat'),col('long'),col('alt'))

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Step-3 Rename the columns as required

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId","circuit_id")\
.withColumnRenamed("circuitRef","circuit_ref")\
.withColumnRenamed("lat","latitude")\
.withColumnRenamed("long","longitude")\
.withColumnRenamed("alt","altitude")\
.withColumn("data_source",lit(v_data_source))\
.withColumn("file_date",lit(v_file_date))
                 

# COMMAND ----------

# MAGIC %md
# MAGIC #### step-4 ingestion the date to the dataframe

# COMMAND ----------

circuits_final_df = add_ingestion_date(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5 Write data to datalake as Delta(before it was - parquet)

# COMMAND ----------

circuits_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.circuits")

# COMMAND ----------

# display(spark.read.parquet("/mnt/formula1dl59/processed/circuits"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.circuits;

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

