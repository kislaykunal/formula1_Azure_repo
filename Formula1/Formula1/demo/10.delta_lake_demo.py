# Databricks notebook source
# MAGIC %md
# MAGIC ###### 1.Write data to delta lake(managed table)
# MAGIC ###### 2. Write data to delta lake(external table)
# MAGIC ###### 3. Read data from delta lake(Table)
# MAGIC ###### 4. Read data from delta lake (File)

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists f1_demo
# MAGIC location '/mnt/formula1dl59/demo'

# COMMAND ----------

results_df = spark.read\
    .option("inferSchema",True)\
    .json("/mnt/formula1dl59/raw/2021-03-28/results.json")

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").saveAsTable("f1_demo.results_managed")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_managed

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").save("/mnt/formula1dl59/demo/results_external")

# COMMAND ----------

# MAGIC %sql
# MAGIC create table f1_demo.results_external
# MAGIC using delta
# MAGIC location '/mnt/formula1dl59/demo/results_external'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_external

# COMMAND ----------

results_external_df = spark.read.format('delta').load("/mnt/formula1dl59/demo/results_external")

# COMMAND ----------

display(results_external_df)

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").partitionBy("constructorId").saveAsTable("f1_demo.results_partitioned")

# COMMAND ----------

# MAGIC %sql
# MAGIC show partitions f1_demo.results_partitioned

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1. Update Delta Table
# MAGIC ##### 2. Delete from Delta Table

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_managed;

# COMMAND ----------

# MAGIC %sql
# MAGIC --#updating data using sql 
# MAGIC update f1_demo.results_managed
# MAGIC  set points = 11-position
# MAGIC where position <=10

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_managed;

# COMMAND ----------

#updating data using python
from delta.tables import DeltaTable


deltaTable = DeltaTable.forPath(spark,"/mnt/formula1dl59/demo/results_managed")

deltaTable.update("position <=10 ",{"points": "21-position"})

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_managed;

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from f1_demo.results_managed
# MAGIC where position >10;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_managed;

# COMMAND ----------

#deleting data using python
from delta.tables import DeltaTable


deltaTable = DeltaTable.forPath(spark,"/mnt/formula1dl59/demo/results_managed")

deltaTable.delete("points =0")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_managed;

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Upsert using merge
# MAGIC ###### used to update exisiting records for which new data have been recieved
# MAGIC ###### or apply delete as well

# COMMAND ----------

drivers_day1_df = spark.read\
.option("inferSchema",True)\
.json("/mnt/formula1dl59/raw/2021-03-28/drivers.json")\
.filter("driverId <=10")\
.select("driverId","dob","name.forename","name.surname")


# COMMAND ----------

display(drivers_day1_df)

# COMMAND ----------

drivers_day1_df.createOrReplaceTempView("drivers_day1")

# COMMAND ----------

from pyspark.sql.functions import upper

drivers_day2_df = spark.read\
.option("inferSchema",True)\
.json("/mnt/formula1dl59/raw/2021-03-28/drivers.json")\
.filter("driverId BETWEEN 6 and 15")\
.select("driverId","dob",upper("name.forename").alias("forename"),upper("name.surname").alias("surname"))


# COMMAND ----------

drivers_day2_df.createOrReplaceTempView("drivers_day2")

# COMMAND ----------

display(drivers_day2_df)

# COMMAND ----------

from pyspark.sql.functions import upper

drivers_day3_df = spark.read\
.option("inferSchema",True)\
.json("/mnt/formula1dl59/raw/2021-03-28/drivers.json")\
.filter("driverId BETWEEN 1 and 5 OR driverId BETWEEN 16 and 20 ")\
.select("driverId","dob",upper("name.forename").alias("forename"),upper("name.surname").alias("surname"))


# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists f1_demo.drivers_merge(
# MAGIC driverId INT,
# MAGIC dob DATE,
# MAGIC forename string,
# MAGIC surname string,
# MAGIC createdDate date,
# MAGIC updatedDate date
# MAGIC )
# MAGIC using DELTA
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC DAY 1

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge tgt
# MAGIC USING drivers_day1 upd
# MAGIC ON tgt.driverId = upd.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET tgt.dob = upd.dob,
# MAGIC              tgt.forename = upd.forename,
# MAGIC              tgt.surname = upd.surname,
# MAGIC              tgt.updatedDate = current_timestamp
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (driverId, dob, forename,surname,createdDate ) VALUES (driverId, dob, forename,surname, current_timestamp)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %md
# MAGIC Day 2

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge tgt
# MAGIC USING drivers_day2 upd
# MAGIC ON tgt.driverId = upd.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET tgt.dob = upd.dob,
# MAGIC              tgt.forename = upd.forename,
# MAGIC              tgt.surname = upd.surname,
# MAGIC              tgt.updatedDate = current_timestamp
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (driverId, dob, forename,surname,createdDate ) VALUES (driverId, dob, forename,surname, current_timestamp)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %md
# MAGIC Day 3

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, "/mnt/formula1dl59/demo/drivers_merge")

deltaTable.alias("tgt").merge(
    drivers_day3_df.alias("upd"),
    "tgt.driverId = upd.driverId") \
  .whenMatchedUpdate(set = { "dob" : "upd.dob", "forename" : "upd.forename", "surname" : "upd.surname", "updatedDate": "current_timestamp()" } ) \
  .whenNotMatchedInsert(values =
    {
      "driverId": "upd.driverId",
      "dob": "upd.dob",
      "forename" : "upd.forename", 
      "surname" : "upd.surname", 
      "createdDate": "current_timestamp()"
    }
  ) \
  .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %md
# MAGIC 1. History & Versioning
# MAGIC 2. Time Travel
# MAGIC 3. Vaccum

# COMMAND ----------

# MAGIC %sql
# MAGIC desc history f1_demo.drivers_merge

# COMMAND ----------

`%sql
select * from f1_demo.drivers_merge version as of 2`

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge timestamp as of '2023-09-08T18:40:09.000+0000'

# COMMAND ----------

#python method
df = spark.read.format("delta").option("timestampAsOf", '2023-09-08T18:48:11.000+0000').load("/mnt/formula1dl59/demo/drivers_merge")

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC vacuum f1_demo.drivers_merge;
# MAGIC --required 7 days to delete

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge version as of 2

# COMMAND ----------

# MAGIC %sql 
# MAGIC set spark.databricks.delta.retentionDurationCheck.enabled = false;
# MAGIC vacuum f1_demo.drivers_merge retain 0 hours

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge timestamp as of '2023-09-08T18:40:09.000+0000'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC desc history f1_demo.drivers_merge;

# COMMAND ----------

-- retrieve if accidently deleted the data
%sql
delete from f1_demo.drivers_merge where driverId =1;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge version as of 3;

# COMMAND ----------

# MAGIC %sql
# MAGIC merge into f1_demo.drivers_merge tgt
# MAGIC using f1_demo.drivers_merge version as of 3 src
# MAGIC  on (tgt.driverId = src.driverId)
# MAGIC when not matched then
# MAGIC  insert *

# COMMAND ----------

# MAGIC %sql desc history f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %md
# MAGIC #### Transaction Logs

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists f1_demo.drivers_txn(
# MAGIC driverId INT,
# MAGIC dob DATE,
# MAGIC forename string,
# MAGIC surname string,
# MAGIC createdDate date,
# MAGIC updatedDate date
# MAGIC )
# MAGIC using DELTA
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC desc history f1_demo.drivers_txn
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into f1_demo.drivers_txn
# MAGIC select * from f1_demo.drivers_merge
# MAGIC where driverId =1;

# COMMAND ----------

# MAGIC %sql
# MAGIC desc history f1_demo.drivers_txn
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into f1_demo.drivers_txn
# MAGIC select * from f1_demo.drivers_merge
# MAGIC where driverId =2;

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from f1_demo.drivers_txn
# MAGIC where driverId =1;

# COMMAND ----------

for driver_id in range (3,20):
    spark.sql(f"""INSERT INTO f1_demo.drivers_txn
              SELECT * FROM f1_demo.drivers_merge
              WHERE driverId = {driver_id}""")

# COMMAND ----------

# %sql
# insert into f1_demo.drivers_txn
# select * from f1_demo.drivers_merge
# where driverId =2;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Convert Parquet to Delta

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists f1_demo.drivers_convert_to_delta(
# MAGIC driverId INT,
# MAGIC dob DATE,
# MAGIC forename string,
# MAGIC surname string,
# MAGIC createdDate date,
# MAGIC updatedDate date
# MAGIC )
# MAGIC using DELTA
# MAGIC

# COMMAND ----------

# MAGIC %sql 
# MAGIC insert into  f1_demo.drivers_convert_to_delta
# MAGIC select * from f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC CONVERT TO DELTA  f1_demo.drivers_convert_to_delta
# MAGIC

# COMMAND ----------

#if there is no table
df = spark.table("f1_demo.drivers_convert_to_delta")

# COMMAND ----------

df.write.format("parquet").save("/mnt/formula1dl59/demo/drivers_convert_to_delta_new")

# COMMAND ----------

# MAGIC %sql
# MAGIC CONVERT TO DELTA parquet.`/mnt/formula1dl/demo/drivers_convert_to_delta_new`

# COMMAND ----------

