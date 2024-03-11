-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##### Lesson Objectives
-- MAGIC 1. Spark SQL documentation
-- MAGIC 2. Create Database demo
-- MAGIC 3. Data tab in the UI
-- MAGIC 4. SHOW command
-- MAGIC 5. DESCRIBE command
-- MAGIC 6. Find the current database

-- COMMAND ----------

create database demo;

-- COMMAND ----------

create database if not exists demo;

-- COMMAND ----------

show databases;

-- COMMAND ----------

DESCRIBE DATABASE demo;

-- COMMAND ----------

describe database extended demo;

-- COMMAND ----------

select current_database()

-- COMMAND ----------

show tables;

-- COMMAND ----------

show tables in demo

-- COMMAND ----------

use demo

-- COMMAND ----------

select current_database()

-- COMMAND ----------

show tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Learning Objectives
-- MAGIC 1. Create managed table using Pyhton
-- MAGIC 2. Create managed table using sql
-- MAGIC 3. Effect of dropping a managed table
-- MAGIC 4. Describe table

-- COMMAND ----------

-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").saveAsTable("demo.race_results_python")

-- COMMAND ----------

use demo;
show tables;

-- COMMAND ----------

DESCRIBE EXTENDED race_results_python;

-- COMMAND ----------

SELECT *
  FROM demo.race_results_python
 where race_year = 2020;

-- COMMAND ----------

CREATE table demo.race_results_sql
as
SELECT *
  FROM demo.race_results_python
 where race_year = 2020;

-- COMMAND ----------

select current_database()

-- COMMAND ----------

describe extended demo.race_results_sql;

-- COMMAND ----------

drop table demo.race_results_sql;

-- COMMAND ----------

show tables in demo;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Learning Objectives
-- MAGIC 1. Create external table using Python
-- MAGIC 2. Create external table using SQL
-- MAGIC 3. Effect of dropping an external table

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").option("path",f"{presentation_folder_path}/race_results_ext_py").saveAsTable("demo.race_results_ext_py")

-- COMMAND ----------

DESC extended demo.race_results_ext_py

-- COMMAND ----------

create table demo.race_results_ext_sql
(
race_year INT,
race_name STRING,
race_date TIMESTAMP,
circuit_location  STRING,
driver_name STRING,
driver_number INT,
driver_nationality STRING,
team STRING,
grid INT,
fastest_lap INT,
race_time STRING,
points FLOAT,
position INT,
created_date TIMESTAMP
)
USING parquet
location "/mnt/formula1dl59/presentation/race_results_ext_sql"

-- COMMAND ----------

show tables in demo;

-- COMMAND ----------

insert into demo.race_results_ext_sql
select * from demo.race_results_ext_py where race_year =2020;

-- COMMAND ----------

select count(1) from demo.race_results_ext_sql;

-- COMMAND ----------

show tables in demo;

-- COMMAND ----------

-- MAGIC  %md
-- MAGIC  #### View on tables
-- MAGIC  1. Create TEmp view
-- MAGIC  2. Create Global Temp view
-- MAGIC  3. Create permanent View

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_race_results
AS
SELECT * 
 FROM demo.race_results_python
 WHERE race_year =2018;

-- COMMAND ----------

select * from v_race_results;

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMP VIEW gv_race_results
AS
SELECT * 
 FROM demo.race_results_python
 WHERE race_year =2018;

-- COMMAND ----------

select * from global_temp.v_race_results;

-- COMMAND ----------

show tables in global_temp;

-- COMMAND ----------

CREATE OR REPLACE VIEW demo.pv_race_results
AS
SELECT * 
 FROM demo.race_results_python
 WHERE race_year =2000;

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

select * from demo.pv_race_results;

-- COMMAND ----------

