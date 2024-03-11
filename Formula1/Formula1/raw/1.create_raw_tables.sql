-- Databricks notebook source
create database if not exists f1_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create circuits table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.circuits;
CREATE TABLE IF NOT EXISTS f1_raw.circuits(circuitId INT,
circuitRef STRING,
name STRING,
location STRING,
country STRING,
lat DOUBLE,
lng DOUBLE,
alt INT,
url STRING
)
using csv
options (path "/mnt/formula1dl59/raw/circuits.csv",header true)


-- COMMAND ----------

select * from f1_raw.circuits;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create races Table

-- COMMAND ----------

drop table if exists f1_raw.races;
create table if not exists f1_raw.races(
  raceId int,
  year int,
  round int,
  circuitId int,
  name string,
  date date,
  time string,
  url string
)
using csv
options(path "/mnt/formula1dl59/raw/races.csv",header true)

-- COMMAND ----------

select * from f1_raw.races;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create tables for JSON files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### Create constructors table
-- MAGIC 1. Single Line JSON
-- MAGIC 2. Simple structure 

-- COMMAND ----------

drop table if exists f1_raw.constructors;
create table if not exists f1_raw.constructors
(
  constructorId INT, 
  constructorRef STRING,
   name STRING,
    nationality STRING,
    url STRING
)
using json
options(path "/mnt/formula1dl59/raw/constructors.json")

-- COMMAND ----------

select * from f1_raw.constructors

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create drivers table
-- MAGIC 1. Single Table
-- MAGIC 2. Complex structure

-- COMMAND ----------

drop table if exists f1_raw.drivers;
create table if not exists f1_raw.drivers(
  driverId int,
  driverRef string,
  number INT,
  code string,
  name STRUCT<forename: STRING, surname: STRING>,
  dob DATE,
  nationality STRING,
  url STRING)
  using json
  options (path "/mnt/formula1dl59/raw/drivers.json")


-- COMMAND ----------

select * from f1_raw.drivers

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC #### Create results table
-- MAGIC #####. Single Line JSON 
-- MAGIC #####. Simple structure

-- COMMAND ----------

drop table if exists f1_raw.results;
create table if not exists f1_raw.results(
resultId int,
raceId int,
driverId int,
constructorId int,
number int,
grid int,
position int,
positionText string,
positionOrder int,
points float,
laps int,
time string,
milliseconds int,
fastestLap int,
rank int,
fastestLapTime string,
fastestLapSpeed float,
statusId string
)
using json
options (path "/mnt/formula1dl59/raw/results.json")

-- COMMAND ----------

select * from f1_raw.results

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC #### Create pit stops table
-- MAGIC #####. Multi Line JSON 
-- MAGIC #####. Simple structure

-- COMMAND ----------

drop table if exists f1_raw.stops;
create table if not exists f1_raw.stops(
  driverId int,
  duration string,
  lap int,
  milliseconds int,
  raceId int,
  stop int,
  time string)
  using json
  options (path  "/mnt/formula1dl59/raw/pit_stops.json", multiLine true)



-- COMMAND ----------

select * from f1_raw.stops;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create Lap Times Table
-- MAGIC ###### .CSV file
-- MAGIC ###### .Multiple files
-- MAGIC

-- COMMAND ----------

drop table if exists f1_raw.lap_times;
create table if not exists f1_raw.lap_times(
  raceId int,
  driverId int,
  lap int,
  position int,
  time string,
  millisecond int)
  using csv
  options (path  "/mnt/formula1dl59/raw/lap_times")



-- COMMAND ----------

select * from f1_raw.lap_times

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create Qualifying Table
-- MAGIC ###### .JSON file
-- MAGIC ###### . MultiLine JSON
-- MAGIC ###### .Multiple files
-- MAGIC

-- COMMAND ----------

drop table if exists f1_raw.qualifying;
create table if not exists f1_raw.qualifying(
  qualifyId int,
  raceId int,
  driverId int,
  number int,
  position int,
  constructorId int,
  q1 string,
  q2 string,
  q3 string)
  using json
  options (path  "/mnt/formula1dl59/raw/qualifying", multiLine true)

-- COMMAND ----------

select * from f1_raw.qualifying

-- COMMAND ----------

desc extended f1_raw.qualifying

-- COMMAND ----------

