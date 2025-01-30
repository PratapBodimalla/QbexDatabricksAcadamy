-- Databricks notebook source
-- MAGIC %python
-- MAGIC spark.conf.set(
-- MAGIC     "fs.azure.account.key.qbexdatasets.dfs.core.windows.net",
-- MAGIC    "<AZURE STORAGE ACCOUNT ACCESS KEY>")

-- COMMAND ----------


CREATE TABLE cat_demo_001.sch_demo_001.student (id INT, name STRING, age INT) 
using DELTA
TBLPROPERTIES (delta.enableChangeDataFeed = true)
location "abfss://sampledatacatalogy@qbexdatasets.dfs.core.windows.net/external_tables/student"


-- COMMAND ----------

insert into cat_demo_001.sch_demo_001.student values (1,'pratap',16)

-- COMMAND ----------

select * from cat_demo_001.sch_demo_001.student

-- COMMAND ----------

select * from table_changes('cat_demo_001.sch_demo_001.student', 2,5)

-- COMMAND ----------

update cat_demo_001.sch_demo_001.student 
set age = 46
where id = 1