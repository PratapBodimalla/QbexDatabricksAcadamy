-- Databricks notebook source
-- MAGIC %python
-- MAGIC spark

-- COMMAND ----------

show databases
-- create database qbex;

-- COMMAND ----------

CREATE TABLE qbex.emp
(empno string,
empname string,
address string,
salary integer)
using delta

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls("/FileStore/mydata/"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC emp_schema = "empno string, empname string, address string, salary integer"
-- MAGIC emp_df = spark.read.format("csv").schema(emp_schema).load("/FileStore/mydata/emp_99.csv")
-- MAGIC display(emp_df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # display(emp_df)
-- MAGIC emp_df.write.format("delta").mode("append").saveAsTable("qbex.emp")

-- COMMAND ----------

-- insert into qbex.emp
select empno, empname, address, salary
from read_files("/FileStore/mydata/emp_77.csv",
header => True,
format => "csv")

-- COMMAND ----------

MERGE INTO qbex.emp target USING (select empno, empname, address, salary
from read_files("/FileStore/mydata/emp_77.csv",
header => True,
format => "csv")) source
  ON source.empno = target.empno
  WHEN MATCHED THEN UPDATE SET *
  WHEN NOT MATCHED THEN INSERT *

-- COMMAND ----------

-- insert into qbex.emp values ('E3','ABC3','HYD',50000)
SELECT * FROM qbex.emp
-- update qbex.emp set address = 'ATP' where empno = 'E1'
-- delete from qbex.emp where empno = 'E1'

-- COMMAND ----------

describe HISTORY qbex.emp

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls('dbfs:/user/hive/warehouse/qbex.db/emp/'))

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC head dbfs:/user/hive/warehouse/qbex.db/emp/_delta_log/00000000000000000005.json
