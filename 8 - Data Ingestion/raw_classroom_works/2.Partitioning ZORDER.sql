-- Databricks notebook source
-- MAGIC %python
-- MAGIC spark.conf.set(
-- MAGIC     "fs.azure.account.key.qbexdatasets.dfs.core.windows.net",
-- MAGIC    "<AZURE STORAGE ACCOUNT ACCESS KEY>")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls("abfss://sampledatacatalogy@qbexdatasets.dfs.core.windows.net/")

-- COMMAND ----------

list "abfss://sampledatacatalogy@qbexdatasets.dfs.core.windows.net/sample_table_dir/"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC file_name = "25gb_salesfile.csv"
-- MAGIC sales_schema = """ order_id string,
-- MAGIC                    order_date date,
-- MAGIC                    customer_id string,
-- MAGIC                    qty int,
-- MAGIC                    price int,
-- MAGIC                    amount int,
-- MAGIC                    sales_region string,
-- MAGIC                    country string
-- MAGIC                 """
-- MAGIC df_sales = spark.read.format("csv").option("header",True).schema(sales_schema).load(f"abfss://sampledatacatalogy@qbexdatasets.dfs.core.windows.net/sample_table_dir/{file_name}")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_sales.schema

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_sales.limit(10).show()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_sales.rdd.getNumPartitions()

-- COMMAND ----------

-- drop TABLE cat_demo_001.sch_demo_001.tbl_demo_001_part_25gb;
CREATE or replace TABLE cat_demo_001.sch_demo_001.tbl_demo_001_part_25gb
(
  order_id string,
  order_date date,
  customer_id string,
  qty int,
  price INT,
  amount INT,
  sales_region string,
  country string
)
using DELTA
partitioned by (sales_region)
location "abfss://sampledatacatalogy@qbexdatasets.dfs.core.windows.net/external_tables/sales_001_part_25gb";

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_sales.write.mode("overwrite").saveAsTable("cat_demo_001.sch_demo_001.tbl_demo_001_part_25gb")

-- COMMAND ----------

describe extended  cat_demo_001.sch_demo_001.tbl_demo_001_part_25gb

-- COMMAND ----------

-- MAGIC %python
-- MAGIC max_file_size = spark.conf.get("spark.databricks.delta.optimize.maxFileSize")
-- MAGIC display(max_file_size)

-- COMMAND ----------

set spark.databricks.delta.optimize.maxFileSize = 134217728

-- COMMAND ----------

optimize cat_demo_001.sch_demo_001.tbl_demo_001_part_25gb

-- COMMAND ----------

DESCRIBE HISTORY cat_demo_001.sch_demo_001.tbl_demo_001_part_25gb

-- COMMAND ----------

SET spark.databricks.delta.retentionDurationCheck.enabled = false;

-- COMMAND ----------

vacuum cat_demo_001.sch_demo_001.tbl_demo_001_part_25gb retain 0 hours

-- COMMAND ----------

select * from cat_demo_001.sch_demo_001.tbl_demo_001_part_25gb
where sales_region = 'East'
and order_id = 'ORD00128'

-- COMMAND ----------

optimize cat_demo_001.sch_demo_001.tbl_demo_001_part_25gb zorder by order_id