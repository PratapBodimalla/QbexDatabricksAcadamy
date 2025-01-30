-- Databricks notebook source
-- MAGIC %python
-- MAGIC spark.conf.set(
-- MAGIC     "fs.azure.account.key.qbexdatasets.dfs.core.windows.net",
-- MAGIC    "<AZURE STORAGE ACCOUNT ACCESS KEY>")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls("abfss://sampledatacatalogy@qbexdatasets.dfs.core.windows.net/sample_table_dir/")

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

-- drop TABLE cat_demo_001.sch_demo_001.tbl_demo_001_part_25gb_cluster;
CREATE or replace TABLE cat_demo_001.sch_demo_001.tbl_demo_001_part_25gb_cluster
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
CLUSTER BY (order_id)
location "abfss://sampledatacatalogy@qbexdatasets.dfs.core.windows.net/external_tables/sales_001_part_25gb_cluster";

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_sales.write.mode("overwrite").saveAsTable("cat_demo_001.sch_demo_001.tbl_demo_001_part_25gb_cluster")

-- COMMAND ----------

select min(order_id), max(order_id) , _metadata.file_name
from cat_demo_001.sch_demo_001.tbl_demo_001_part_25gb_cluster
group by _metadata.file_name

-- COMMAND ----------

describe detail cat_demo_001.sch_demo_001.tbl_demo_001_part_25gb_cluster

-- COMMAND ----------

optimize cat_demo_001.sch_demo_001.tbl_demo_001_part_25gb_cluster

-- COMMAND ----------

describe detail cat_demo_001.sch_demo_001.tbl_demo_001_part_25gb_cluster

-- COMMAND ----------

insert into cat_demo_001.sch_demo_001.tbl_demo_001_part_25gb_cluster
values ("ORD01",'2024-01-01','CUS001',12,10,120,'East','India')
 

-- COMMAND ----------

optimize cat_demo_001.sch_demo_001.tbl_demo_001_part_25gb_cluster

-- COMMAND ----------

describe history  cat_demo_001.sch_demo_001.tbl_demo_001_part_25gb_cluster

-- COMMAND ----------

-- MAGIC %python
-- MAGIC max_file_size = spark.conf.get("spark.databricks.delta.optimize.maxFileSize")
-- MAGIC display(max_file_size)

-- COMMAND ----------

DESCRIBE DETAIL cat_demo_001.sch_demo_001.tbl_demo_001_part_25gb_cluster;

-- COMMAND ----------

DESCRIBE HISTORY cat_demo_001.sch_demo_001.tbl_demo_001_part_25gb_cluster

-- COMMAND ----------

DESCRIBE HISTORY cat_demo_001.sch_demo_001.tbl_demo_001_part_25gb_cluster

-- COMMAND ----------

select * from cat_demo_001.sch_demo_001.tbl_demo_001_part_25gb_cluster
where sales_region = 'East'
and order_id = 'ORD00128'

-- COMMAND ----------

optimize cat_demo_001.sch_demo_001.tbl_demo_001_part_25gb zorder by order_id