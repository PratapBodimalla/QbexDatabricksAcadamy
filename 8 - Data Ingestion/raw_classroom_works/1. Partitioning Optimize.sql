-- Databricks notebook source
-- MAGIC %python
-- MAGIC spark.conf.set(
-- MAGIC     "fs.azure.account.key.qbexdatasets.dfs.core.windows.net",
-- MAGIC    "<AZURE STORAGE ACCOUNT ACCESS KEY>")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls("abfss://sampledatacatalogy@qbexdatasets.dfs.core.windows.net/sample_table_dir/"))

-- COMMAND ----------

list "abfss://sampledatacatalogy@qbexdatasets.dfs.core.windows.net/sample_table_dir/"

-- COMMAND ----------

-- drop TABLE cat_demo_001.sch_demo_001.tbl_demo_001;
CREATE or replace TABLE cat_demo_001.sch_demo_001.tbl_demo_001
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
location "abfss://sampledatacatalogy@qbexdatasets.dfs.core.windows.net/external_tables/sales_001";

-- COMMAND ----------

-- MAGIC %python
-- MAGIC file_name = "50mb_salesfile.csv"
-- MAGIC sales_schema = """ order_id string,
-- MAGIC                    order_date date,
-- MAGIC                    customer_id string,
-- MAGIC                    qty int,
-- MAGIC                    price int,
-- MAGIC                    amount int,
-- MAGIC                    sales_region string,
-- MAGIC                    country string
-- MAGIC                 """
-- MAGIC df_sales = spark.read.format("csv") \
-- MAGIC                         .option("header",True) \
-- MAGIC                         .schema(sales_schema) \
-- MAGIC                         .load(f"abfss://sampledatacatalogy@qbexdatasets.dfs.core.windows.net/sample_table_dir/{file_name}")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_sales.schema

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_sales.count()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_sales.rdd.getNumPartitions()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_sales.write.mode("overwrite").saveAsTable("cat_demo_001.sch_demo_001.tbl_demo_001")

-- COMMAND ----------

describe extended cat_demo_001.sch_demo_001.tbl_demo_001

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls("abfss://sampledatacatalogy@qbexdatasets.dfs.core.windows.net/external_tables/sales_001"))

-- COMMAND ----------

select customer_id, sum(amount)
from cat_demo_001.sch_demo_001.tbl_demo_001
where sales_region = 'North'
group by customer_id


-- COMMAND ----------

-- drop TABLE cat_demo_001.sch_demo_001.tbl_demo_001_part;
CREATE or replace TABLE cat_demo_001.sch_demo_001.tbl_demo_001_part
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
location "abfss://sampledatacatalogy@qbexdatasets.dfs.core.windows.net/external_tables/sales_001_part";

-- COMMAND ----------

4 * 4

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_sales.write.mode("overwrite").saveAsTable("cat_demo_001.sch_demo_001.tbl_demo_001_part")

-- COMMAND ----------

describe extended  cat_demo_001.sch_demo_001.tbl_demo_001_part

-- COMMAND ----------

optimize cat_demo_001.sch_demo_001.tbl_demo_001_part

-- COMMAND ----------

DESCRIBE HISTORY cat_demo_001.sch_demo_001.tbl_demo_001_part

-- COMMAND ----------

SET spark.databricks.delta.retentionDurationCheck.enabled = false;

-- COMMAND ----------

vacuum cat_demo_001.sch_demo_001.tbl_demo_001_part retain 0 hours

-- COMMAND ----------

select * from cat_demo_001.sch_demo_001.tbl_demo_001
where order_id = 'ORD00006'

-- COMMAND ----------

select min(order_id), max(order_id) , _metadata.file_name
from cat_demo_001.sch_demo_001.tbl_demo_001
group by _metadata.file_name

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.conf.set("spark.databricks.delta.optimize.maxFileSize", 160 * 1024 * 8)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC print(160 * 1024 * 8)

-- COMMAND ----------

optimize cat_demo_001.sch_demo_001.tbl_demo_001 zorder by order_id

-- COMMAND ----------

vacuum cat_demo_001.sch_demo_001.tbl_demo_001 retain 0 hours