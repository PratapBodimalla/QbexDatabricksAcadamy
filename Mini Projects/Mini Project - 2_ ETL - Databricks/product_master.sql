-- Databricks notebook source
-- Read data from PostgreSQL into a temporary view
CREATE OR REPLACE TEMPORARY VIEW temp_product_master_view
USING org.apache.spark.sql.jdbc
OPTIONS (
  url 'jdbc:postgresql://<HOST IP ADDRESS>:5432/<DATABASE NAME>',
  dbtable '<TABLE NAME>',
  user '<USER NAME>',
  password '<PASSWORD>',
  driver 'org.postgresql.Driver'
);

-- COMMAND ----------

-- Display Temporary View.
SELECT * FROM temp_product_master_view;

-- COMMAND ----------

-- Create a permanent view in the desired catalog and schema
CREATE OR REPLACE TABLE sample_catalog_v1.sample_schema_v1.product_master_table AS
SELECT * FROM temp_product_master_view;


-- COMMAND ----------

-- Display Data.
SELECT * FROM sample_catalog_v1.sample_schema_v1.product_master_table;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.notebook.exit("Success")