-- Databricks notebook source
-- MAGIC %md
-- MAGIC The notebook demonstrates the implementation of Delta Live Tables (DLT) in Databricks, focusing on creating and refreshing materialized views, streaming tables, and handling change data capture (CDC) for Slowly Changing Dimensions (SCD). Through a series of steps, you will see how to create and manage DLT pipelines that process data from different sources, apply transformations, and update data incrementally using CDC methods. Key concepts covered include the creation of materialized views, managing streaming tables, and applying changes to data while maintaining historical records using SCD Type 1 and Type 2.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC # Delta Live Tables
-- MAGIC
-- MAGIC Delta Live Tables (DLT) is a powerful ETL (Extract, Transform, Load) framework provided by Databricks that simplifies data pipeline development, reduces complexity, and ensures reliable data processing. Below, I will explain its features and important concepts like Materialized Views, Streaming Tables, Views, and DLT Pipelines in detail.
-- MAGIC
-- MAGIC Delta Live Tables provides:
-- MAGIC - **Declarative Pipelines**: Automatically handles data dependencies and transformations.
-- MAGIC - **Auto-Optimization**: Automatically manages partitioning, file formats, and other optimizations.
-- MAGIC - **Data Quality Rules**: Allows users to define data quality expectations.
-- MAGIC - **Schema Evolution**: Automatically adjusts to changes in the schema without manual intervention.
-- MAGIC - **Real-Time Monitoring**: Provides real-time monitoring and auditing of pipelines.
-- MAGIC
-- MAGIC
-- MAGIC ## Materialized Views
-- MAGIC
-- MAGIC A **Materialized View** is a precomputed query result stored in the table. It is refreshed periodically or on-demand to provide up-to-date data. This reduces computation time for complex queries.
-- MAGIC
-- MAGIC ### Key Features:
-- MAGIC - **Performance Optimization**: Saves the query results, avoiding recomputation.
-- MAGIC - **Automatic or On-demand Refresh**: Keeps data fresh with periodic updates.
-- MAGIC - **Data Consistency**: Ensures data reflects the current state after refresh.
-- MAGIC
-- MAGIC ### Example of Creating a Materialized View:
-- MAGIC ```sql
-- MAGIC -- Create or Refresh a Materialized View for Customer Data
-- MAGIC CREATE OR REFRESH MATERIALIZED VIEW customer_master_dlt_view AS
-- MAGIC SELECT
-- MAGIC   customer_id,
-- MAGIC   customer_name,
-- MAGIC   email
-- MAGIC FROM
-- MAGIC   customer_data
-- MAGIC WHERE
-- MAGIC   active = TRUE;
-- MAGIC ```
-- MAGIC
-- MAGIC ## Streaming Tables
-- MAGIC
-- MAGIC A **Streaming Tables** allow data to be ingested in real-time and stored as Delta tables. DLT supports continuous data ingestion from sources like Kafka, ADLS, and more.
-- MAGIC
-- MAGIC ### Key Features:
-- MAGIC - **Real-Time Ingestion**: Allows for processing real-time streaming data.
-- MAGIC - **Auto-Schema Inference**: Automatically handles schema changes in streaming data.
-- MAGIC - **Stateful and Stateless Processing**: Supports both states for aggregation and stateless data transformation.
-- MAGIC - **Checkpointing**: Ensures data is processed without duplication.
-- MAGIC
-- MAGIC ```sql
-- MAGIC -- Create or Refresh Streaming Table from a Data Lake
-- MAGIC CREATE OR REFRESH STREAMING TABLE sales_orders_streaming_table AS
-- MAGIC SELECT *
-- MAGIC FROM cloud_files(
-- MAGIC   '/path/to/streaming_data', 
-- MAGIC   'parquet'
-- MAGIC );
-- MAGIC ```
-- MAGIC ##  Views
-- MAGIC **Views** in DLT are virtual tables that present data in a structured format, applying filters or transformations without storing the data. Views are great for simplifying complex queries and presenting data for real-time analysis.
-- MAGIC
-- MAGIC ## Key Features:
-- MAGIC - **Virtual Tables**: Do not store data but represent transformed data from underlying tables.
-- MAGIC - **Dynamic Data** Representation: Can be used to join, filter, or aggregate data for flexible querying.
-- MAGIC
-- MAGIC Example of Creating a View:
-- MAGIC ```sql
-- MAGIC -- Create a view to join customer and product data
-- MAGIC CREATE OR REFRESH VIEW customer_product_view AS
-- MAGIC SELECT
-- MAGIC   c.customer_id,
-- MAGIC   c.customer_name,
-- MAGIC   p.product_name,
-- MAGIC   p.price
-- MAGIC FROM
-- MAGIC   customers c
-- MAGIC JOIN
-- MAGIC   products p ON c.customer_id = p.customer_id;
-- MAGIC ```
-- MAGIC
-- MAGIC ## DLT Pipelines
-- MAGIC **DLT Pipelines** allow users to create and schedule ETL pipelines with ease. These pipelines handle the ingestion, transformation, and storage of data, ensuring seamless operations.
-- MAGIC
-- MAGIC ## Key Features:
-- MAGIC - **Declarative Syntax**: Pipelines are defined using simple SQL or Python code.
-- MAGIC - **Automatic Execution**: DLT automatically manages the execution of pipelines, including dependency management.
-- MAGIC - **Monitoring**: Provides real-time health checks and performance monitoring of the pipeline.
-- MAGIC - **Streaming and Batch Processing**: Supports both types of workloads seamlessly.
-- MAGIC
-- MAGIC Example of Creating a Simple DLT Pipeline:
-- MAGIC ```python
-- MAGIC # Python example of a DLT Pipeline for transforming sales data
-- MAGIC @dlt.table
-- MAGIC def sales_summary():
-- MAGIC     return (
-- MAGIC         spark.read.table("sales_data")
-- MAGIC         .groupBy("product_id")
-- MAGIC         .agg({"total_sales": "sum"})
-- MAGIC     )
-- MAGIC ```

-- COMMAND ----------

-- MAGIC %md DLT - Materialized Views

-- COMMAND ----------

-- CREATE OR REFRESH MATERIALIZED VIEW view_name [CLUSTER BY (col_name1, col_name2, ... )]
--   [(
--     [
--     col_name1 col_type1 [ GENERATED ALWAYS AS generation_expression1 ] [ COMMENT col_comment1 ] [ column_constraint ] [ MASK func_name [ USING COLUMNS ( other_column_name | constant_literal [, ...] ) ] ],
--     col_name2 col_type2 [ GENERATED ALWAYS AS generation_expression2 ] [ COMMENT col_comment2 ] [ column_constraint ] [ MASK func_name [ USING COLUMNS ( other_column_name | constant_literal [, ...] ) ] ],
--     ...
--     ]
--     [
--     CONSTRAINT expectation_name_1 EXPECT (expectation_expr1) [ON VIOLATION { FAIL UPDATE | DROP ROW }],
--     CONSTRAINT expectation_name_2 EXPECT (expectation_expr2) [ON VIOLATION { FAIL UPDATE | DROP ROW }],
--     ...
--     ]
--     [ table_constraint ] [, ...]
--   )]
--   [USING DELTA]
--   [PARTITIONED BY (col_name1, col_name2, ... )]
--   [LOCATION path]
--   [COMMENT table_comment]
--   [TBLPROPERTIES (key1 [ = ] val1, key2 [ = ] val2, ... )]
--   [ WITH { ROW FILTER func_name ON ( [ column_name | constant_literal [, ...] ] ) [...] } ]
--   AS select_statement

-- COMMAND ----------

-- Creating a Materialized View for Customer Master Data
CREATE OR REFRESH MATERIALIZED VIEW customer_master_dlt_view(
  customer_id STRING,  -- Customer's unique ID
  customer_name STRING,  -- Customer's name
  email STRING,  -- Customer's email
  phone STRING,  -- Customer's phone number
  address STRING,  -- Customer's address
  updated_date DATE  -- Date of the last update
)
COMMENT 'Materialized view for customer data with enforced constraints'
TBLPROPERTIES (
  'data_source' = 'qbexdataset Container, "sampledatacatalogy / dlt_volume_dir" path'  -- Metadata for the source of data
) AS
SELECT
  customer_id,  -- Select customer_id from the source
  customer_name,  -- Select customer_name from the source
  email,  -- Select email from the source
  phone,  -- Select phone from the source
  address,  -- Select address from the source
  CURRENT_DATE AS updated_date  -- Set the current date as updated date
FROM
  read_files("/Volumes/sample_catalog_v1/sample_schema_v1/sample_volumn_v1/dlt_dir/customer_master.parquet", format => "parquet", header => "true", inferSchema => "true");  -- Read data from a parquet file

-- COMMAND ----------

-- Creating a Materialized View for Product Master Data
CREATE OR REFRESH MATERIALIZED VIEW product_master_dlt_view(
  product_id STRING,  -- Product's unique ID
  product_name STRING,  -- Product's name
  price DOUBLE,  -- Product's price
  start_date DATE,  -- Product's start date
  end_date DATE  -- Product's end date
)
COMMENT 'Materialized view for product data with enforced constraints'
TBLPROPERTIES (
  'data_source' = 'qbexdataset Container, "sampledatacatalogy / dlt_volume_dir" path'  -- Metadata for the source of data
) AS
SELECT
  product_id,  -- Select product_id from the source
  product_name,  -- Select product_name from the source
  price,  -- Select price from the source
  CAST(start_date AS DATE) AS start_date,  -- Ensure start_date is in DATE format
  CAST(end_date AS DATE) AS end_date  -- Ensure end_date is in DATE format
FROM
  read_files("/Volumes/sample_catalog_v1/sample_schema_v1/sample_volumn_v1/dlt_dir/product_master.parquet", format => "parquet", header => "true", inferSchema => "true");  -- Read data from a parquet file

-- COMMAND ----------

-- Syntax to refresh materialized views.
-- REFRESH MATERIALIZED VIEW customer_master_dlt_view;
-- REFRESH MATERIALIZED VIEW product_master_dlt_view;

-- COMMAND ----------

-- MAGIC %md DLT - Streaming Tables

-- COMMAND ----------

-- CREATE OR REFRESH [TEMPORARY] STREAMING TABLE table_name [CLUSTER BY (col_name1, col_name2, ... )]
--   [(
--     [
--     col_name1 col_type1 [ GENERATED ALWAYS AS generation_expression1 ] [ COMMENT col_comment1 ] [ column_constraint ] [ MASK func_name [ USING COLUMNS ( other_column_name | constant_literal [, ...] ) ] ],
--     col_name2 col_type2 [ GENERATED ALWAYS AS generation_expression2 ] [ COMMENT col_comment2 ] [ column_constraint ] [ MASK func_name [ USING COLUMNS ( other_column_name | constant_literal [, ...] ) ] ],
--     ...
--     ]
--     [
--     CONSTRAINT expectation_name_1 EXPECT (expectation_expr1) [ON VIOLATION { FAIL UPDATE | DROP ROW }],
--     CONSTRAINT expectation_name_2 EXPECT (expectation_expr2) [ON VIOLATION { FAIL UPDATE | DROP ROW }],
--     ...
--     ]
--     [ table_constraint ] [, ...]
--   )]
--   [USING DELTA]
--   [PARTITIONED BY (col_name1, col_name2, ... )]
--   [LOCATION path]
--   [COMMENT table_comment]
--   [TBLPROPERTIES (key1 [ = ] val1, key2 [ = ] val2, ... )]
--   [ WITH { ROW FILTER func_name ON ( [ column_name | constant_literal [, ...] ] ) [...] } ]
--   AS select_statement

-- COMMAND ----------

-- -- Define a STREAMING TABLE to read data from PostgreSQL
-- CREATE OR REFRESH STREAMING TABLE sales_orders_jdbc_dlt_streaming_table 
-- CLUSTER BY (customer_id)
-- PARTITIONED BY (order_date)
-- COMMENT "Streaming Delta table for sales_orders"
-- TBLPROPERTIES (
--   'data_source' = 'PostgreSQL "SALES_ORDERS" TABLE'
-- )
-- AS SELECT * 
-- FROM READ_STREAM(
--   FORMAT => 'jdbc',
--   OPTIONS => (
--     'url' 'jdbc:postgresql://<HOST IP ADDRESS>:5432/<DATABASE NAME>',
--     'dbtable' '<TABLE NAME>',
--     'user' '<USER NAME>',
--     'password' '<PASSWORD>',
--     'driver' 'org.postgresql.Driver'
--   )
-- );

-- COMMAND ----------

-- Define a streaming table for sales orders
CREATE OR REFRESH STREAMING TABLE sales_orders_adls_dlt_streaming_table
(
  order_id STRING,  -- Order's unique ID
  customer_id STRING,  -- Customer's ID
  product_id STRING,  -- Product's ID
  quantity BIGINT,  -- Quantity of the product ordered
  order_date DATE,  -- Date of the order
  total_price DOUBLE  -- Total price for the order
)
CLUSTER BY (order_date)  -- Cluster by the order date for efficient queries
COMMENT "Streaming Delta table for sales orders"
TBLPROPERTIES (
  'data_source' = 'ADLS Gen2 qbexmetastore container "abfss://samplevolumes@qbexmetastore.dfs.core.windows.net/sampledata/dlt_dir/sales_orders_files/"'  -- Data source for the streaming table
) AS SELECT 
  order_id,  -- Select order_id from the source
  customer_id,  -- Select customer_id from the source
  product_id,  -- Select product_id from the source
  quantity,  -- Select quantity from the source
  CAST(order_date AS DATE) AS order_date,  -- Ensure order_date is in DATE format
  total_price  -- Select total_price from the source
FROM cloud_files(  -- Using cloud_files to load data from external storage
  'abfss://samplevolumes@qbexmetastore.dfs.core.windows.net/sampledata/dlt_dir/sales_orders_files/',
  'parquet',  -- File format is parquet
  map(
    'cloudFiles.inferColumnTypes', 'true'  -- Automatically infer column types
  )
);

-- COMMAND ----------

-- MAGIC %md DLT - View

-- COMMAND ----------

-- CREATE TEMPORARY [STREAMING] LIVE VIEW view_name
--   [(
--     [
--     col_name1 [ COMMENT col_comment1 ],
--     col_name2 [ COMMENT col_comment2 ],
--     ...
--     ]
--     [
--     CONSTRAINT expectation_name_1 EXPECT (expectation_expr1) [ON VIOLATION { FAIL UPDATE | DROP ROW }],
--     CONSTRAINT expectation_name_2 EXPECT (expectation_expr2) [ON VIOLATION { FAIL UPDATE | DROP ROW }],
--     ...
--     ]
--   )]
--   [COMMENT view_comment]
--   AS select_statement

-- COMMAND ----------

-- MAGIC %md Data Transformations

-- COMMAND ----------

-- Create a materialized view to merge customer, product, and sales data
CREATE OR REFRESH MATERIALIZED VIEW full_sales_orders_dlt_view_1 (
  order_id STRING, 
  customer_id STRING, 
  customer_name STRING, 
  email STRING, 
  phone STRING, 
  address STRING, 
  product_id STRING, 
  product_name STRING, 
  price DOUBLE, 
  quantity BIGINT, 
  order_date DATE, 
  total_price DOUBLE, 
  start_date DATE, 
  end_date DATE, 
  updated_date DATE
)
COMMENT 'Materialized view for merged customer, product, and sales orders data'
AS
SELECT
  so.order_id,  -- Select order_id from sales_orders
  so.customer_id,  -- Select customer_id from sales_orders
  cm.customer_name,  -- Join with customer_master to get customer_name
  cm.email,  -- Get email from customer_master
  cm.phone,  -- Get phone from customer_master
  cm.address,  -- Get address from customer_master
  so.product_id,  -- Select product_id from sales_orders
  pm.product_name,  -- Join with product_master to get product_name
  pm.price,  -- Get price from product_master
  so.quantity,  -- Get quantity from sales_orders
  so.order_date,  -- Get order_date from sales_orders
  so.total_price,  -- Get total_price from sales_orders
  pm.start_date,  -- Get product start_date
  pm.end_date,  -- Get product end_date
  CURRENT_DATE AS updated_date  -- Set the current date as the update date
FROM
  LIVE.sales_orders_adls_dlt_streaming_table AS so
JOIN
  LIVE.customer_master_dlt_view AS cm
  ON so.customer_id = cm.customer_id
JOIN
  LIVE.product_master_dlt_view AS pm
  ON so.product_id = pm.product_id;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # custom_order_year = spark.conf.get("custom.order_year")

-- COMMAND ----------

-- Analysis - How much sales each customer contributed on each product in 2025.

-- Create a materialized view.
CREATE OR REFRESH MATERIALIZED VIEW sales_orders_analysis_dlt_view
AS
SELECT
  customer_id,
  customer_name,
  product_id,
  product_name,
  sum(total_price) as total_sales
FROM
  LIVE.full_sales_orders_dlt_view_1
WHERE 
  YEAR(order_date) = CAST('2025' AS INT)
GROUP BY
  customer_id,
  customer_name,
  product_id,
  product_name

-- COMMAND ----------

-- MAGIC %md DLT - CDC/SCD -  APPLY CHANGES INTO

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE customer_master_dlt_cdc_streaming_table
COMMENT "Streaming source table for customer master data"
TBLPROPERTIES (
  'pipelines.autoOptimize.zOrderCols' = 'customer_id'
)
AS
SELECT
  customer_id,
  customer_name,
  email,
  phone,
  address,
  'ACTIVE' AS CUSTOMER_STATUS,
  CURRENT_TIMESTAMP AS UPDATED_DATE
FROM cloud_files(
  '/Volumes/sample_catalog_v1/sample_schema_v1/sample_volumn_v1/dlt_dir/',
  'parquet',
  map(
    'cloudFiles.format', 'parquet',
    'pathGlobFilter', '*customer_master.parquet',
    'cloudFiles.inferColumnTypes', 'true',
    'cloudFiles.schemaLocation', '/Volumes/sample_catalog_v1/sample_schema_v1/sample_volumn_v1/dlt_dir/checkpoints/customer_master'
  )
)
-- CONSTRAINT valid_customer_status EXPECT (CUSTOMER_STATUS IN ("ACTIVE", "DELETE", "TRUNCATE"));

-- COMMAND ----------

-- MAGIC %md SCD - 1

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE customer_master_dlt_cdc_scd1_streaming_table;

-- COMMAND ----------

-- SCD Type 1: Apply changes and delete outdated records
CREATE OR REFRESH STREAMING TABLE customer_master_dlt_cdc_scd1_streaming_table;
APPLY CHANGES INTO
  LIVE.customer_master_dlt_cdc_scd1_streaming_table
FROM
  stream(LIVE.customer_master_dlt_cdc_streaming_table)
KEYS
  (customer_id)  -- Use customer_id as the key for applying changes
APPLY AS DELETE WHEN CUSTOMER_STATUS = 'DELETE'  -- Delete record if status is 'DELETE'
APPLY AS TRUNCATE WHEN CUSTOMER_STATUS = 'TRUNCATE'  -- Truncate record if status is 'TRUNCATE'
SEQUENCE BY UPDATED_DATE  -- Order the changes by the updated date
STORED AS SCD TYPE 1;  -- Store as SCD Type 1, which overwrites old records with new ones

-- COMMAND ----------

-- MAGIC %md SCD - 2

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE customer_master_dlt_cdc_scd2_streaming_table;

-- COMMAND ----------

-- SCD Type 2: Track changes with historical records
CREATE OR REFRESH STREAMING TABLE customer_master_dlt_cdc_scd2_streaming_table;
APPLY CHANGES INTO
  LIVE.customer_master_dlt_cdc_scd2_streaming_table
FROM
  stream(LIVE.customer_master_dlt_cdc_streaming_table)
KEYS
  (customer_id)  -- Use customer_id as the key
APPLY AS DELETE WHEN CUSTOMER_STATUS = 'DELETE'  -- Apply deletion if status is 'DELETE'
SEQUENCE BY UPDATED_DATE  -- Order by updated date for sequence
COLUMNS * EXCEPT 
  (CUSTOMER_STATUS, UPDATED_DATE)  -- Exclude status and updated date from the columns
STORED AS SCD TYPE 2;  -- Store as SCD Type 2, maintaining history of changes

-- COMMAND ----------

-- -- Let make change to the source table i.e. "customer_master_dlt_cdc_streaming_table".

-- SELECT * FROM sample_catalog_v1.sample_schema_v1.customer_master_dlt_cdc_streaming_table;

-- COMMAND ----------

-- INSERT INTO sample_catalog_v1.sample_schema_v1.customer_master_dlt_cdc_streaming_table
-- VALUES (
--   'CUST021', 'Customer_21', 'customer21@gmail.com', '+1234567890', 'Address_21', 'ACTIVE', CURRENT_TIMESTAMP
-- )

-- COMMAND ----------

-- SELECT * FROM sample_catalog_v1.sample_schema_v1.customer_master_dlt_cdc_scd1_streaming_table  WHERE customer_id = 'CUST021';

-- COMMAND ----------

-- SELECT * FROM sample_catalog_v1.sample_schema_v1.customer_master_dlt_cdc_scd2_streaming_table WHERE customer_id = 'CUST021';

-- COMMAND ----------

-- INSERT INTO sample_catalog_v1.sample_schema_v1.customer_master_dlt_cdc_streaming_table
-- VALUES (
--   'CUST021', 'Customer_21', 'customer21@gmail.com', '+1234567890', 'Address_21', 'ACTIVE', CURRENT_TIMESTAMP
-- )

-- COMMAND ----------

-- -- Let make change to the source table i.e. "customer_master_dlt_cdc_streaming_table".

-- SELECT * FROM sample_catalog_v1.sample_schema_v1.customer_master_dlt_cdc_streaming_table;