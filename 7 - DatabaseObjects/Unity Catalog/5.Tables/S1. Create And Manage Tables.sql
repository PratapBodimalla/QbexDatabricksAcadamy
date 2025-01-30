-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Create and Manage Tables in Databricks
-- MAGIC Databricks supports multiple types of tables to handle different use cases efficiently. Below, we explore:
-- MAGIC
-- MAGIC 1. **Managed Tables**
-- MAGIC 2. **External Tables**
-- MAGIC 3. **Delta Tables**
-- MAGIC 4. **Streaming Tables**
-- MAGIC 5. **Foreign Tables**
-- MAGIC 6. **Feature Tables**
-- MAGIC 7. **Hive Tables (Legacy)**
-- MAGIC 8. **Live Tables (Deprecated)**

-- COMMAND ----------

-- MAGIC %md ## 1. Managed Tables
-- MAGIC
-- MAGIC Managed tables are stored in a Databricks-managed location, and dropping them removes both metadata and data.

-- COMMAND ----------

DROP TABLE IF EXISTS qbexcatalog.qbexschema.student;

-- COMMAND ----------

CREATE TABLE qbexcatalog.qbexschema.student (
    id INT, 
    name STRING, 
    age INT
);

-- COMMAND ----------

-- MAGIC %md ### Describe the table structure

-- COMMAND ----------

DESC TABLE qbexcatalog.qbexschema.student;

-- COMMAND ----------

DESC TABLE EXTENDED qbexcatalog.qbexschema.student;

-- COMMAND ----------

-- MAGIC %md ### Insert and query data

-- COMMAND ----------

INSERT INTO qbexcatalog.qbexschema.student VALUES (1, 'Pratap', 10);

-- COMMAND ----------

SELECT * FROM qbexcatalog.qbexschema.student;

-- COMMAND ----------

-- MAGIC %md ### Using IF NOT EXISTS

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS qbexcatalog.qbexschema.student (
    id INT, 
    name STRING, 
    age INT
);

-- COMMAND ----------

-- MAGIC %md ### Using OR REPLACE to overwrite table

-- COMMAND ----------

CREATE OR REPLACE TABLE qbexcatalog.qbexschema.student (
    id INT NOT NULL, 
    name STRING, 
    age INT
);

-- COMMAND ----------

-- Insert the sample records.
INSERT INTO qbexcatalog.qbexschema.student (id, name, age) VALUES (1, 'Pratap', 10);

-- COMMAND ----------

-- Query the sql table.
SELECT * FROM qbexcatalog.qbexschema.student;

-- COMMAND ----------

-- MAGIC %md ## 2. Delta Tables
-- MAGIC
-- MAGIC Delta tables provide ACID transactions, versioning, and schema enforcement.

-- COMMAND ----------

CREATE OR REPLACE TABLE qbexcatalog.qbexschema.sales (
    id INT, 
    price FLOAT, 
    qty FLOAT, 
    rev FLOAT GENERATED ALWAYS AS (price * qty)
);

-- COMMAND ----------

-- MAGIC %md ### Insert and query data

-- COMMAND ----------

INSERT INTO qbexcatalog.qbexschema.sales (id, price, qty) VALUES (1, 10, 20);

-- COMMAND ----------

SELECT * FROM qbexcatalog.qbexschema.sales;

-- COMMAND ----------

-- MAGIC %md ### Using IDENTITY column for auto-increment

-- COMMAND ----------

CREATE OR REPLACE TABLE qbexcatalog.qbexschema.sales (
    id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1), 
    price FLOAT, 
    qty FLOAT, 
    rev FLOAT GENERATED ALWAYS AS (price * qty)
);

-- COMMAND ----------

INSERT INTO qbexcatalog.qbexschema.sales (price, qty) VALUES (10, 20);

-- COMMAND ----------

SELECT * FROM qbexcatalog.qbexschema.sales;

-- COMMAND ----------

-- MAGIC %md ### Adding default column values

-- COMMAND ----------

CREATE OR REPLACE TABLE qbexcatalog.qbexschema.sales (
    id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1), 
    price FLOAT, 
    qty FLOAT, 
    rev FLOAT GENERATED ALWAYS AS (price * qty), 
    region STRING DEFAULT 'south'
) TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'enabled');

-- COMMAND ----------

INSERT INTO qbexcatalog.qbexschema.sales (price, qty) VALUES (5, 4);

-- COMMAND ----------

SELECT * FROM qbexcatalog.qbexschema.sales;

-- COMMAND ----------

-- MAGIC %md ### Using Primary Key in Delta Table

-- COMMAND ----------

CREATE OR REPLACE TABLE qbexcatalog.qbexschema.sales (
    id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1) PRIMARY KEY, 
    price FLOAT, 
    qty FLOAT, 
    rev FLOAT GENERATED ALWAYS AS (price * qty), 
    region STRING DEFAULT 'south'
) TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'enabled');

-- COMMAND ----------

DESC TABLE EXTENDED qbexcatalog.qbexschema.sales;

-- COMMAND ----------

-- MAGIC %md ### Adding more columns dynamically

-- COMMAND ----------

CREATE OR REPLACE TABLE qbexcatalog.qbexschema.sales (
    id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1) PRIMARY KEY, 
    price FLOAT, 
    qty FLOAT, 
    rev FLOAT GENERATED ALWAYS AS (price * qty), 
    region STRING DEFAULT 'south',
    phone STRING
) TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'enabled');

-- COMMAND ----------

-- MAGIC %md ## 3. External Tables
-- MAGIC
-- MAGIC External tables store data externally but manage metadata within Databricks.

-- COMMAND ----------

CREATE EXTERNAL TABLE qbexcatalog.qbexschema.external_student
(id INT, name STRING, age INT)
LOCATION 'abfss://samplevolumes@qbexmetastore.dfs.core.windows.net/sampledata/';

-- COMMAND ----------

-- MAGIC %md ### Insert and query data

-- COMMAND ----------

-- Insert the sample records into external tables.
INSERT INTO qbexcatalog.qbexschema.external_student (id, name, age) VALUES (1, 'Pratap', 10);

-- COMMAND ----------

-- Query the sql external table.
SELECT * FROM qbexcatalog.qbexschema.external_student;

-- COMMAND ----------

-- MAGIC %md ## 4. Streaming Tables
-- MAGIC
-- MAGIC Streaming tables process real-time data continuously.

-- COMMAND ----------

CREATE STREAMING TABLE qbexcatalog.qbexschema.streaming_sales (
    id INT, 
    price FLOAT, 
    qty FLOAT
);

-- COMMAND ----------

INSERT INTO qbexcatalog.qbexschema.streaming_sales VALUES (1, 10, 5);

-- COMMAND ----------

SELECT * FROM qbexcatalog.qbexschema.streaming_sales;

-- COMMAND ----------

-- MAGIC %md ## 5. Foreign Tables
-- MAGIC
-- MAGIC Foreign tables reference data from external databases.

-- COMMAND ----------

CREATE FOREIGN TABLE qbexcatalog.qbexschema.foreign_sales (
    id INT, 
    price FLOAT
) OPTIONS (
    database 'external_db', 
    table 'external_sales'
);


-- COMMAND ----------

SELECT * FROM qbexcatalog.qbexschema.foreign_sales;

-- COMMAND ----------

-- MAGIC %md ## 6. Feature Tables (for ML use cases)
-- MAGIC
-- MAGIC Feature tables store feature data for ML pipelines.

-- COMMAND ----------

CREATE TABLE qbexcatalog.qbexschema.feature_sales (
    id INT, 
    feature_vector ARRAY<FLOAT>
) USING DELTA;

-- COMMAND ----------

-- MAGIC %md ## 7. Hive Tables (Legacy)
-- MAGIC
-- MAGIC
-- MAGIC Hive tables are based on Apache Hive’s metastore.

-- COMMAND ----------

CREATE TABLE qbexcatalog.qbexschema.hive_student (
    id INT, 
    name STRING
) STORED AS PARQUET;

-- COMMAND ----------

-- MAGIC %md ## 8. Live Tables (Deprecated)
-- MAGIC
-- MAGIC Deprecated but previously used for continuous ingestion.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS live_table (
    id INT, value STRING
);

-- COMMAND ----------

-- MAGIC %md ## Drop Tables

-- COMMAND ----------

DROP TABLE IF EXISTS qbexcatalog.qbexschema.student;

-- COMMAND ----------

DROP TABLE IF EXISTS qbexcatalog.qbexschema.sales;

-- COMMAND ----------

-- MAGIC %md ## Summary:
-- MAGIC - Managed tables are fully controlled by Databricks.
-- MAGIC - External tables store data externally but manage metadata.
-- MAGIC - Delta tables provide versioning and ACID transactions.
-- MAGIC - Streaming tables handle continuous real-time ingestion.
-- MAGIC - Foreign tables reference external databases.
-- MAGIC - Feature tables are for ML feature storage.
-- MAGIC - Hive tables store data using the Hive metastore.
-- MAGIC - Live tables are deprecated for newer approaches.