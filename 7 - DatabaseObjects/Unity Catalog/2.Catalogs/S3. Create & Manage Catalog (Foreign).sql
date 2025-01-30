-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Create and Manage Foreign Catalog
-- MAGIC   0. Pre-Requisites
-- MAGIC   1. How to Create a Foreign Catalog using SQL
-- MAGIC   2. How to Create a Foreign Catalog using Databricks UI
-- MAGIC   3. List all the catalogs SQL / UI
-- MAGIC   4. Get Details of a Catalog using SQL / UI
-- MAGIC   5. How to Rename a Catalog
-- MAGIC   6. How to Update a Catalog
-- MAGIC   7. How to Delete a Catalog using SQL / UI
-- MAGIC   8. Default Schemas in a Catalog

-- COMMAND ----------

-- MAGIC %md ## Pre-Requisites
-- MAGIC Lakehouse Federated Connection
-- MAGIC
-- MAGIC   1. Create Connection using SQL
-- MAGIC   2. Create Connection using UI

-- COMMAND ----------

-- MAGIC %md ## Introduction
-- MAGIC
-- MAGIC -- This notebook covers how to create, manage, and retrieve details about foreign catalogs in Databricks using SQL and UI.
-- MAGIC
-- MAGIC -- It also includes details on creating connections and handling catalog metadata.

-- COMMAND ----------

-- MAGIC %md ## Example: Checking Existing Connections

-- COMMAND ----------

-- Retrieve a list of active federated connections
SHOW CONNECTIONS;

-- COMMAND ----------

-- MAGIC %md ## 1. Creating a Connection using SQL

-- COMMAND ----------

-- Create a connection to an external SQL Server database
CREATE CONNECTION mysqlsql TYPE sqlserver
OPTIONS (
  host 'qbex-sql-server.database.windows.net',
  port '1433',
  user '<USER NAME>',
  password '<PASSWORD>'
);

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC
-- MAGIC ### Example: Creating a Connection using UI
-- MAGIC 1. Navigate to Databricks UI -> Data Explorer
-- MAGIC 2. Select 'Create Connection' and fill in SQL Server details.
-- MAGIC 3. Save the connection and use it in queries.

-- COMMAND ----------

-- MAGIC %md ## 2. Creating a Foreign Catalog

-- COMMAND ----------

-- Create a foreign catalog linked to the SQL Server connection
CREATE FOREIGN CATALOG v_catsql_qbex
USING CONNECTION mysqlsql
OPTIONS (database 'qbex');

-- COMMAND ----------

-- MAGIC %md ### Example: Creating a Foreign Catalog using UI
-- MAGIC 1. Navigate to Databricks UI -> Catalog Explorer
-- MAGIC 2. Click 'Create Catalog' and select 'Foreign Catalog'.
-- MAGIC 3. Choose the existing federated connection and database.
-- MAGIC 4. Save the catalog.

-- COMMAND ----------

-- MAGIC %md ## 3. Listing All Catalogs

-- COMMAND ----------

-- Retrieve a list of available catalogs in Databricks
SHOW CATALOGS;

-- COMMAND ----------

-- MAGIC %md ## 4. Fetching Catalog Details

-- COMMAND ----------

-- Get extended metadata information for a specific foreign catalog
DESC CATALOG EXTENDED v_catsql_qbex;

-- COMMAND ----------

-- MAGIC %md ## 5. Renaming a Foreign Catalog (Workaround)
-- MAGIC
-- MAGIC Direct renaming is not supported. Instead, follow these steps:
-- MAGIC 1. Create a new foreign catalog
-- MAGIC 2. Move dependent objects
-- MAGIC 3. Drop the old catalog

-- COMMAND ----------

CREATE FOREIGN CATALOG new_v_catsql_qbex
USING CONNECTION mysqlsql
OPTIONS (database 'qbex');
DROP CATALOG IF EXISTS v_catsql_qbex CASCADE;

-- COMMAND ----------

-- MAGIC %md ## 6. Updating a Foreign Catalog

-- COMMAND ----------

-- Modify catalog properties if needed
ALTER CATALOG v_catsql_qbex SET COMMENT 'Updated foreign catalog';

-- COMMAND ----------

-- MAGIC %md ## 7. Dropping a Foreign Catalog

-- COMMAND ----------

-- Remove the foreign catalog if it exists
DROP CATALOG IF EXISTS v_catsql_qbex CASCADE;

-- COMMAND ----------

-- MAGIC %md ## 8. Default Schemas in a Catalog

-- COMMAND ----------

-- List all schemas within a catalog
SHOW SCHEMAS IN v_catsql_qbex;