-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Create and Manage Standard Catalog
-- MAGIC   1. How to Create a Standadrd Catalog using SQL
-- MAGIC   2. How to Create a Standadrd Catalog using Databricks UI
-- MAGIC   3. How to Create a Standadrd Catalog using SQL Using External Location
-- MAGIC   4. List all the catalogs SQL / UI
-- MAGIC   5. Get Details of a Catalog using SQL / UI
-- MAGIC   6. How to Rename a Catalog
-- MAGIC   7. How to Update a Catalog
-- MAGIC   8. How to Delete a Catalog using SQL / UI
-- MAGIC   9. Default Schemas in a Catalog

-- COMMAND ----------

-- MAGIC %md ## Introduction
-- MAGIC
-- MAGIC -- This notebook covers how to create, manage, and retrieve details about standard catalogs in Databricks.
-- MAGIC
-- MAGIC -- It includes SQL commands for creating standard catalogs, managing external locations, and handling catalog metadata.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. How to Create a Standard Catalog using SQL

-- COMMAND ----------

-- Create a catalog if it does not exist
CREATE CATALOG IF NOT EXISTS cat_demo_001;

-- COMMAND ----------

-- Retrieve detailed metadata of the catalog
DESCRIBE CATALOG EXTENDED cat_demo_001;

-- COMMAND ----------

-- List all available catalogs
SHOW CATALOGS;

-- COMMAND ----------

-- Drop the catalog if it exists (including all dependent objects)
DROP CATALOG IF EXISTS cat_demo_001 CASCADE;

-- COMMAND ----------

-- MAGIC %md ## 2. Creating a Standard Catalog using Databricks UI
-- MAGIC
-- MAGIC 1. Navigate to the Databricks Catalog Explorer.
-- MAGIC 2. Click on "Create Catalog" and enter the required details such as name and owner.
-- MAGIC 3. Set the optional managed location if needed.
-- MAGIC 4. Click "Create" to finalize the catalog creation.

-- COMMAND ----------

-- MAGIC %md ## 3. Creating a Standard Catalog using SQL with External Location

-- COMMAND ----------

-- MAGIC %md ### Pre-Requisites
-- MAGIC 1. Create Storage Credentials
-- MAGIC 2. Create External Connection

-- COMMAND ----------

-- Create an external location for storing metadata
CREATE EXTERNAL LOCATION IF NOT EXISTS eloc_cat_demo_001
    URL 'abfss://catdemo001@qbexmetastore.dfs.core.windows.net/'
    WITH (STORAGE CREDENTIAL sc_cat_demo_001)
    COMMENT 'This is the location for storing metadata for the catalog cat_demo_001';

-- COMMAND ----------

-- Create a catalog using the external location
CREATE CATALOG IF NOT EXISTS cat_demo_001
    MANAGED LOCATION 'abfss://catdemo001@qbexmetastore.dfs.core.windows.net/';

-- COMMAND ----------

-- Retrieve catalog details
DESC CATALOG EXTENDED cat_demo_001;

-- COMMAND ----------

-- Drop a catalog if it exists
DROP CATALOG IF EXISTS cat_ipl_sql CASCADE;

-- COMMAND ----------

-- MAGIC %md ## 4. Listing All Catalogs (SQL / UI)

-- COMMAND ----------

-- Using SQL:
SHOW CATALOGS;

-- COMMAND ----------

-- Using UI:
-- Navigate to the Databricks Catalog Explorer to view all available catalogs.

-- COMMAND ----------

-- MAGIC %md ## 5. Getting Details of a Catalog (SQL / UI)

-- COMMAND ----------

-- Using SQL:
DESCRIBE CATALOG EXTENDED cat_demo_001;

-- COMMAND ----------

-- Using UI:
-- Navigate to the Catalog Explorer and select the catalog to view its details.

-- COMMAND ----------

-- MAGIC %md ## 6. Renaming a Catalog (Workaround)
-- MAGIC
-- MAGIC -- Renaming a catalog is not directly supported.
-- MAGIC
-- MAGIC -- Workaround: Create a new catalog, move objects, and drop the old one.

-- COMMAND ----------

CREATE CATALOG new_cat_demo_001;
-- Move objects manually
DROP CATALOG IF EXISTS cat_demo_001 CASCADE;

-- COMMAND ----------

-- MAGIC %md ## 7. Updating a Catalog

-- COMMAND ----------

-- Updating certain properties of a catalog, such as comments or owners:
ALTER CATALOG cat_demo_001 SET OWNER TO 'new_owner';
ALTER CATALOG cat_demo_001 SET COMMENT 'Updated catalog description';

-- COMMAND ----------

-- MAGIC %md ## 8. Deleting a Catalog (SQL / UI)

-- COMMAND ----------

-- Using SQL:
DROP CATALOG IF EXISTS cat_demo_001 CASCADE;

-- COMMAND ----------

-- Using UI:
-- Navigate to the Catalog Explorer, select the catalog, and choose "Delete".

-- COMMAND ----------

-- MAGIC %md ## 9. Default Schemas in a Catalog

-- COMMAND ----------

-- When a catalog is created, it includes default schemas such as:
SHOW SCHEMAS IN cat_demo_001;