-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Create and Manage Schema
-- MAGIC   1. Create Schema - Default Location (catalog) (UI & SQL)
-- MAGIC   2. Create Schema - Managed Location (UI & SQL)
-- MAGIC   3. View Schema Details
-- MAGIC   4. Delete Schema

-- COMMAND ----------

-- MAGIC %md ## Introduction
-- MAGIC - A schema (or database) is a logical grouping of tables within a catalog.
-- MAGIC - In Databricks, schemas help organize and manage data efficiently.
-- MAGIC - This notebook covers schema creation, management, and deletion using SQL and UI.

-- COMMAND ----------

-- MAGIC %md ## Topics Covered
-- MAGIC 1. Creating a Schema in Default Location (UI & SQL)
-- MAGIC 2. Creating a Schema in a Managed Location (UI & SQL)
-- MAGIC 3. Viewing Schema Details
-- MAGIC 4. Deleting a Schema

-- COMMAND ----------

-- MAGIC %md ## 1. Create Schema - Default Location (Catalog)
-- MAGIC
-- MAGIC - If no location is specified, Databricks stores the schema in the default location.
-- MAGIC - The schema is created inside a catalog (e.g., 'cat_demo_001').

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS cat_demo_001.sch_demo_001;

-- COMMAND ----------

-- Verify the schema creation
SHOW SCHEMAS IN cat_demo_001;

-- COMMAND ----------

-- MAGIC %md ## 2. Create Schema - Managed Location
-- MAGIC - Managed schemas store all tables within a specified storage location.
-- MAGIC - Useful for controlled data management and compliance requirements.

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS qbexcatalog.dummyschema
  MANAGED LOCATION 'abfss://catalogs@dbricksdatasets.dfs.core.windows.net/';

-- COMMAND ----------

-- MAGIC %md ## 3. View Schema Details

-- COMMAND ----------

-- Retrieve details about a schema, including its properties and location.
DESCRIBE SCHEMA cat_demo_001.sch_demo_001;

-- COMMAND ----------

-- MAGIC %md ## 4. Delete Schema
-- MAGIC - Remove an existing schema (use with caution).
-- MAGIC - If the schema contains objects (tables, views, etc.), use CASCADE to force deletion.

-- COMMAND ----------

DROP SCHEMA IF EXISTS qbexcatalog.dummyschema CASCADE;

-- COMMAND ----------

-- MAGIC %md ## Summary
-- MAGIC - We covered how to create schemas in both default and managed locations.
-- MAGIC - Learned how to retrieve schema details.
-- MAGIC - Demonstrated schema deletion for cleanup and organization.
-- MAGIC - Schemas help structure data efficiently within Databricks Unity Catalog.