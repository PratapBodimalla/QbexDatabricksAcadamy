-- Databricks notebook source
-- MAGIC %md ## Introduction to Metastores
-- MAGIC - A Metastore in Databricks is a central metadata repository that stores metadata about tables, schemas, and catalogs.
-- MAGIC - It is crucial for managing structured data and enables secure access across different Databricks workspaces.

-- COMMAND ----------

-- MAGIC %md ## Topics Covered
-- MAGIC 1. What is a Metastore?
-- MAGIC 2. Setting Up a Metastore
-- MAGIC 3. Managing Metastores
-- MAGIC 4. Connecting to a Metastore
-- MAGIC 5. Best Practices
-- MAGIC 6. Advanced Configurations
-- MAGIC 7. Live Examples

-- COMMAND ----------

-- MAGIC %md ## 1. What is a Metastore?
-- MAGIC A Metastore is responsible for:
-- MAGIC - Storing metadata for Unity Catalogs.
-- MAGIC - Managing user access and permissions.
-- MAGIC - Supporting multi-workspace data governance.

-- COMMAND ----------

-- MAGIC %md ## 2. Setting Up a Metastore
-- MAGIC
-- MAGIC Before creating a metastore, ensure you have the necessary admin permissions.

-- COMMAND ----------

-- Creating a Metastore
CREATE METASTORE my_metastore LOCATION 'abfss://mycontainer@mystorage.dfs.core.windows.net/';

-- COMMAND ----------

-- Assigning a Metastore to a Workspace
USE METASTORE my_metastore;

-- COMMAND ----------

-- MAGIC %md ## 3. Managing Metastores

-- COMMAND ----------

-- Listing Available Metastores
SHOW METASTORES;

-- COMMAND ----------

-- Describing a Specific Metastore
DESCRIBE METASTORE my_metastore;

-- COMMAND ----------

-- Dropping a Metastore (Use with Caution)
DROP METASTORE my_metastore;

-- COMMAND ----------

-- MAGIC %md ## 4. Connecting to a Metastore
-- MAGIC
-- MAGIC -- A Metastore must be linked to a Unity Catalog for full functionality.
-- MAGIC
-- MAGIC -- Assign a workspace to the Metastore using Databricks Admin Console.

-- COMMAND ----------

-- MAGIC %md ## 5. Best Practices
-- MAGIC - Use a single Metastore per region to maintain data consistency.
-- MAGIC - Assign appropriate permissions to avoid unauthorized access.
-- MAGIC - Regularly back up metadata for disaster recovery.

-- COMMAND ----------

-- MAGIC %md ## 6. Advanced Configurations
-- MAGIC - Configure cross-region metastores.
-- MAGIC - Enable access control policies for secure data sharing.

-- COMMAND ----------

-- MAGIC %md ## 7. Live Examples

-- COMMAND ----------

-- Creating a Catalog within a Metastore
CREATE CATALOG IF NOT EXISTS my_catalog;

-- COMMAND ----------

-- Creating a Schema within a Catalog
CREATE SCHEMA IF NOT EXISTS my_catalog.my_schema;

-- COMMAND ----------

-- Creating a Table within a Schema
CREATE TABLE my_catalog.my_schema.my_table (
    id INT,
    name STRING,
    created_at TIMESTAMP
);

-- COMMAND ----------

-- Querying the Table
SELECT * FROM my_catalog.my_schema.my_table;

-- COMMAND ----------

-- MAGIC %md ## Summary
-- MAGIC - This notebook covers the fundamental and advanced concepts of metastores in Databricks.
-- MAGIC - By following the steps outlined, users can efficiently manage their data governance setup in Unity Catalog.