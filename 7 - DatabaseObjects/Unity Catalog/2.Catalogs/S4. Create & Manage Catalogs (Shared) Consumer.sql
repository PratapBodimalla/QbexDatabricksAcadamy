-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Create and Manage Shared Catalogs (CONSUMER)
-- MAGIC   0. Pre-Requistes
-- MAGIC   1. Create Reciepients - Databricks Consumer
-- MAGIC   2. Create Reciepients - Databricks Consumer
-- MAGIC   3. Create a Share
-- MAGIC   4. Assign Recipients to a Share
-- MAGIC   5. Add tables to Share
-- MAGIC   6. How Reciepts will be able to access the data

-- COMMAND ----------

-- MAGIC %md ## Introduction
-- MAGIC
-- MAGIC -- This notebook covers the process of creating and managing shared catalogs in Databricks for consumers.
-- MAGIC
-- MAGIC -- It includes SQL commands for setting up recipients, creating shares, assigning recipients, and accessing shared data.

-- COMMAND ----------

-- MAGIC %md ## 0. Pre-Requisites

-- COMMAND ----------

-- Ensure that you have the necessary permissions and an existing provider with shares available.
SELECT current_metastore();

-- COMMAND ----------

SHOW PROVIDERS;

-- COMMAND ----------

-- MAGIC %md ## 1. Create Recipients - Databricks Consumer

-- COMMAND ----------

-- Recipients define which users or organizations can access the shared data.
CREATE RECIPIENT IF NOT EXISTS recipient_name
    USING ID 'azure:region:unique-id';

-- COMMAND ----------

-- MAGIC %md ## 2. Create Recipients - Databricks Consumer (Alternative Method)

-- COMMAND ----------

-- If required, create another recipient using a different method.
CREATE RECIPIENT IF NOT EXISTS another_recipient_name;

-- COMMAND ----------

-- MAGIC %md ## 3. Create a Share

-- COMMAND ----------

-- A share is a container for datasets that are shared with external consumers.
CREATE SHARE IF NOT EXISTS shared_data;

-- COMMAND ----------

-- MAGIC %md ## 4. Assign Recipients to a Share

-- COMMAND ----------

-- Grant access to the created recipients so they can use the shared data.
GRANT USAGE ON SHARE shared_data TO RECIPIENT recipient_name;

-- COMMAND ----------

GRANT USAGE ON SHARE shared_data TO RECIPIENT another_recipient_name;

-- COMMAND ----------

-- MAGIC %md ## 5. Add Tables to Share

-- COMMAND ----------

-- Tables from an existing catalog and schema can be added to a share.
ALTER SHARE shared_data ADD TABLE catalog_name.schema_name.table_name;
SHOW ALL IN SHARE shared_data;

-- COMMAND ----------

-- MAGIC %md ## 6. How Recipients Will Be Able to Access the Data

-- COMMAND ----------

-- Recipients can view and query shared data using the following commands:
SHOW SHARES IN PROVIDER provider_name;

-- COMMAND ----------

DESCRIBE PROVIDER provider_name;

-- COMMAND ----------

-- Create a catalog using the shared data
CREATE CATALOG consumer_catalog USING SHARE provider_name.shared_data;

-- COMMAND ----------

-- Query data from the shared catalog
SELECT * FROM consumer_catalog.schema_name.table_name;