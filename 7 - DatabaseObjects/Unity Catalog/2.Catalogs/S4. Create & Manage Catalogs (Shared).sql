-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Create and Manage Shared Catalogs
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
-- MAGIC This notebook covers how to create, manage, and retrieve details about shared catalogs in Databricks.
-- MAGIC
-- MAGIC It includes steps for creating recipients, creating shares, assigning recipients, and adding tables to shares.

-- COMMAND ----------

-- MAGIC %md ## 0. Pre-Requisites
-- MAGIC
-- MAGIC Ensure the following prerequisites before creating a shared catalog:
-- MAGIC 1. Proper Databricks workspace setup.
-- MAGIC 2. Required permissions for creating shares and assigning recipients.

-- COMMAND ----------

-- MAGIC %md ## 1. Creating Recipients (Databricks Consumer)

-- COMMAND ----------

-- Create a recipient for a Databricks consumer using a unique ID
CREATE RECIPIENT IF NOT EXISTS sruthi_db_reciepient_sql
    USING ID 'azure:centralindia:<Unique Databricks CONSUMER ID>';

-- COMMAND ----------

-- Create another recipient without specifying an ID
CREATE RECIPIENT IF NOT EXISTS sruthi_nondb_reciepient_sql;

-- COMMAND ----------

-- MAGIC %md ## 2. Creating a Share

-- COMMAND ----------

-- Create a share if it does not exist
CREATE SHARE IF NOT EXISTS sql_sales_share;

-- COMMAND ----------

-- MAGIC %md ## 3. Assigning Recipients to a Share

-- COMMAND ----------

-- Grant access to recipients for the created share
GRANT SELECT ON SHARE sql_sales_share TO RECIPIENT sruthi_db_reciepient_sql;

-- COMMAND ----------

GRANT SELECT ON SHARE sql_sales_share TO RECIPIENT sruthi_nondb_reciepient_sql;

-- COMMAND ----------

-- MAGIC %md ## 4. Adding Tables to a Share

-- COMMAND ----------

-- Add a table to an existing share
ALTER SHARE sql_sales_share ADD TABLE qbexcatalog.qbexschema.salesordersdata;

-- COMMAND ----------

-- MAGIC %md ## 5. Listing Shared Resources

-- COMMAND ----------

-- Show all objects in the specified share
SHOW ALL IN SHARE sql_sales_share;

-- COMMAND ----------

-- MAGIC %md ## 6. Checking Recipient Access

-- COMMAND ----------

-- Retrieve details about a specific recipient
DESCRIBE RECIPIENT sruthi_nondb_reciepient_sql;

-- COMMAND ----------

DESCRIBE RECIPIENT sruthi_db_reciepient_sql;

-- COMMAND ----------

-- MAGIC %md ## Additional Learning: Practical Workarounds

-- COMMAND ----------

-- MAGIC %md ### Example: Checking Available Shares

-- COMMAND ----------

SHOW SHARES;

-- COMMAND ----------

-- MAGIC %md ### Example: Viewing Tables in a Shared Catalog

-- COMMAND ----------

SHOW TABLES IN qbexcatalog.qbexschema;

-- COMMAND ----------

-- MAGIC %md ### Example: Removing a Table from a Share

-- COMMAND ----------

ALTER SHARE sql_sales_share REMOVE TABLE qbexcatalog.qbexschema.salesordersdata;

-- COMMAND ----------

-- MAGIC %md ### Example: Revoking Access from a Recipient

-- COMMAND ----------

REVOKE SELECT ON SHARE sql_sales_share FROM RECIPIENT sruthi_db_reciepient_sql;