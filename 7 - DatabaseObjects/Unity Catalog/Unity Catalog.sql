-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Unity Catalog in Databricks
-- MAGIC Unity Catalog is a governance layer that allows you to manage data access, metadata, and lineage across Databricks workspaces.

-- COMMAND ----------

-- MAGIC %md ## 1. Create and Manage Catalogs

-- COMMAND ----------

-- Create a catalog if it does not exist
CREATE CATALOG IF NOT EXISTS iplanalytics;

-- COMMAND ----------

-- List all catalogs
SHOW CATALOGS;

-- COMMAND ----------

-- Describe catalog metadata
DESC CATALOG EXTENDED iplanalytics;

-- COMMAND ----------

-- MAGIC %md ## 2. Creating a Managed Catalog with a Storage Location

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS iplanalytics_v1
MANAGED LOCATION 'abfss://datasets@qbexdatasets.dfs.core.windows.net/';

-- COMMAND ----------

DESC CATALOG EXTENDED iplanalytics_v1;

-- COMMAND ----------

-- MAGIC %md ## 3. Creating and Managing Schemas

-- COMMAND ----------

-- Create a schema within a catalog
CREATE SCHEMA IF NOT EXISTS iplanalytics_v1.raw;

-- COMMAND ----------

-- Describe schema metadata
DESCRIBE SCHEMA EXTENDED iplanalytics_v1.raw;

-- COMMAND ----------

-- MAGIC %md ## 4. Creating and Managing Tables

-- COMMAND ----------

-- Create a table inside the schema
CREATE TABLE IF NOT EXISTS iplanalytics_v1.raw.ipl_matches 
(match_id INT,
player STRING,
runs INT);

-- COMMAND ----------

-- Describe table metadata
DESCRIBE TABLE EXTENDED iplanalytics_v1.raw.ipl_matches;

-- COMMAND ----------

-- MAGIC %md ## 5. Performing Data Operations

-- COMMAND ----------

-- Insert data into the table
INSERT INTO iplanalytics_v1.raw.ipl_matches VALUES (1, 'Pratap', 100);

-- COMMAND ----------

-- Retrieve data
SELECT * FROM iplanalytics_v1.raw.ipl_matches;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # List datasets in the mounted storage
-- MAGIC print(dbutils.fs.ls('/mnt/dbricksdatasets'))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Unity Catalog: Access Control, Lineage Tracking & Data Sharing

-- COMMAND ----------

-- MAGIC %md ### 1. Access Control

-- COMMAND ----------

-- MAGIC %md #### Granting & Revoking Permissions

-- COMMAND ----------

-- Create a catalog if it doesn't exist
CREATE CATALOG IF NOT EXISTS iplanalytics;

-- COMMAND ----------

-- Grant SELECT permission on a table to a user
GRANT SELECT ON TABLE iplanalytics.raw.ipl_matches TO user 'data_analyst@company.com';

-- COMMAND ----------

-- Revoke SELECT permission
REVOKE SELECT ON TABLE iplanalytics.raw.ipl_matches FROM user 'data_analyst@company.com';

-- COMMAND ----------

-- Grant ALL privileges to a role
GRANT ALL PRIVILEGES ON SCHEMA iplanalytics.raw TO role 'data_engineers';

-- COMMAND ----------

-- Show current privileges
SHOW GRANTS ON TABLE iplanalytics.raw.ipl_matches;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2. Lineage Tracking

-- COMMAND ----------

-- MAGIC %md #### Understanding Data Lineage

-- COMMAND ----------

-- Query to track table dependencies (view lineage via UI in Catalog Explorer)
DESCRIBE TABLE EXTENDED iplanalytics.raw.ipl_matches;

-- COMMAND ----------

-- Example transformation that can be tracked in lineage
CREATE OR REPLACE VIEW iplanalytics.analysis.top_scorers AS
SELECT player, SUM(runs) AS total_runs
FROM iplanalytics.raw.ipl_matches
GROUP BY player;

-- COMMAND ----------

-- Check lineage of the new view
DESCRIBE TABLE EXTENDED iplanalytics.analysis.top_scorers;

-- COMMAND ----------

-- MAGIC %md ### 3. Data Sharing with Delta Sharing

-- COMMAND ----------

-- MAGIC %md #### Securely Sharing Data

-- COMMAND ----------

-- Create a share
CREATE SHARE cricket_data_share;

-- COMMAND ----------

-- Add a table to the share
ALTER SHARE cricket_data_share ADD TABLE iplanalytics.raw.ipl_matches;

-- COMMAND ----------

-- Grant access to an external user
GRANT SELECT ON SHARE cricket_data_share TO 'partner@externalcompany.com';

-- COMMAND ----------

-- List shares
SHOW SHARES;

-- COMMAND ----------

-- Consumer access (external user fetching shared data)
SELECT * FROM iplanalytics.raw.ipl_matches;

-- COMMAND ----------

-- MAGIC %md ## Summary
-- MAGIC | Feature | Purpose | Key Benefit |
-- MAGIC |-----------|------------|----------------|
-- MAGIC | **Access Control** | Restricts access using RBAC/ABAC | Prevents unauthorized access & ensures security |
-- MAGIC | **Lineage Tracking** | Tracks data flow & dependencies | Helps in troubleshooting, auditing & compliance |
-- MAGIC | **Data Sharing** | Enables external & cross-cloud data access | Eliminates data duplication & simplifies collaboration |