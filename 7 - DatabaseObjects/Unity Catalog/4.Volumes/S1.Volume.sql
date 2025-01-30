-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Create and Manage Volumes
-- MAGIC   1. Create Managed Volume ( UI & SQL )
-- MAGIC   2. Create External Volume ( UI & SQL )
-- MAGIC   3. Manage Files in Volume
-- MAGIC
-- MAGIC       - 3.a) Upload files to Volume
-- MAGIC       - 3.b) Download files from a volume
-- MAGIC       - 3.c) Delete files from a volume
-- MAGIC       - 3.d) Create a blank directory
-- MAGIC       - 3.e) Delete directories from a volume
-- MAGIC       - 3.f) Create a table from data in a volume
-- MAGIC       - 3.g) Programmatically work with files in volumes on Azure Databricks
-- MAGIC           - Read Files
-- MAGIC             - Python
-- MAGIC             - SQL
-- MAGIC             - Pandas
-- MAGIC           - Utility commands for files in volumes
-- MAGIC           - SQL commands for files in volumes
-- MAGIC             - PUT INTO
-- MAGIC             - GET
-- MAGIC             - REMOVE
-- MAGIC             - LIST

-- COMMAND ----------

-- MAGIC %md This notebook covers how to create and manage volumes in Databricks, including both managed and external volumes. You will also learn how to upload, download, and manipulate files within a volume using SQL and Python.

-- COMMAND ----------

-- MAGIC %md ## 1. Create Managed Volume (UI & SQL)
-- MAGIC
-- MAGIC Managed volumes are created within a schema and are fully managed by Databricks.

-- COMMAND ----------

-- Creating a managed volume within a schema
CREATE VOLUME IF NOT EXISTS cat_demo_001.sch_demo_001.vol_demo_001;

-- COMMAND ----------

-- Creating another managed volume within a different schema
CREATE VOLUME IF NOT EXISTS qbexcatalog.qbexschema.qbexvolume;

-- COMMAND ----------

-- MAGIC %md ## 2. Create External Volume (UI & SQL)
-- MAGIC
-- MAGIC An external volume is stored in an external location, such as an Azure Data Lake container.

-- COMMAND ----------

-- CREATE EXTERNAL VOLUME <catalog>.<schema>.<external-volume-name>
-- LOCATION 'abfss://<container-name>@<storage-account>.dfs.core.windows.net/<path>/<directory>';

-- COMMAND ----------

-- Creating an external volume with a specified location
CREATE EXTERNAL VOLUME qbexcatalog.qbexschema.external_volume
LOCATION 'abfss://container-name@storage-account.dfs.core.windows.net/path/directory';

-- COMMAND ----------

-- MAGIC %md ## 3. Managing Files in a Volume

-- COMMAND ----------

-- MAGIC %md ### 3.a) Upload Files to a Volume
-- MAGIC
-- MAGIC Use the PUT command to upload files to a volume.

-- COMMAND ----------

-- Uploading a CSV file into the volume
PUT 'C:\Users\User\Documents\Datasets\ipl_matches.csv'
INTO '/Volumes/qbexcatalog/qbexschema/qbexvolume/ipl_matches.csv' OVERWRITE;

-- COMMAND ----------

-- MAGIC %md ### 3.b) Download Files from a Volume
-- MAGIC
-- MAGIC Use GET to download files from a volume.

-- COMMAND ----------

-- Downloading a file from a volume
GET '/Volumes/qbexcatalog/qbexschema/qbexvolume/ipl_matches.csv'
TO 'C:\Users\User\Downloads\ipl_matches.csv';

-- COMMAND ----------

-- MAGIC %md ### 3.c) Delete Files from a Volume
-- MAGIC
-- MAGIC Use REMOVE to delete files from a volume.

-- COMMAND ----------

-- Deleting a specific file from the volume
REMOVE '/Volumes/qbexcatalog/qbexschema/qbexvolume/ipl_matches.csv';

-- COMMAND ----------

-- MAGIC %md ### 3.d) Create a Blank Directory
-- MAGIC
-- MAGIC Use mkdirs in Python to create a directory.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Creating a directory inside a volume
-- MAGIC dbutils.fs.mkdirs("/Volumes/qbexcatalog/qbexschema/qbexvolume/new_directory/")

-- COMMAND ----------

-- MAGIC %md ### 3.e) Delete Directories from a Volume
-- MAGIC
-- MAGIC Use rm in Python to delete a directory.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Deleting a directory inside a volume
-- MAGIC dbutils.fs.rm("/Volumes/qbexcatalog/qbexschema/qbexvolume/new_directory/", True)

-- COMMAND ----------

-- MAGIC %md ### 3.f) Create a Table from Data in a Volume
-- MAGIC
-- MAGIC Using SQL to create a table directly from a CSV file stored in a volume.

-- COMMAND ----------

-- Creating a table from a CSV file inside a volume
CREATE TABLE qbexcatalog.qbexschema.ipl_matches
USING CSV
LOCATION '/Volumes/qbexcatalog/qbexschema/qbexvolume/ipl_matches.csv';

-- COMMAND ----------

-- MAGIC %md ### 3.g) Programmatically Work with Files in Volumes
-- MAGIC
-- MAGIC Read Files using PySpark

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Reading CSV file from a volume using PySpark
-- MAGIC from pyspark.sql import SparkSession
-- MAGIC spark = SparkSession.builder.getOrCreate()
-- MAGIC df = spark.read.format("csv").load("/Volumes/qbexcatalog/qbexschema/qbexvolume/ipl_matches.csv")
-- MAGIC display(df)

-- COMMAND ----------

-- MAGIC %md Read Files using SQL

-- COMMAND ----------

-- Reading a CSV file from a volume using SQL
SELECT * FROM csv.`/Volumes/qbexcatalog/qbexschema/qbexvolume/ipl_matches.csv`;

-- COMMAND ----------

-- MAGIC %md Read Files using Pandas

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Reading CSV file from a volume using Pandas
-- MAGIC import pandas as pd
-- MAGIC df = pd.read_csv('/Volumes/qbexcatalog/qbexschema/qbexvolume/ipl_matches.csv')
-- MAGIC display(df)

-- COMMAND ----------

-- MAGIC %md ## 4. Utility Commands for Files in Volumes

-- COMMAND ----------

-- MAGIC %md ### Listing Files in a Volume

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC
-- MAGIC # Listing all files inside a volume
-- MAGIC dbutils.fs.ls("/Volumes/qbexcatalog/qbexschema/qbexvolume/")

-- COMMAND ----------

-- MAGIC %md ## 5. SQL Commands for Files in Volumes

-- COMMAND ----------

-- Listing all files in a volume
LIST '/Volumes/qbexcatalog/qbexschema/qbexvolume/ipl/';

-- COMMAND ----------

-- Removing a specific file
REMOVE '/Volumes/qbexcatalog/qbexschema/qbexvolume/ipl/ipl_matches.csv';

-- COMMAND ----------

-- MAGIC %md ## Summary
-- MAGIC
-- MAGIC This document covered:
-- MAGIC - Creating managed and external volumes
-- MAGIC - Uploading, downloading, and deleting files
-- MAGIC - Creating and managing directories
-- MAGIC - Creating tables from volume data
-- MAGIC - Reading volume data using PySpark, Pandas, and SQL
-- MAGIC - Utility and SQL commands for file handling
-- MAGIC
-- MAGIC By using these features, you can efficiently manage and work with volumes in Databricks Unity Catalog.