-- Databricks notebook source
-- MAGIC %md 
-- MAGIC
-- MAGIC The Databricks notebook demonstrates how to use Auto Loader to efficiently ingest CSV data into a Delta table using streaming. It covers various operations like creating directories for input data and checkpoint storage, copying files from a source location to the input directories, and configuring Auto Loader to process incoming CSV files. The notebook uses Auto Loader with a checkpoint mechanism to handle incremental data ingestion and applies some SQL commands to validate the data in the Delta table. Additionally, it demonstrates a clean-up process to remove the directories used for storage once the operation is complete.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Create a directory for Auto Loader to read input data and store it in the DBFS (Databricks File System).
-- MAGIC dbutils.fs.mkdirs("/Volumes/sample_catalog_v1/sample_schema_v1/sample_volumn_v1/auto_loader_dir/")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Create subdirectories for storing input data and checkpoint information.
-- MAGIC dbutils.fs.mkdirs("/Volumes/sample_catalog_v1/sample_schema_v1/sample_volumn_v1/auto_loader_dir/input_data/")
-- MAGIC
-- MAGIC dbutils.fs.mkdirs("/Volumes/sample_catalog_v1/sample_schema_v1/sample_volumn_v1/auto_loader_dir/checkpoint/autoloader/")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Create further subdirectories for specific retail daywise data under the input data directory.
-- MAGIC dbutils.fs.mkdirs("/Volumes/sample_catalog_v1/sample_schema_v1/sample_volumn_v1/auto_loader_dir/input_data/retail_daywise/2011/12/01/")
-- MAGIC dbutils.fs.mkdirs("/Volumes/sample_catalog_v1/sample_schema_v1/sample_volumn_v1/auto_loader_dir/input_data/retail_daywise/2011/12/02/")
-- MAGIC dbutils.fs.mkdirs("/Volumes/sample_catalog_v1/sample_schema_v1/sample_volumn_v1/auto_loader_dir/input_data/retail_daywise/2011/12/04/")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # dbutils.fs.mkdirs("/Volumes/sample_catalog_v1/sample_schema_v1/sample_volumn_v1/auto_loader_dir/input_data/retail_daywise/2011/12/05/")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # List the contents of the source directory to see the available retail data files.
-- MAGIC dbutils.fs.ls("/databricks-datasets/definitive-guide/data/retail-data/by-day/")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Copy retail data files into their respective directories in DBFS.
-- MAGIC dbutils.fs.cp("/databricks-datasets/definitive-guide/data/retail-data/by-day/2011-12-01.csv", "/Volumes/sample_catalog_v1/sample_schema_v1/sample_volumn_v1/auto_loader_dir/input_data/retail_daywise/2011/12/01/")
-- MAGIC dbutils.fs.cp("/databricks-datasets/definitive-guide/data/retail-data/by-day/2011-12-02.csv", "/Volumes/sample_catalog_v1/sample_schema_v1/sample_volumn_v1/auto_loader_dir/input_data/retail_daywise/2011/12/02/")
-- MAGIC dbutils.fs.cp("/databricks-datasets/definitive-guide/data/retail-data/by-day/2011-12-04.csv", "/Volumes/sample_catalog_v1/sample_schema_v1/sample_volumn_v1/auto_loader_dir/input_data/retail_daywise/2011/12/04/")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # dbutils.fs.cp("/databricks-datasets/definitive-guide/data/retail-data/by-day/2011-12-05.csv", "/Volumes/sample_catalog_v1/sample_schema_v1/sample_volumn_v1/auto_loader_dir/input_data/retail_daywise/2011/12/05/")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # List the contents of the input directory to verify that the files have been copied.
-- MAGIC dbutils.fs.ls("/Volumes/sample_catalog_v1/sample_schema_v1/sample_volumn_v1/auto_loader_dir/input_data/retail_daywise/2011/12/")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Import necessary functions from PySpark for streaming and timestamp operations.
-- MAGIC from pyspark.sql.functions import col, current_timestamp
-- MAGIC
-- MAGIC # Configure Auto Loader to read the CSV files and stream them into a Delta table with the necessary options.
-- MAGIC (spark.readStream
-- MAGIC   .format("cloudFiles")
-- MAGIC   .option("cloudFiles.format", "csv")
-- MAGIC   .option("cloudFiles.schemaLocation", "/Volumes/sample_catalog_v1/sample_schema_v1/sample_volumn_v1/auto_loader_dir/checkpoint/autoloader/01/")
-- MAGIC   .load("/Volumes/sample_catalog_v1/sample_schema_v1/sample_volumn_v1/auto_loader_dir/input_data/retail_daywise/*/*/")
-- MAGIC   .select("*", col("_metadata.file_name").alias("source_filename"), current_timestamp().alias("processing_time"))
-- MAGIC   .writeStream
-- MAGIC   .option("checkpointLocation", "/Volumes/sample_catalog_v1/sample_schema_v1/sample_volumn_v1/auto_loader_dir/checkpoint/autoloader/01/")
-- MAGIC   .trigger(availableNow=True)
-- MAGIC   .toTable("sample_catalog_v1.sample_schema_v1.auto_loader_retail_daywise_table"))

-- COMMAND ----------

-- Perform a simple SQL query to retrieve all the data from the Delta table.
SELECT * FROM sample_catalog_v1.sample_schema_v1.auto_loader_retail_daywise_table

-- COMMAND ----------

-- Run a SQL query to count the number of records from each source file ingested into the Delta table.
SELECT
  `source_filename`,  -- Display the file name from which the data was ingested.
  COUNT(*) AS COUNT_OF_RECORDS  -- Count the number of records from each file.
FROM
  sample_catalog_v1.sample_schema_v1.auto_loader_retail_daywise_table  -- Query the Delta table.
GROUP BY
  `source_filename`;  -- Group by the file name.

-- COMMAND ----------

-- CREATE OR REFRESH STREAMING TABLE dlt_streaming_auto_loader_retail_daywise_table
-- TBLPROPERTIES (
--   -- 'pipelines.trigger.once' = 'true' -- Trigger.Once: Process all data once and stop. Used for batch-like processing.
--   -- 'pipelines.trigger.interval' = '5 minutes' -- Trigger.ProcessingTime: Process data at fixed intervals (e.g., every 5 minutes).
--   -- 'pipelines.trigger.continuous' = '1 second' -- Trigger.Continuous: Continuously process incoming data with low latency (low throughput).
--   'pipelines.trigger.availableNow' = 'true' -- Trigger.AvailableNow: Process all available data in the source and stop. Suitable for batch-like but efficient processing.
-- )
-- COMMENT "Streaming Delta table loading using Auto Loader"
-- AS SELECT
--   *,
--   _metadata.file_name as _FILE_PATH
-- FROM
--   STREAM cloud_files(
--     '/Volumes/sample_catalog_v1/sample_schema_v1/sample_volumn_v1/auto_loader_dir/input_data/retail_daywise/*/*/',
--     'csv',
--     map(
--       'cloudFiles.format', 'csv',
--       'cloudFiles.inferColumnTypes', 'true',
--       'pathGlobFilter', '*.csv',
--       'header', 'true',
--       -- 'cloudFiles.useNotifications', 'true' -- Auto Loader sets up a notification service and queue service that subscribes to file events from the input directory. By default it is Directory Listing Mode.
--       -- Error handling techniques in Auto Loader for the files where the schema changes.
--       -- 'mergeSchema', 'true', -- Include the changes and process the data.
--       -- 'cloudFiles.schemaEvaluationMode', 'rescue' -- Create an additional column '_rescued_data' and save the schema changed data.
--       -- 'cloudFiles.schemaEvaluationMode', 'None' -- This option will ignore the new column and only presists the old columns.
--       'cloudFiles.schemaLocation', '/Volumes/sample_catalog_v1/sample_schema_v1/sample_volumn_v1/auto_loader_dir/checkpoint/autoloader/01/',
--       'checkpointLocation', '/Volumes/sample_catalog_v1/sample_schema_v1/sample_volumn_v1/auto_loader_dir/checkpoint/autoloader/01/'
--     )
--   )

-- COMMAND ----------

-- SELECT
--   `_FILE_PATH`,
--   COUNT(*) AS COUNT_OF_RECORDS
-- FROM
--   sample_catalog_v1.sample_schema_v1.dlt_streaming_auto_loader_retail_daywise_table
-- GROUP BY
--   `_FILE_PATH`;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Cleanup step: Remove the directories and all their contents from DBFS to free up space.
-- MAGIC dbutils.fs.rm('/Volumes/sample_catalog_v1/sample_schema_v1/sample_volumn_v1/auto_loader_dir/', recurse = True)