# Databricks notebook source
# MAGIC %md The Databricks notebook demonstrates the process of importing and managing retail data using both PySpark and SQL. The notebook guides users through creating a directory, copying CSV files into it, and loading the data into a table using the COPY INTO command. Key operations such as reading data into a Spark DataFrame, performing basic analysis, creating a table schema, and executing SQL queries to view and manage the data are covered. This hands-on approach helps users understand how to efficiently work with data stored in Databricks and perform data loading operations in a scalable and structured manner.

# COMMAND ----------

# Create a directory where we will store the files to be copied into the table.
dbutils.fs.mkdirs("/Volumes/sample_catalog_v1/sample_schema_v1/sample_volumn_v1/copy_into_dir/")

# COMMAND ----------

# List the contents of the source directory where the retail data files are stored.
dbutils.fs.ls("/databricks-datasets/definitive-guide/data/retail-data/by-day/")

# COMMAND ----------

# Copy the first CSV file (2011-12-01.csv) from the source to our destination directory.
dbutils.fs.cp("/databricks-datasets/definitive-guide/data/retail-data/by-day/2011-12-01.csv", "/Volumes/sample_catalog_v1/sample_schema_v1/sample_volumn_v1/copy_into_dir/")

# Copy the second CSV file (2011-12-02.csv) from the source to our destination directory.
dbutils.fs.cp("/databricks-datasets/definitive-guide/data/retail-data/by-day/2011-12-02.csv", "/Volumes/sample_catalog_v1/sample_schema_v1/sample_volumn_v1/copy_into_dir/")

# Copy the third CSV file (2011-12-05.csv) from the source to our destination directory.
dbutils.fs.cp("/databricks-datasets/definitive-guide/data/retail-data/by-day/2011-12-05.csv", "/Volumes/sample_catalog_v1/sample_schema_v1/sample_volumn_v1/copy_into_dir/")

# COMMAND ----------

# dbutils.fs.cp("/databricks-datasets/definitive-guide/data/retail-data/by-day/2011-12-06.csv", "/Volumes/sample_catalog_v1/sample_schema_v1/sample_volumn_v1/copy_into_dir/")

# COMMAND ----------

# List the contents of our destination directory to verify the files have been copied.
dbutils.fs.ls("/Volumes/sample_catalog_v1/sample_schema_v1/sample_volumn_v1/copy_into_dir/")

# COMMAND ----------

# Read the data into a Spark DataFrame from the file for 2011-12-06.csv (make sure the file exists before running this).
retail_daywise_df = spark.read.format("csv").option("header", True).option('inferschema', True).load("/Volumes/sample_catalog_v1/sample_schema_v1/sample_volumn_v1/copy_into_dir/2011-12-06.csv")

# COMMAND ----------

# Display the count of rows in the DataFrame to understand the size of the data.
retail_daywise_df.count()

# COMMAND ----------

# Display the data in the DataFrame for visual inspection.
retail_daywise_df.display()

# COMMAND ----------

# Show the data types of the columns in the DataFrame to understand its structure.
retail_daywise_df.dtypes

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Create a schema for the table in SQL. This defines the database and schema where the table will reside.
# MAGIC CREATE SCHEMA cat_demo_001.sample_schema_v1

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Create the table to hold the data, with appropriate columns and data types for each.
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS cat_demo_001.sample_schema_v1.copy_into_retail_daywise_table (
# MAGIC   InvoiceNo STRING,
# MAGIC   StockCode STRING,
# MAGIC   Description STRING,
# MAGIC   Quantity INT,
# MAGIC   InvoiceDate TIMESTAMP,
# MAGIC   UnitPrice DOUBLE,
# MAGIC   CustomerID DOUBLE,
# MAGIC   Country STRING
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Perform a basic SQL query to check if the table has been created successfully.
# MAGIC SELECT * FROM cat_demo_001.sample_schema_v1.copy_into_retail_daywise_table

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Describe the table to get more detailed metadata, including its schema and partitions.
# MAGIC DESCRIBE EXTENDED sample_catalog_v1.sample_schema_v1.copy_into_retail_daywise_table

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Now we use COPY INTO to load data into the table from the files in the destination directory.
# MAGIC
# MAGIC COPY INTO sample_catalog_v1.sample_schema_v1.copy_into_retail_daywise_table
# MAGIC FROM
# MAGIC   "/Volumes/sample_catalog_v1/sample_schema_v1/sample_volumn_v1/copy_into_dir" FILEFORMAT = CSV PATTERN = '*.csv' FORMAT_OPTIONS (
# MAGIC     'mergeSchema' = 'true',
# MAGIC     # Automatically merge the schema of new data with the existing table
# MAGIC     'header' = 'true',
# MAGIC     # Ensure the first row in the CSV is used as column headers
# MAGIC     'inferSchema' = 'true' # Automatically infer the schema based on the CSV content
# MAGIC   ) COPY_OPTIONS (
# MAGIC     'mergeSchema' = 'true' # Allow merging schemas if there are new or missing columns
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Run a simple SQL query to view the data that has been loaded into the table.
# MAGIC SELECT * FROM sample_catalog_v1.sample_schema_v1.copy_into_retail_daywise_table;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Count the number of rows in the table to verify the data load was successful.
# MAGIC SELECT count(*) FROM sample_catalog_v1.sample_schema_v1.copy_into_retail_daywise_table;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Drop the table after the data load is complete, if we no longer need it.
# MAGIC DROP TABLE sample_catalog_v1.sample_schema_v1.copy_into_retail_daywise_table

# COMMAND ----------

# Remove the directory and its contents, cleaning up after the operation.

dbutils.fs.rm('/Volumes/sample_catalog_v1/sample_schema_v1/sample_volumn_v1/copy_into_dir/', recurse = True)