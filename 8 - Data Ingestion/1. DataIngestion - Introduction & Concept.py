# Databricks notebook source
# MAGIC %md # Data Ingestion Concepts in Azure Databricks
# MAGIC 1. Add Data from UI - New File - Databricks provides an option to manually upload small datasets through the UI. This is useful for quick testing, validation, and exploratory analysis before implementing automated ingestion pipelines. Users can upload CSV, JSON, Parquet, and other file formats, which can then be accessed using Databricks File System (DBFS) or Unity Catalog.
# MAGIC
# MAGIC 2. Try Append File - Appending data refers to adding new records to an existing dataset without modifying or deleting existing records. This is commonly used in scenarios where incoming data is incrementally loaded over time. Ensuring schema consistency is crucial while appending data to avoid conflicts.
# MAGIC
# MAGIC 3. Try Schema Change - Schema changes occur when a dataset structure evolves, such as adding new columns, changing data types, or renaming fields. Databricks supports schema evolution, allowing automatic adaptation to changes, particularly in Delta tables. However, strict schema enforcement may be required to prevent unintended modifications.
# MAGIC
# MAGIC 4. Duplicate Data (Try Merge) - Duplicate data issues arise when multiple copies of the same record exist, leading to data inconsistency. Databricks provides MERGE functionality to handle duplicates by upserting data based on predefined conditions. This ensures that only new or modified records are added while avoiding duplication.
# MAGIC
# MAGIC 5. Data Ingestion with PySpark - PySpark, the Python API for Apache Spark, enables scalable data ingestion by reading structured and semi-structured data from various sources. It supports multiple formats such as CSV, JSON, Parquet, and Delta, and provides transformations to clean and process data before storing it in tables or data lakes.
# MAGIC
# MAGIC 6. Data Ingestion with AutoLoader - AutoLoader is a Databricks feature that allows incremental ingestion of files from cloud storage (Azure Blob, ADLS, S3). It automatically detects new files and efficiently loads them into Delta tables. AutoLoader supports schema inference and evolution, making it suitable for handling continuously arriving data.
# MAGIC
# MAGIC 7. Data Ingestion with AutoLoader with Delta Live Tables - Delta Live Tables (DLT) is a framework that automates data pipeline creation and management. When combined with AutoLoader, it ensures real-time, reliable data ingestion with built-in monitoring and lineage tracking. This is beneficial for maintaining data freshness in analytics and machine learning workloads.
# MAGIC
# MAGIC 8. COPY INTO - COPY INTO is a SQL-based command in Databricks used for efficient batch ingestion from cloud storage into Delta tables. It simplifies the ingestion process by automatically handling file discovery, schema inference, and incremental data loading while ensuring data consistency.
# MAGIC
# MAGIC 9. DELTA LIVE TABLES - Delta Live Tables (DLT) provide a declarative approach to building ETL pipelines with reliability, data quality enforcement, and simplified management. They allow defining transformations on streaming or batch data sources, automatically handling dependencies and optimizing execution.
# MAGIC   - STREAMING TABLES - Streaming tables enable real-time data ingestion and processing. Databricks supports structured streaming, which allows continuous data ingestion from sources like Kafka, Event Hubs, or cloud storage. Streaming tables automatically update as new data arrives, making them suitable for real-time analytics and monitoring applications.
# MAGIC   - MATERIALIZED VIEWS - Materialized views store the results of a query as a physical table, which can be periodically refreshed. They improve query performance by avoiding repetitive computations, making them useful for aggregations and precomputed analytics. In Databricks, materialized views can be used to optimize performance in data lakes and Delta tables.

# COMMAND ----------

# MAGIC %md ## 1. Add Data from UI - New File

# COMMAND ----------

# MAGIC %md You can upload a new file manually using the Databricks UI:
# MAGIC - Go to `Data` -> `Create Table` -> `Upload File`
# MAGIC - Select the file and destination location.
# MAGIC
# MAGIC For automation, we use `dbutils.fs.cp`.

# COMMAND ----------

# Example: Copy file to DBFS (Databricks File System)
source_path = "https://example.com/sample.csv"
destination_path = "/mnt/data/sample.csv"

dbutils.fs.cp(source_path, destination_path)  # Uncomment if running in Databricks

# COMMAND ----------

# MAGIC %md ## 2. Try Append File

# COMMAND ----------

from pyspark.sql.functions import lit

data = [(1, "Alice", 30), (2, "Bob", 25)]
df = spark.createDataFrame(data, ["id", "name", "age"])
df.write.mode("append").format("delta").save("/mnt/data/sample_table")

# COMMAND ----------

# MAGIC %md ## 3. Try Schema Change

# COMMAND ----------

new_data = [(3, "Charlie", 35, "USA")]
df_new = spark.createDataFrame(new_data, ["id", "name", "age", "country"])
df_new.write.mode("append").option("mergeSchema", "true").format("delta").save("/mnt/data/sample_table")

# COMMAND ----------

# MAGIC %md ## 4. Duplicate Data - Try Merge

# COMMAND ----------

from delta.tables import DeltaTable

delta_table = DeltaTable.forPath(spark, "/mnt/data/sample_table")
new_data = [(2, "Bob", 26)]
df_merge = spark.createDataFrame(new_data, ["id", "name", "age"])

delta_table.alias("old").merge(
    df_merge.alias("new"), "old.id = new.id"").whenMatchedUpdate(set={"age": "new.age"}").whenNotMatchedInsertAll().execute()

# COMMAND ----------

# MAGIC %md ## 5. Data Ingestion with PySpark

# COMMAND ----------

df_csv = spark.read.csv("/mnt/data/sample.csv", header=True, inferSchema=True)
df_csv.show()

# COMMAND ----------

# MAGIC %md ## 6. Data Ingestion with AutoLoader

# COMMAND ----------

raw_path = "/mnt/data/raw"

# COMMAND ----------

df_auto = spark.readStream.format("cloudFiles")\
    .option("cloudFiles.format", "csv")\
    .load(raw_path)

# COMMAND ----------

df_auto.display()

# COMMAND ----------

# MAGIC %md ## 7. Data Ingestion with AutoLoader and Delta Live Tables

# COMMAND ----------

def ingest_data():
    return spark.readStream.format("cloudFiles").option("cloudFiles.format", "json").load("/mnt/data/live")

# COMMAND ----------

ingest_data().display()

# COMMAND ----------

# MAGIC %md ## 8. COPY INTO

# COMMAND ----------

query = """
COPY INTO delta.`/mnt/data/sample_table`
FROM '/mnt/data/sample.csv'
FILEFORMAT = CSV
HEADER = TRUE"""

# COMMAND ----------

spark.sql(query)

# COMMAND ----------

# MAGIC %md ## 9. DELTA LIVE TABLES

# COMMAND ----------

@dlt.table
def live_table():
    return spark.readStream.format("json").load("/mnt/data/live")

# COMMAND ----------

# MAGIC %md ### 9.1. STREAMING TABLES

# COMMAND ----------

df_stream = spark.readStream.format("delta").load("/mnt/data/streaming_table")

# COMMAND ----------

df_stream.writeStream.format("delta").option("checkpointLocation", "/mnt/checkpoints").start("/mnt/data/processed")

# COMMAND ----------

# MAGIC %md ### 9.2. MATERIALIZED VIEWS

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE MATERIALIZED VIEW sales_summary AS
# MAGIC SELECT
# MAGIC   product,
# MAGIC   SUM(price) AS total_sales
# MAGIC FROM
# MAGIC   sales
# MAGIC GROUP BY
# MAGIC   product