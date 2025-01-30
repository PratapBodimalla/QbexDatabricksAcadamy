# Databricks notebook source
# Creating a widget that prompts the user to enter a file name
dbutils.widgets.text("file_name", "Please enter file name to read: ")

# COMMAND ----------

# Retrieving the value entered in the widget
file_name_value = dbutils.widgets.get("file_name")

# COMMAND ----------

# Printing the file name value entered by the user
print(file_name_value)

# COMMAND ----------

# Load the Parquet file into a DataFrame using the user-provided file name
sales_orders_df = spark.read.format('parquet').load(f"/Volumes/sample_catalog_v1/sample_schema_v1/sample_volumn_v1/sample_table_dir/{file_name_value}")

# COMMAND ----------

# Display the loaded DataFrame
sales_orders_df.display()

# COMMAND ----------

# Create a temporary view for SQL queries
sales_orders_df.createOrReplaceTempView("temp_sales_orders_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- -- Read data from PostgreSQL into a temporary view
# MAGIC -- CREATE OR REPLACE TEMPORARY VIEW temp_sales_orders_view
# MAGIC -- USING org.apache.spark.sql.jdbc
# MAGIC -- OPTIONS (
# MAGIC --   url 'jdbc:postgresql://<HOST IP ADDRESS>:5432/<DATABASE NAME>',
# MAGIC --   dbtable '<TABLE NAME>',
# MAGIC --   user '<USER NAME>',
# MAGIC --   password '<PASSWORD>',
# MAGIC --   driver 'org.postgresql.Driver'
# MAGIC --   query "SELECT * FROM sales_orders where TO_DATE(order_date, 'yyyy-MM-dd') > TO_DATE('{{order_date_value}}', 'yyyy-MM-dd')"
# MAGIC -- );

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Display the contents of the temporary view
# MAGIC SELECT * FROM temp_sales_orders_view;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- -- Display Temporary View.
# MAGIC -- SELECT count(*) FROM temp_sales_orders_view;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- select count(*) from sample_catalog_v1.sample_schema_v1.sales_orders_table

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Create the `sales_orders_table` if it doesn't already exist
# MAGIC CREATE TABLE IF NOT EXISTS sample_catalog_v1.sample_schema_v1.sales_orders_table (
# MAGIC     order_id STRING NOT NULL PRIMARY KEY,  -- Primary key for the orders
# MAGIC     customer_id STRING NOT NULL,  -- Customer identifier
# MAGIC     product_id STRING NOT NULL,  -- Product identifier
# MAGIC     quantity INT,  -- Number of units ordered
# MAGIC     order_date DATE,  -- Date when the order was placed
# MAGIC     total_price DOUBLE  -- Total price for the order
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- -- Display the table.
# MAGIC -- SELECT * FROM sample_catalog_v1.sample_schema_v1.sales_orders_table;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Use MERGE statement to update or insert data from the temporary view into the target table
# MAGIC MERGE INTO sample_catalog_v1.sample_schema_v1.sales_orders_table AS target
# MAGIC USING temp_sales_orders_view AS source
# MAGIC ON target.order_id = source.order_id AND target.order_date = source.order_date AND target.customer_id = source.customer_id AND target.product_id = source.product_id
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     target.customer_id = source.customer_id,
# MAGIC     target.product_id = source.product_id,
# MAGIC     target.quantity = source.quantity,
# MAGIC     target.total_price = source.total_price
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (order_id, customer_id, product_id, quantity, order_date, total_price)
# MAGIC   VALUES (source.order_id, source.customer_id, source.product_id, source.quantity, source.order_date, source.total_price);
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- -- Display the contents of the `sales_orders_table` after merge operation
# MAGIC -- SELECT * FROM sample_catalog_v1.sample_schema_v1.sales_orders_table;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- -- Display the table.
# MAGIC -- SELECT count(*) FROM sample_catalog_v1.sample_schema_v1.sales_orders_table;