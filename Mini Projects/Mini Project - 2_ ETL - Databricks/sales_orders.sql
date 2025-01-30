-- Databricks notebook source
-- MAGIC %python
-- MAGIC
-- MAGIC # 1. Define input widgets for user interaction in the Databricks notebook.
-- MAGIC dbutils.widgets.text("order_date", "1900-01-01")  # Widget to accept an order date (defaults to 1900-01-01)
-- MAGIC dbutils.widgets.text("table_name", "sales_orders")  # Widget to accept the table name (defaults to "sales_orders")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # 2. Get the values from the widgets for further use in the code.
-- MAGIC order_date_value = dbutils.widgets.get("order_date")  # Fetches the value entered in the 'order_date' widget
-- MAGIC table_name_value = dbutils.widgets.get("table_name")  # Fetches the value entered in the 'table_name' widget
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # 3. Print the values for debugging or logging purposes.
-- MAGIC print(order_date_value, table_name_value)  # Prints the fetched values to verify them

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # 4. PostgreSQL connection configuration (JDBC).
-- MAGIC jdbc_url = "jdbc:postgresql://<HOST IP ADDRESS>:5432/<DATABASE NAME>"  # JDBC URL with the host IP address and database name
-- MAGIC jdbc_properties = {
-- MAGIC     "user": "<USER NAME>",  # PostgreSQL username
-- MAGIC     "password": "<PASSWORD>",  # PostgreSQL password
-- MAGIC     "driver": "org.postgresql.Driver",  # PostgreSQL JDBC driver
-- MAGIC     "url": jdbc_url,  # JDBC URL for PostgreSQL
-- MAGIC }
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # 5. Query data from PostgreSQL and filter using the 'order_date_value'.
-- MAGIC sales_orders_df = spark.read.format('jdbc').options(**jdbc_properties).option("query",f"SELECT * FROM {table_name_value} where TO_DATE(order_date, 'yyyy-MM-dd') > TO_DATE('{order_date_value}', 'yyyy-MM-dd')").load()

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC # 7. Display the queried data in the notebook.
-- MAGIC sales_orders_df.display()  # Displays the queried data as a table in the notebook output

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # 8. Create a temporary view for the queried data.
-- MAGIC sales_orders_df.createOrReplaceTempView("temp_sales_orders_view")  # Creates or replaces a temporary view with the queried data

-- COMMAND ----------

-- SQL Code to create and query a temporary view.
-- The SQL below is commented out in Python and is for visualization or manual execution:

-- Read data from PostgreSQL into a temporary view
-- CREATE OR REPLACE TEMPORARY VIEW temp_sales_orders_view
-- USING org.apache.spark.sql.jdbc
-- OPTIONS (
--   url 'jdbc:postgresql://20.219.241.147:5432/postgres',
--   dbtable 'sales_orders',
--   user 'pratap',
--   password '1qaz!QAZ',
--   driver 'org.postgresql.Driver',
--   query "SELECT * FROM sales_orders where TO_DATE(order_date, 'yyyy-MM-dd') > TO_DATE('{{order_date_value}}', 'yyyy-MM-dd')"
-- );

-- COMMAND ----------

-- Display Temporary View.
SELECT * FROM temp_sales_orders_view;

-- COMMAND ----------

-- SQL Query to display the count of rows in the temporary view
SELECT count(*) FROM temp_sales_orders_view;

-- COMMAND ----------

-- select count(*) from sample_catalog_v1.sample_schema_v1.sales_orders_table

-- COMMAND ----------

-- SQL: Create a table in a sample schema if it doesn't already exist
CREATE TABLE IF NOT EXISTS sample_catalog_v1.sample_schema_v1.sales_orders_table (
    order_id STRING NOT NULL PRIMARY KEY,
    customer_id STRING NOT NULL,
    product_id STRING NOT NULL,
    quantity INT,
    order_date DATE,
    total_price DOUBLE
);

-- COMMAND ----------

-- -- Display the table.
-- SELECT * FROM sample_catalog_v1.sample_schema_v1.sales_orders_table;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # 9. Merge data from the temporary view into the existing table using a MERGE statement.
-- MAGIC spark.sql(f"""
-- MAGIC         MERGE INTO sample_catalog_v1.sample_schema_v1.sales_orders_table AS target
-- MAGIC         USING temp_sales_orders_view AS source
-- MAGIC         ON target.order_id = source.order_id AND target.product_id = source.product_id
-- MAGIC         WHEN MATCHED THEN
-- MAGIC         UPDATE SET
-- MAGIC             target.customer_id = source.customer_id,
-- MAGIC             target.product_id = source.product_id,
-- MAGIC             target.quantity = source.quantity,
-- MAGIC             target.total_price = source.total_price
-- MAGIC         WHEN NOT MATCHED THEN
-- MAGIC         INSERT (order_id, customer_id, product_id, quantity, order_date, total_price)
-- MAGIC         VALUES (source.order_id, source.customer_id, source.product_id, source.quantity, source.order_date, source.total_price);
-- MAGIC """)

-- COMMAND ----------

-- SQL Query to display the merged data from the final table.
SELECT * FROM sample_catalog_v1.sample_schema_v1.sales_orders_table;

-- COMMAND ----------

-- SQL Query to count the number of rows in the final table.
SELECT count(*) FROM sample_catalog_v1.sample_schema_v1.sales_orders_table;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # 10. Exit the notebook after successful execution.
-- MAGIC dbutils.notebook.exit("Success")  # Exit the Databricks notebook with a success message