-- Databricks notebook source
-- MAGIC %md ## Create and Manage Views in Databricks
-- MAGIC 1. **Temporary Views**
-- MAGIC 2. **Global Temporary Views**
-- MAGIC 3. **Permanent Views**
-- MAGIC 4. **Modifying and Dropping Views**
-- MAGIC 5. **Using Views for Security and Abstraction**

-- COMMAND ----------

-- MAGIC %md ## Introduction
-- MAGIC - Views in Databricks SQL provide a way to store SQL queries as virtual tables.
-- MAGIC - They help simplify complex queries and enforce security by restricting access to underlying tables.
-- MAGIC - There are three main types of views: Global Views, Temporary Views, and Permanent Views.

-- COMMAND ----------

-- MAGIC %md ## 1. Temporary Views
-- MAGIC - Temporary views exist only during the session and are not stored permanently.

-- COMMAND ----------

-- Creating a temporary view from a query result
CREATE TEMP VIEW temp_student AS
SELECT * FROM qbexcatalog.qbexschema.student;

-- COMMAND ----------

-- Querying the temporary view
SELECT * FROM temp_student;

-- COMMAND ----------

-- MAGIC %md Temporary views are session-scoped; they disappear after the session ends.

-- COMMAND ----------

-- MAGIC %md ## 2. Global Temporary Views
-- MAGIC - Global temporary views persist across sessions but are not permanent.
-- MAGIC - They are stored in the "global_temp" schema.

-- COMMAND ----------

-- Creating a global temporary view
CREATE GLOBAL TEMP VIEW global_student AS
SELECT * FROM qbexcatalog.qbexschema.student;

-- COMMAND ----------

-- Querying the global temporary view (must use "global_temp" schema)
SELECT * FROM global_temp.global_student;

-- COMMAND ----------

-- MAGIC %md Global views persist across sessions but are removed when the cluster restarts.

-- COMMAND ----------

-- MAGIC %md ## 3. Permanent Views
-- MAGIC - Permanent views persist in the catalog and can be referenced like regular tables.

-- COMMAND ----------

-- Creating a permanent view
CREATE VIEW qbexcatalog.qbexschema.student_view AS
SELECT id, name FROM qbexcatalog.qbexschema.student;

-- COMMAND ----------

-- Querying the permanent view
SELECT * FROM qbexcatalog.qbexschema.student_view;

-- COMMAND ----------

-- MAGIC %md Permanent views exist until explicitly dropped.

-- COMMAND ----------

-- MAGIC %md ## 4. Modifying and Dropping Views

-- COMMAND ----------

-- Replacing an existing view
CREATE OR REPLACE VIEW qbexcatalog.qbexschema.student_view AS
SELECT id, name, age FROM qbexcatalog.qbexschema.student;

-- COMMAND ----------

-- Dropping a view
DROP VIEW IF EXISTS qbexcatalog.qbexschema.student_view;

-- COMMAND ----------

-- MAGIC %md ## 5. Using Views for Security and Abstraction

-- COMMAND ----------

-- Creating a view that hides sensitive columns
CREATE VIEW qbexcatalog.qbexschema.secure_student AS
SELECT id, name FROM qbexcatalog.qbexschema.student;

-- COMMAND ----------

-- MAGIC %md The "secure_student" view prevents direct access to the age column.

-- COMMAND ----------

-- MAGIC %md ## Summary
-- MAGIC - Temporary Views: Exist only during the session.
-- MAGIC - Global Temporary Views: Persist across sessions but are removed after cluster restarts.
-- MAGIC - Permanent Views: Persist in the catalog until explicitly dropped.
-- MAGIC - Views can simplify queries and restrict access to sensitive data.