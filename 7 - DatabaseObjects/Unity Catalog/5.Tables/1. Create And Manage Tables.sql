-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Create and Manage Tables in Databricks
-- MAGIC Databricks supports multiple types of tables to handle different use cases efficiently. Below, we explore:
-- MAGIC
-- MAGIC 1. **Managed Tables**
-- MAGIC 2. **External Tables**
-- MAGIC 3. **Delta Tables**
-- MAGIC 4. **Streaming Tables**
-- MAGIC 5. **Foreign Tables**
-- MAGIC 6. **Feature Tables**
-- MAGIC 7. **Hive Tables (Legacy)**
-- MAGIC 8. **Live Tables (Deprecated)**

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ```sql
-- MAGIC { { [CREATE OR] REPLACE TABLE | CREATE [EXTERNAL] TABLE [ IF NOT EXISTS ] }
-- MAGIC   table_name
-- MAGIC   [ table_specification ]
-- MAGIC   [ USING data_source ]
-- MAGIC   [ table_clauses ]
-- MAGIC   [ AS query ] }
-- MAGIC
-- MAGIC table_specification
-- MAGIC   ( { column_identifier column_type [ column_properties ] } [, ...]
-- MAGIC     [ , table_constraint ] [...] )
-- MAGIC
-- MAGIC column_properties
-- MAGIC   { NOT NULL |
-- MAGIC     GENERATED ALWAYS AS ( expr ) |
-- MAGIC     GENERATED { ALWAYS | BY DEFAULT } AS IDENTITY [ ( [ START WITH start ] [ INCREMENT BY step ] ) ] |
-- MAGIC     DEFAULT default_expression |
-- MAGIC     COMMENT column_comment |
-- MAGIC     column_constraint |
-- MAGIC     MASK clause } [ ... ]
-- MAGIC
-- MAGIC table_clauses
-- MAGIC   { OPTIONS clause |
-- MAGIC     PARTITIONED BY clause |
-- MAGIC     CLUSTER BY clause |
-- MAGIC     clustered_by_clause |
-- MAGIC     LOCATION path [ WITH ( CREDENTIAL credential_name ) ] |
-- MAGIC     COMMENT table_comment |
-- MAGIC     TBLPROPERTIES clause |
-- MAGIC     WITH { ROW FILTER clause } } [...]
-- MAGIC
-- MAGIC clustered_by_clause
-- MAGIC   { CLUSTERED BY ( cluster_column [, ...] )
-- MAGIC     [ SORTED BY ( { sort_column [ ASC | DESC ] } [, ...] ) ]
-- MAGIC     INTO num_buckets BUCKETS }
-- MAGIC