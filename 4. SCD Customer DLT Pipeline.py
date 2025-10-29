# Databricks notebook source
# MAGIC %md
# MAGIC ## SLOWLY CHANGING DIMENSIONS

# COMMAND ----------

# MAGIC %md
# MAGIC ### SIlVER - STREAMING VIEW

# COMMAND ----------

@dlt.table

def silver_customer():
  df = spark.readStream.table("farhan_catalog.raw.raw_customer_dimm")
  return df

# COMMAND ----------

# MAGIC %md
# MAGIC ### GOLD - SCD TYPE 1

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

import dlt

# COMMAND ----------

dlt.create_streaming_table("dim_customer_type1")

# COMMAND ----------

dlt.apply_changes(
    target = 'dim_customer_type1',
    source = 'silver_customer',
    keys = ['id'],
    sequence_by = col('date'),
    except_column_list = ['date' , 'action'],
    apply_as_deletes = expr("action == 'd'"),
    stored_as_scd_type = 1
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## GOLD - SCD TYPE 2

# COMMAND ----------

dlt.create_streaming_table("dim_customer_type2")

# COMMAND ----------

dlt.apply_changes(
    target = 'dim_customer_type2',
    source = 'silver_customer',
    keys = ['id'],
    sequence_by = col('date'),
    except_column_list = ['date' , 'action'],
    apply_as_deletes = expr("action == 'd'"),
    stored_as_scd_type = 2
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM farhan_catalog.dim_schema.dim_customer_type1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM farhan_catalog.dim_schema.dim_customer_type2