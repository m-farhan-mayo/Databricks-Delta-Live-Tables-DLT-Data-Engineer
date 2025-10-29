# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## DLT Pipeline

# COMMAND ----------

# MAGIC %md
# MAGIC #### Bronze - Streaming Table

# COMMAND ----------

import dlt

@dlt.table

def bronze_customer_mat():
  df = spark.read.table("farhan_catalog.raw.raw_customer")


  return df

# COMMAND ----------

# MAGIC %md
# MAGIC #### Silver - View 

# COMMAND ----------

@dlt.table

def silver_customer_stream():
  df = spark.readStream.table("LIVE.bronze_customer_mat")

  df = df.withColumn("Flag", lit("New"))

  return df

# COMMAND ----------

# MAGIC %md
# MAGIC #### Gold - Mat View

# COMMAND ----------

@dlt.table

def gold_customer():

  df = spark.read.table("LIVE.silver_customer_stream")

  #df = df.groupBy("id").agg(count("*").alias("max"))

  return df