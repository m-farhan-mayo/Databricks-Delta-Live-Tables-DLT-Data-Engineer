# Databricks notebook source
# MAGIC %md
# MAGIC ## Parameters
# MAGIC

# COMMAND ----------

myvar = spark.conf.get("v_name")


myvar_list = myvar.split(",")

# COMMAND ----------

# MAGIC %md
# MAGIC ### APPEND FLOW

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze - Customer Streaming

# COMMAND ----------

@dlt.table


def bronze_customer():
  df = spark.readStream.table("farhan_catalog.raw.raw_customer")

  return df

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze - Customer Streaming

# COMMAND ----------

@dlt.table


def bronze_customer_new():
  df = spark.readStream.table("farhan_catalog.raw.raw_customer_new")

  return df

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver - UNION MAT VIEW

# COMMAND ----------

# @dlt.table

# def silver_customer_union():
#     df1 = spark.read.table("LIVE.bronze_customer")
#     df2 = spark.read.table("LIVE.bronze_customer_new")


#     df = df1.union(df2)


#     return df

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver - APPEND FLOW Streaming Table

# COMMAND ----------

import dlt

# COMMAND ----------

dlt.create_streaming_table("silver_appendflowtable")

# COMMAND ----------

@dlt.append_flow(
    target = "silver_appendflowtable"
)

def silver_cust():
    df = spark.readStream.table("LIVE.bronze_customer")

    return df

# COMMAND ----------

@dlt.append_flow(
    target = "silver_appendflowtable"
)

def silver_cust_new():
    df = spark.readStream.table("LIVE.bronze_customer_new")

    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold - MAT VIEW

# COMMAND ----------

for j,i in enumerate(myvar_list):
    
    @dlt.table(
        name = f"gold_customer_{j}"
     )
    def gold_customer_appendflow():
        df = spark.readStream.table("LIVE.silver_appendflowtable")
        df = df.filter(df.name == i)
        return df