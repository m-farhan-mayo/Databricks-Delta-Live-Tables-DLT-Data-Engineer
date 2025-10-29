# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Live Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA farhan_catalog.raw;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating Source
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE farhan_catalog.raw.raw_customer(
# MAGIC   id INT,
# MAGIC   name STRING,
# MAGIC   salary INT,
# MAGIC   email STRING
# MAGIC )
# MAGIC USING DELTA 
# MAGIC LOCATION 'abfss://raw@farhandl.dfs.core.windows.net/raw_customer'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE farhan_catalog.raw.raw_customer_new(
# MAGIC   id INT,
# MAGIC   name STRING,
# MAGIC   salary INT,
# MAGIC   email STRING
# MAGIC )
# MAGIC USING DELTA 
# MAGIC LOCATION 'abfss://raw@farhandl.dfs.core.windows.net/raw_customer_new'

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO farhan_catalog.raw.raw_customer_new
# MAGIC VALUES
# MAGIC (16, 'Paul', 125000, 'paul@example.com'),
# MAGIC (17, 'Quincy', 130000, 'quincy@example.com'),
# MAGIC (18, 'Rachel', 135000, 'rachel@example.com'),
# MAGIC (19, 'Steve', 140000, 'steve@example.com'),
# MAGIC (20, 'Tracy', 145000, 'tracy@example.com'),
# MAGIC (21, 'Uma', 150000, 'uma@example.com'),
# MAGIC (22, 'Victor', 155000, 'victor@example.com'),
# MAGIC (23, 'Wendy', 160000, 'wendy@example.com'),
# MAGIC (24, 'Xander', 165000, 'xander@example.com'),
# MAGIC (25, 'Yara', 170000, 'yara@example.com'),
# MAGIC (26, 'Zane', 175000, 'zane@example.com'),
# MAGIC (27, 'Amy', 180000, 'amy@example.com'),
# MAGIC (28, 'Brian', 185000, 'brian@example.com'),
# MAGIC (29, 'Cathy', 190000, 'cathy@example.com'),
# MAGIC (30, 'Derek', 195000, 'derek@example.com')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inserting Data

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO farhan_catalog.raw.raw_customer
# MAGIC VALUES
# MAGIC (1, 'Alice', 50000, 'alice@example.com'),
# MAGIC (2, 'Bob', 55000, 'bob@example.com'),
# MAGIC (3, 'Charlie', 60000, 'charlie@example.com'),
# MAGIC (4, 'David', 65000, 'david@example.com'),
# MAGIC (5, 'Eve', 70000, 'eve@example.com'),
# MAGIC (6, 'Frank', 75000, 'frank@example.com'),
# MAGIC (7, 'Grace', 80000, 'grace@example.com'),
# MAGIC (8, 'Hank', 85000, 'hank@example.com'),
# MAGIC (9, 'Ivy', 90000, 'ivy@example.com'),
# MAGIC (10, 'Jack', 95000, 'jack@example.com'),
# MAGIC (11, 'Kathy', 100000, 'kathy@example.com'),
# MAGIC (12, 'Leo', 105000, 'leo@example.com'),
# MAGIC (13, 'Mona', 110000, 'mona@example.com'),
# MAGIC (14, 'Nina', 115000, 'nina@example.com'),
# MAGIC (15, 'Oscar', 120000, 'oscar@example.com')
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM farhan_catalog.raw.raw_customer

# COMMAND ----------

# MAGIC %sql
# MAGIC Drop table farhan_catalog.raw.raw_customer_dim

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE farhan_catalog.raw.raw_customer_dimm(
# MAGIC   id INT,
# MAGIC   name STRING,
# MAGIC   salary INT,
# MAGIC   email STRING,
# MAGIC   action STRING,
# MAGIC   date DATE
# MAGIC )
# MAGIC USING DELTA 
# MAGIC LOCATION 'abfss://raw@farhandl.dfs.core.windows.net/raw_customer_dimm'

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO farhan_catalog.raw.raw_customer_dimm
# MAGIC VALUES
# MAGIC (1, 'John Doe', 50000, 'john.doe@example.com', "i" , '2025-01-15'),
# MAGIC (2, 'Jane Smith', 60000, 'jane.smith@example.com', "i" , '2025-02-20'),
# MAGIC (3, 'Alice Johnson', 55000, 'alice.johnson@example.com', "i" , '2025-03-10'),
# MAGIC (4, 'Bob Brown', 70000, 'bob.brown@example.com', "i" , '2025-04-25'),
# MAGIC (5, 'Charlie Davis', 65000, 'charlie.davis@example.com', "i" , '2025-05-30'),
# MAGIC (6, 'Diana Evans', 72000, 'diana.evans@example.com', "d" , '2025-06-15'),
# MAGIC (7, 'Evan Harris', 48000, 'evan.harris@example.com', "i" , '2025-07-20'),
# MAGIC (8, 'Fiona Clark', 53000, 'fiona.clark@example.com', "i" , '2025-08-05'),
# MAGIC (9, 'George Lewis', 59000, 'george.lewis@example.com', "i" , '2025-09-10'),
# MAGIC (10, 'Hannah Walker', 61000, 'hannah.walker@example.com', "i" , '2025-10-25'),
# MAGIC (11, 'Ian Hall', 68000, 'ian.hall@example.com', "d" , '2025-11-30'),
# MAGIC (12, 'Jack Young', 47000, 'jack.young@example.com', "i" , '2025-12-20'),
# MAGIC (13, 'Karen King', 75000, 'karen.king@example.com', "d" , '2025-01-20'),
# MAGIC (16, 'Larry Scott', 52000, 'larry.scott@example.com', "i" , '2025-02-25'),
# MAGIC (15, 'Farhan MaYo', 63000, 'mona.adams@example.com', "d" , '2025-03-30'),
# MAGIC (17, 'Farhab MaYo', 63000, 'mona.adams@example.com', "j" , '2025-01-30')
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM farhan_catalog.raw.raw_customer_dimm

# COMMAND ----------

