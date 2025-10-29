# Databricks-Delta-Live-Tables-DLT-Data-Engineer


This project is a practical demonstration of building and managing data pipelines using Delta Live Tables (DLT) in Databricks. It focuses on processing Customer data and showcases two of the most common and important pipeline patterns:

Append-Only Flow: A simple, streaming-style pipeline where new data is just added.

SCD (Slowly Changing Dimension) Flow: An advanced, powerful pattern for handling updates to existing records (like a customer changing their address) without losing history or creating duplicates.

File Breakdown (Project Recall)
Here is what each Python script does in logical order:

1._Source

Purpose: This script defines the starting point of the pipeline. It contains the code to read the raw source data (e.g., from a cloud storage path like ADLS or S3) and creates the initial "Bronze" or "landing" DLT table.

2._Creating_DLT_Table

Purpose: This file likely contains the initial setup for the "Silver" layer table. It reads from the Source (Bronze) table, applies basic cleaning and schema, and creates the base customer table that the other pipelines will load into.

3._Append_Flow_Customer_DLT_Pipeline

Purpose: This script demonstrates the append-only DLT pattern. It's a pipeline that takes source data and simply inserts all new records into the target customer table. This is used for data that never changes, like event logs or initial bulk loads.

4._SCD_Customer_DLT_Pipeline

Purpose: This is the most advanced and important script. It demonstrates how to handle Slowly Changing Dimensions. This pipeline uses the DLT apply_changes() function to intelligently manage changes:

It inserts new customers.

It updates existing customers (e.g., SCD Type 1, where the old record is overwritten).

It merges this data into the final customer table, ensuring data quality and handling updates and new records correctly.
