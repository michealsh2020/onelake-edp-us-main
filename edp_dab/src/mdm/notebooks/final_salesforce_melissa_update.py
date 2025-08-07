# Databricks notebook source
%pip install pyyaml

# COMMAND ----------

import pandas as pd
import json
import urllib.parse
import urllib.request
from delta.tables import DeltaTable
from pyspark.sql.functions import col, current_date
from datetime import datetime
import requests
import logging
import yaml

# COMMAND ----------

dbutils.widgets.text("catalog", "", "catalog")
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

# MAGIC %run "../functions/udf_read_config"

# COMMAND ----------

# MAGIC %run "../functions/udf_setup_logging"

# COMMAND ----------

# MAGIC %run "../functions/udf_log_clean_operation_metrics"

# COMMAND ----------

# MAGIC %run "../functions/udf_send_email"

# COMMAND ----------

# directory and file path for creating and storing the logs
log_directory = f"/Volumes/{catalog}/enriched/enriched_logs/{datetime.now().strftime('%Y%m%d')}"
log_file_path = f"{log_directory}/sf_account_validated_address_{datetime.now().strftime('%Y%m%d%H%M%S')}.log"
# creating a logger to log the execution and errors
logger = udf_setup_logging(log_directory, log_file_path)

# COMMAND ----------

# Load source tables
df1 = spark.read.table(f"{catalog}.clean.sf_account").filter(
    (col("record_current_flag") == "1")) 
#df2 = spark.read.table(f"{catalog}.clean.melissa_api_final")

df2= spark.read.table(f"{catalog}.clean.melissa_api_final").filter(
    (col("flag_revisit_billing") == "0")  & (col("flag_revisit_shipping") == "0") )


# Join to get required columns from table2
df = df1.join(df2.select("sid_id", "validated_billing_street_clean", "validated_billing_city_clean", "validated_billing_state_clean", "validated_billing_postal_code_clean", "validated_billing_country_clean", "validated_shipping_street_clean", "validated_shipping_city_clean", "validated_shipping_state_clean", "validated_shipping_postal_code_clean", "validated_shipping_country_clean"), on="sid_id", how="left")

#display(df)

# Define target table name

target_table_name = f"{catalog}.enriched.sf_account_validated_address"

# Try to get the Delta table reference (Only execute merge if the table exists)
try:
    delta_target = DeltaTable.forName(spark, target_table_name)

    # Perform the merge (Insert or Update)
    delta_target.alias("tgt").merge(
        df .alias("src"),
        "tgt.sid_id = src.sid_id"  # Replace 'id' with the correct primary key column
    ).whenMatchedUpdate(
        set={col_name: col(f"src.{col_name}") for col_name in df.columns}  # Update all columns
    ).whenNotMatchedInsert(
        values={col_name: col(f"src.{col_name}") for col_name in df.columns}  # Insert new records
    ).execute()
    logger.info(f"Processing completed for {target_table_name}\n")
    udf_send_email('Account Data Refresh', 'salesforce address update success', '',catalog)

except Exception as e:
    logger.error(f"Error occurred while processing for {target_table_name} : {str(e)}")
    logging.shutdown()
    udf_send_email(f'Account Data Refresh','salesforce address update failure',{str(e)},catalog) 

# COMMAND ----------

logging.shutdown()
