# Databricks notebook source
# MAGIC %md
# MAGIC ## Set up widgets

# COMMAND ----------

dbutils.widgets.text("source_catalog", "", "Catalog")
source_catalog = dbutils.widgets.get("source_catalog")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Call email function

# COMMAND ----------

# MAGIC %run "../functions/udf_send_email"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import libraries

# COMMAND ----------

import pandas as pd
import numpy as np
 
import time
import uuid
 
from ipywidgets import widgets, interact
 
import pyspark.sql.functions as fn

# COMMAND ----------

# MAGIC %md
# MAGIC ## Merge -1: sf_account_clean_address_reviewed Table

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import current_date, to_date

try:
    # Convert target table to Delta Table
    delta_target = DeltaTable.forName(spark, f'{source_catalog}.clean.sf_account_clean_address_reviewed')
    
    source_df = spark.read.table(f'{source_catalog}.tmp.sf_account_clean_address_for_review').filter(col('review_status') == 'Reviewed')    
    target_df = DeltaTable.forName(spark, f'{source_catalog}.clean.sf_account_clean_address_reviewed')

    source_count = source_df.count()

    # Get the columns of the target Delta table
    target_columns = target_df.toDF().columns
    
    # Perform Merge Operation on Delta table
    target_df.alias("tgt").merge(
        source_df.alias("src"),
        "tgt.sid_id = src.sid_id AND tgt.hash_key = src.hash_key AND tgt.needs_manual_review =src.needs_manual_review"
    ).whenMatchedUpdate(
        set={col: f"src.{col}" for col in source_df.columns if col in target_columns}  # Replace only columns that exist in target table
    ).whenNotMatchedInsert(
        values={col: f"src.{col}" for col in source_df.columns if col in target_columns}  # Insert only columns that exist in target table
    ).execute()

    target_count = target_df.toDF().filter((col('review_status') == 'Reviewed') & (to_date(col('reviewed_date')) == current_date())).count()
    print(f"Source Count: {source_count}, Target Count: {target_count}")
except Exception as e:
    print(str(e))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Merge -2: sf_account_clean_address Table

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import col

try:
    # Convert target table to Delta Table
    source_df = spark.read.table(f'{source_catalog}.tmp.sf_account_clean_address_for_review').filter((col('review_status') == 'Reviewed') & (col('reviewed_by') != 'Salesforce'))
    #display(source_df)
    source_count = source_df.count()
    target_dt = DeltaTable.forName(spark, f'{source_catalog}.clean.sf_account_clean_address')
    #display(source_df)

    target_table = f'{source_catalog}.clean.sf_account_clean_address'
    
    # Get the columns of the target Delta table
    target_columns = target_dt.toDF().columns
    
    # Perform Merge Operation on Delta table
    target_dt.alias("tgt").merge(
        source_df.alias("src"),
        "tgt.sid_id = src.sid_id"
    ).whenMatchedUpdate(
        condition="tgt.hash_key != src.hash_key",  # Update only if hash_key differs 
        set={col: f"src.{col}" for col in source_df.columns if col in target_columns}  # Replace only columns that exist in target table
    ).whenNotMatchedInsert(
        values={col: f"src.{col}" for col in source_df.columns if col in target_columns}  # Insert only columns that exist in target table
    ).execute()

    #target_df = target_dt.toDF()
    #target_count = target_df.filter(col('review_status') == 'Reviewed').count()
    #print(f"Source Count: {source_count}, Target Count: {target_count}")
    
    if(source_count > 0):
        udf_send_email('Account Data Refresh','merge success','',source_catalog,source_count,target_table)
        print("Merge Success")
    else:
        print("No records to merge")

except Exception as e:
    print(str(e))
    udf_send_email('Account Data Refresh','merge failure',{str(e)},source_catalog)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delete Reviewed Records

# COMMAND ----------

# check the count and then delete
# include other status also
try:
    source_df = spark.read.table(f'{source_catalog}.tmp.sf_account_clean_address_for_review').filter((col('review_status') == 'Reviewed') & (col('reviewed_by') != 'Salesforce'))
    source_count = source_df.count()
    display(source_count)

    target_df = DeltaTable.forName(spark, f'{source_catalog}.clean.sf_account_clean_address').history(2)
    operation_metrics = target_df.select("operationMetrics").filter(col('operation')=="MERGE").orderBy(col('version').desc()).limit(1).collect()[0][0] 
    display(operation_metrics)
    num_inserted = int(operation_metrics.get("numTargetRowsInserted") or 0)
    num_updated = int(operation_metrics.get("numTargetRowsUpdated") or 0)
    num_total = num_inserted + num_updated

    display(num_total)

    if(source_count == num_total):
        delete_query = f"""delete from {source_catalog}.tmp.sf_account_clean_address_for_review where review_status = 'Reviewed'"""
        spark.sql(delete_query)
        delete_sf_query = f"""delete from {source_catalog}.tmp.sf_account_clean_address_for_review where reviewed_by = 'Salesforce'"""
        spark.sql(delete_sf_query)
    elif(source_count != num_total):
        print("Source count and target count do not match")

except Exception as e:
    print(str(e))
    udf_send_email('Account Data Refresh','merge failure',{str(e)},source_catalog)
