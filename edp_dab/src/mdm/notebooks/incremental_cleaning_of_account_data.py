# Databricks notebook source
# MAGIC %pip install pyyaml

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import SparkSession, SQLContext, DataFrame, Row
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType, TimestampType, IntegerType,DataType
from delta.tables import *
from datetime import datetime, date
from pyspark.sql.window import Window
from pyspark.sql.functions import col, concat, row_number
import re
import json
import os
import logging
import yaml

# COMMAND ----------

# MAGIC %run "../functions/udf_read_config"

# COMMAND ----------

# MAGIC %run "../functions/udf_setup_logging"

# COMMAND ----------

# MAGIC %run "../functions/cleaning_helper_functions"

# COMMAND ----------

# MAGIC %run "../functions/udf_log_clean_operation_metrics"

# COMMAND ----------

# MAGIC %run "../functions/udf_send_email"

# COMMAND ----------

# MAGIC %md ### Set up widgets

# COMMAND ----------

dbutils.widgets.text("source_catalog", "", "Catalog")
dbutils.widgets.text("Source_Schema", "", "Source Schema")
dbutils.widgets.text("Source_Table_Name", "", "Source Table Name")
dbutils.widgets.text("Target_Table_Name", "", "Target Table Name")

# COMMAND ----------

# MAGIC %md ##read the input parameters

# COMMAND ----------

source_catalog = dbutils.widgets.get("source_catalog")
source_schema = dbutils.widgets.get("Source_Schema")
source_table = dbutils.widgets.get("Source_Table_Name")
target_table = dbutils.widgets.get("Target_Table_Name")

# COMMAND ----------

source_table =f"{source_catalog}.{source_schema}.{source_table}"
target_table =f"{source_catalog}.{source_schema}.{target_table}"

# COMMAND ----------

# directory and file path for creating and storing the logs
log_directory = f"/Volumes/{source_catalog}/clean/clean_logs/{datetime.now().strftime('%Y%m%d')}"
log_file_path = f"{log_directory}/sf_account_clean_address{datetime.now().strftime('%Y%m%d%H%M%S')}.log"
# creating a logger to log the execution and errors
logger = udf_setup_logging(log_directory, log_file_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Audit Tracking

# COMMAND ----------

def audit_tracking_clean_address(insert_run_id, update_run_id, delete_run_id=None, clean_run_id=None, review_run_id=None, accounttype=None):
    source_table = f'{source_catalog}.clean.sf_account_clean_address'
    audit_table  = f'{source_catalog}.landing.audit_logs'
    start_time   = datetime.now()
    insert_run_id  =  insert_run_id
    update_run_id  =  update_run_id
    delete_run_id  =  delete_run_id
    clean_run_id   =  clean_run_id
    review_run_id  =  review_run_id

    if(accounttype == 'archived'):
        audit_data = [(source_table, "INSERT", start_time, None,"IN_PROGRESS","" ,insert_run_id,'clean_address_insert'),(source_table, "UPDATE", start_time, None,"IN_PROGRESS","" ,update_run_id,'clean_address_update'),(source_table, "DELETE", start_time, None,"IN_PROGRESS","" ,delete_run_id, 'clean_address_delete'),(source_table, "CLEAN", start_time, None,"IN_PROGRESS","" ,clean_run_id, 'clean_address_clean'),(source_table, "REVIEW", start_time, None,"IN_PROGRESS","" ,review_run_id, 'clean_address_review')]
    elif(accounttype == 'naddress'):
        audit_data = [(source_table, "INSERT", start_time, None,"IN_PROGRESS","" ,insert_run_id,'clean_naddress_insert'),(source_table, "UPDATE", start_time, None,"IN_PROGRESS","" ,update_run_id,'clean_naddress_update')]
    else:
        audit_data = [(source_table, "INSERT", start_time, None,"IN_PROGRESS","" ,insert_run_id,'clean_narchive_address_insert'),(source_table, "UPDATE", start_time, None,"IN_PROGRESS","" ,update_run_id,'clean_narchive_address_update')]   

    schema = StructType([
        StructField("table_name", StringType(), True),
        StructField("operation", StringType(), True),
        StructField("start_time", TimestampType(), True),
        StructField("end_time", TimestampType(), True),
        StructField("status", StringType(), True),
        StructField("error_message", StringType(), True),
        StructField("run_id", StringType(), True),
        StructField("task_name", StringType(), True)
    ])
   
    audit_df = spark.createDataFrame(audit_data, schema)
    #display(audit_df)
    audit_df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(audit_table)
    print(f"Audit record inserted for {source_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Identify new records

# COMMAND ----------

from pyspark.sql.functions import col, max as F_max, to_date, lit
from delta.tables import DeltaTable

# Step 1: Create DataFrames for source and target tables
target_df = spark.read.table(target_table)
max_last_ingested_timestamp = target_df.agg(F_max("ingested_timestamp")).collect()[0][0]
max_last_ingested_timestamp = datetime(1970, 1, 1) if max_last_ingested_timestamp is None else max_last_ingested_timestamp
max_last_ingested_timestamp = max_last_ingested_timestamp.strftime("%Y-%m-%d")
if(max_last_ingested_timestamp == date.today().strftime('%Y-%m-%d')):
    source_df = spark.read.table(source_table).filter(
    (to_date(col("ingested_timestamp")) >= to_date(lit(max_last_ingested_timestamp))) & 
    (col("record_current_flag") == 1))
else:
    source_df = spark.read.table(source_table).filter(
    (to_date(col("ingested_timestamp")) > to_date(lit(max_last_ingested_timestamp))) & 
    (col("record_current_flag") == 1))
# # Step 2: Identify new records (sid_id not in target table)
new_records_df = source_df.join(target_df, ['sid_id'], how='left_anti')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Identify Incremental Records

# COMMAND ----------

# Step 3: Identify updated records (sid_id exists but hash_key is different)
updated_records_df = source_df.alias('src').join(
    target_df.alias('tgt'),
    (F.col('src.sid_id') == F.col('tgt.sid_id')) & (F.col('src.hash_key') != F.col('tgt.hash_key')),
    how='inner'
).select('src.*')
# Combine the new and updated records
df_account_1 = new_records_df.union(updated_records_df)
display(df_account_1)

# COMMAND ----------

# Read h_df table
h_df = spark.read.table(f"{source_catalog}.{source_schema}.melissa_api_final")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Identify Incremental Records With Address Changes

# COMMAND ----------

# Records in df_account but not in h_df
df_account = df_account_1.alias('src').join(
    h_df.alias('tgt'),
    F.col('src.sid_id') == F.col('tgt.sid_id'),
    how='left_anti'  # Get records in df_account but NOT in h_df
)
# Records where sid_id matches, hash_key differs, final_clean_flag == 1, and addresses differ
df_account = df_account.union(
    updated_records_df.alias('src')   #changed from df_account_1 to updated_records_df
    .join(
        h_df.alias('tgt'),
        (F.col('src.sid_id') == F.col('tgt.sid_id')) &
        (F.col('src.hash_key') != F.col('tgt.hash_key')),
        how='inner'
    )
    .join(                      # Ensure target_df is joined before using final_clean_flag
        target_df.alias('tf'),
        F.col('src.sid_id') == F.col('tf.sid_id'),
        how='inner'
    )
    .filter(F.col('tf.final_clean_flag') == 1)  # Now Spark recognizes final_clean_flag
    .filter(  # Address comparison when final_clean_flag = 1
        (F.col('src.billing_street') != F.col('tf.billing_street')) |
        (F.col('src.billing_city') != F.col('tf.billing_city')) |
        (F.col('src.billing_state') != F.col('tf.billing_state')) |
        (F.col('src.billing_country') != F.col('tf.billing_country')) |
        (F.col('src.billing_postal_code') != F.col('tf.billing_postal_code')) |
        (F.col('src.shipping_street') != F.col('tf.shipping_street')) |
        (F.col('src.shipping_city') != F.col('tf.shipping_city')) |
        (F.col('src.shipping_state') != F.col('tf.shipping_state')) |
        (F.col('src.shipping_country') != F.col('tf.shipping_country')) |
        (F.col('src.shipping_postal_code') != F.col('tf.shipping_postal_code'))
    )
    .select('src.*')
)

#display(df_account)
display(df_account.count())

# COMMAND ----------

'''
address_matched_df = df_account_1.alias('src').join(
    h_df.alias('tgt'),
    (F.col('src.sid_id') == F.col('tgt.sid_id')) &
    (F.col('src.hash_key') != F.col('tgt.hash_key')),
    how='inner'
).join(
    target_df.alias('tf'),
    F.col('src.sid_id') == F.col('tf.sid_id'),
    how='inner'
).filter(
    F.col('tf.final_clean_flag') == 1
).filter(
    (F.col('src.billing_street') == F.col('tf.billing_street')) &
    (F.col('src.billing_city') == F.col('tf.billing_city')) &
    (F.col('src.billing_state') == F.col('tf.billing_state')) &
    (F.col('src.billing_country') == F.col('tf.billing_country')) &
    (F.col('src.billing_postal_code') == F.col('tf.billing_postal_code')) &
    (F.col('src.shipping_street') == F.col('tf.shipping_street')) &
    (F.col('src.shipping_city') == F.col('tf.shipping_city')) &
    (F.col('src.shipping_state') == F.col('tf.shipping_state')) &
    (F.col('src.shipping_country') == F.col('tf.shipping_country')) &
    (F.col('src.shipping_postal_code') == F.col('tf.shipping_postal_code'))
).select('src.*')
'''

# COMMAND ----------

# MAGIC %md
# MAGIC ## Non-Address match records check

# COMMAND ----------

naddress_matched_df = updated_records_df.alias('src').join(
    df_account.alias('tgt'),
    F.col('src.sid_id') == F.col('tgt.sid_id'),
    how='left'
) .select('src.*')   

naddress_matched_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Identify Incremental Records with Non-Address changes

# COMMAND ----------

# List of IDs to check from h_df
ids_to_check = [row['sid_id'] for row in h_df.select('sid_id').distinct().collect()]
# Columns to check
columns_to_check = [
    'sid_id', 'account_id', 'named_account', 'billing_geolocation', 'shipping_geolocation',
    'billing_address_complete', 'billing_address_validation_status_msg', 'billing_country_2char_code',
    'shipping_address_complete', 'shipping_address_validation_status_msg', 'shipping_country_2char_code',
    'master_record_id', 'type', 'billing_latitude', 'billing_longitude', 'billing_geocode_accuracy',
    'shipping_latitude', 'shipping_longitude', 'shipping_geocode_accuracy', 'phone', 'account_number',
    'website', 'archived', 'billing_county', 'shipping_county', 'billing_address_validation_status'
]

# Filter the DataFrames based on IDs to check
source_filtered_df = naddress_matched_df.filter(F.col('sid_id').isin(ids_to_check)).select(columns_to_check)
target_filtered_df = target_df.filter(F.col('sid_id').isin(ids_to_check)).select(columns_to_check)

# Join source and target DataFrames on 'sid_id'
diff_df = source_filtered_df.alias('src').join(
    target_filtered_df.alias('tgt'),
    'sid_id'
).where(
    F.expr(" OR ".join([
        f"(src.{col} IS NOT NULL AND tgt.{col} IS NOT NULL AND src.{col} != tgt.{col})"
        for col in columns_to_check if col != 'sid_id'
    ]))
).select('src.*')  # Select full row from source_df

# COMMAND ----------

filtered_naddress_matched_df = naddress_matched_df.filter(
    naddress_matched_df["sid_id"].isin([row["sid_id"] for row in diff_df.select("sid_id").distinct().collect()])
)
display(filtered_naddress_matched_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Merge-1 : Non-Address Incremental records to Target Table

# COMMAND ----------

from delta.tables import DeltaTable
import pyspark.sql.functions as F
from pyspark.sql.functions import col, unix_timestamp
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

naddress_insert_run_id  =  str(datetime.now().strftime('%Y%m%d%H%M')) + '-' + 'clean_naddress_insert'
naddress_update_run_id  =  str(datetime.now().strftime('%Y%m%d%H%M')) + '-' + 'clean_naddress_update'

try:
    source_audit_table = f'{source_catalog}.clean.sf_account_clean_address'
    dest_audit_table = f'{source_catalog}.landing.audit_logs'
    audit_df = spark.read.table(dest_audit_table)
    audit_tracking_clean_address(naddress_insert_run_id,naddress_update_run_id,None,None,None,'naddress')

    delta_table = DeltaTable.forName(spark, target_table)
    delta_table.alias('tgt').merge(
        filtered_naddress_matched_df.alias('src'),
        'tgt.sid_id = src.sid_id'
    ).whenMatchedUpdate(
        set={
            "account_id": "src.account_id",
            "named_account": "src.named_account",
            "billing_geolocation": "src.billing_geolocation",
            "shipping_geolocation": "src.shipping_geolocation",
            "billing_address_complete": "src.billing_address_complete",
            "billing_address_validation_status_msg": "src.billing_address_validation_status_msg",
            "billing_country_2char_code": "src.billing_country_2char_code",
            "shipping_address_complete": "src.shipping_address_complete",
            "shipping_address_validation_status_msg": "src.shipping_address_validation_status_msg",
            "shipping_country_2char_code": "src.shipping_country_2char_code",
            "master_record_id": "src.master_record_id",
            "type": "src.type",
            "billing_latitude": "src.billing_latitude",
            "billing_longitude": "src.billing_longitude",
            "billing_geocode_accuracy": "src.billing_geocode_accuracy",
            "shipping_latitude": "src.shipping_latitude",
            "shipping_longitude": "src.shipping_longitude",
            "shipping_geocode_accuracy": "src.shipping_geocode_accuracy",
            "phone": "src.phone",
            "account_number": "src.account_number",
            "website": "src.website",
            "archived": "src.archived",
            "billing_county": "src.billing_county",
            "shipping_county": "src.shipping_county",
            "billing_address_validation_status": "src.billing_address_validation_status",
            "hash_key": "src.hash_key",
            "last_modified_date": "src.last_modified_date",
            "last_modified_by_id": "src.last_modified_by_id",
            "ingested_timestamp": "src.ingested_timestamp"
        }
    ).execute()
    logger.info(f"Processing completed for {target_table}\n")

    # Audit Tracking 
    insert_start_time_row = audit_df.filter(col("run_id") == naddress_insert_run_id).select("start_time").collect()[0]
    insert_start_time = insert_start_time_row["start_time"]
    update_start_time_row = audit_df.filter(col("run_id") == naddress_update_run_id).select("start_time").collect()[0]
    update_start_time = update_start_time_row["start_time"]

    end_time = datetime.now()
    insert_run_time_value = (end_time - insert_start_time).total_seconds()
    update_run_time_value = (end_time - update_start_time).total_seconds()

    history_df = DeltaTable.forName(spark, f"{source_audit_table}").history(5)

    if history_df.count() > 0:
        #operation_metrics = history_df.select("operationMetrics").collect()[0][0]
        operation_metrics = history_df.select("operationMetrics").filter(col('operation')=="MERGE").orderBy(col('version').desc()).limit(1).collect()[0][0]
        num_inserted = int(operation_metrics.get("numTargetRowsInserted") or 0)
        num_updated = int(operation_metrics.get("numTargetRowsUpdated") or 0)
     
        insert_query = f"""MERGE INTO {dest_audit_table} src USING (SELECT '{naddress_insert_run_id}' AS run_id) dst ON src.run_id = dst.run_id
        WHEN MATCHED THEN UPDATE SET src.end_time = '{end_time}', src.status = 'SUCCESS', error_message = '', src.record_count = {num_inserted}, src.run_time = {insert_run_time_value}"""
        update_query = f"""MERGE INTO {dest_audit_table} src USING (SELECT '{naddress_update_run_id}' AS run_id) dst ON src.run_id = dst.run_id
        WHEN MATCHED THEN UPDATE SET src.end_time = '{end_time}', src.status = 'SUCCESS', error_message = '', src.record_count = {num_updated}, src.run_time = {update_run_time_value}"""
        
        spark.sql(insert_query)
        spark.sql(update_query)
except Exception as e:
    logger.error(f"Error occurred while processing for {target_table} : {str(e)}")
    logging.shutdown()
    udf_send_email(f'Account Data Refresh','address clean failure',{str(e)},source_catalog) 

    insert_start_time_row = audit_df.filter(col("run_id") == naddress_insert_run_id).select("start_time").collect()[0]
    insert_start_time = insert_start_time_row["start_time"]
    update_start_time_row = audit_df.filter(col("run_id") == naddress_update_run_id).select("start_time").collect()[0]
    update_start_time = update_start_time_row["start_time"]

    end_time = datetime.now()
    insert_run_time_value = (end_time - insert_start_time).total_seconds()
    update_run_time_value = (end_time - update_start_time).total_seconds()
   
    error_message = str(e).replace("'", "''")
    audit_df = spark.read.table(dest_audit_table)

    insert_query = f"""MERGE INTO {dest_audit_table} src USING (SELECT '{naddress_insert_run_id}' AS run_id) dst ON src.run_id = dst.run_id
    WHEN MATCHED THEN UPDATE SET src.end_time = '{end_time}', src.status = 'FAILURE', error_message = '{error_message}', src.run_time = {insert_run_time_value}"""
    update_query = f"""MERGE INTO {dest_audit_table} src USING (SELECT '{naddress_update_run_id}' AS run_id) dst ON src.run_id = dst.run_id
    WHEN MATCHED THEN UPDATE SET src.end_time = '{end_time}', src.status = 'FAILURE', error_message = '{error_message}', src.run_time = {update_run_time_value}"""
 
    spark.sql(insert_query)
    spark.sql(update_query)
    raise

# COMMAND ----------

udf_log_clean_operation_metrics(target_table)

# COMMAND ----------

# send matching_sid_id_df for cleaning and after cleaning set the final_clean_flag to 0,so that Harshita will consider those flags which are 0 and send it to Melissa API
# update target table(sf_account_clean_address) with address_matched_df records if the sid_id is matching

# COMMAND ----------

# MAGIC %md
# MAGIC ## Segregate Archived/N-Archived for Processing

# COMMAND ----------

df_archived = df_account.filter(col('archived') == True)
df_archived = df_archived.withColumn('final_clean_flag', when(col('archived') == True, None))
df_account = df_account.filter(col('archived') == False)

# COMMAND ----------

# dataset with clean column added for account name and reduced columns for demo purpose
# starting by populating name_clean column with Name so it can be updated overtime with each cleaning function
df_account_with_clean = df_account.select(col('sid_id'), col('name'), col('name').alias('name_clean'))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Function call

# COMMAND ----------

# main function to call all cleaning steps
def clean_name(df, column_name):
    df=split_acct_from_string(df, column_name)
    df=split_on_behalf_from_string(df, column_name)
    df=split_aka_from_string(df, column_name)
    df=remove_asterisks_from_string(df, column_name)
    df=split_careof_from_string(df, column_name)
    df=split_dba_from_string(df, column_name)
    df=split_do_not_from_string(df, column_name)
    df=split_duplicate_from_string(df, column_name)
    df=split_fbo_from_string(df, column_name)
    df=check_length_of_string(df, column_name)
    df=split_none_na_from_string(df, column_name)
    df=split_student_from_string(df, column_name)
    df=split_test_from_string(df, column_name)
    df=split_trust_from_string(df, column_name)
    df=split_tbd_from_string(df, column_name)
    # quick code to create final review flag
    df = df.withColumn(f'review_{column_name}_flag', F.when(F.col(f'{column_name}_acct_flag') | F.col(f'{column_name}_asterisks_flag')|
                                               F.col(f'{column_name}_care_of_flag')  | F.col(f'{column_name}_on_behalf_flag')|
                                                F.col(f'{column_name}_test_flag') |F.col(f'{column_name}_student_flag')   |
                                               F.col(f'{column_name}_trust_flag') |F.col(f'{column_name}_aka_flag')       | 
                                               F.col(f'{column_name}_tbd_flag')   | F.col(f'{column_name}_dba_flag')      | 
                                               F.col(f'{column_name}_fbo_flag')   | F.col(f'{column_name}_none_na_flag')  |
                                               F.col(f'{column_name}_donot_flag') | F.col(f'{column_name}_duplicate_flag') |
                                               F.col(f'{column_name}_length_flag')
                                              , True).otherwise(False))
    return df

# COMMAND ----------

# Apply the functions
try:
    cleaned_account_df = clean_name(df_account_with_clean, 'name_clean')
    logger.info(f"Processing completed for {target_table}\n")
except Exception as e:
    logger.error(f"Error occurred while processing for {target_table} : {str(e)}")
    logging.shutdown()
    udf_send_email('Account Data Refresh', 'address clean failure', {str(e)},source_catalog)
    print('into failure')

# COMMAND ----------

# MAGIC %md
# MAGIC ## BillingStreet / ShippingStreet Clean

# COMMAND ----------

df_account_BillingStreet_clean = df_account.select(col('sid_id'), col('billing_street'), col('billing_street').alias('billing_street_clean'))

# COMMAND ----------

df_account_ShippingStreet_clean = df_account.select(col('sid_id'), col('shipping_street'), col('shipping_street').alias('shipping_street_clean'))

# COMMAND ----------

# main function to call all cleaning steps
def clean_BStreet(df, column_name):
    df=split_attn_from_string(df, column_name)
    df=remove_asterisks_from_string(df, column_name)
    df=split_careof_from_string(df, column_name)
    df=split_tbd_from_string(df, column_name)
    df=split_test_from_string(df, column_name)
    # quick code to create final review flag
    df = df.withColumn(f'review_{column_name}_flag', F.when(F.col(f'{column_name}_attn_flag') | F.col(f'{column_name}_asterisks_flag')|F.col(f'{column_name}_care_of_flag')|F.col(f'{column_name}_tbd_flag') | F.col(f'{column_name}_test_flag'), True).otherwise(False))
    return df

# COMMAND ----------

# main function to call all cleaning steps
def clean_SStreet(df, column_name):
    df=split_attn_from_string(df, column_name)
    df=remove_asterisks_from_string(df, column_name)
    df=split_careof_from_string(df, column_name)
    df=split_tbd_from_string(df, column_name)
    df=split_test_from_string(df, column_name)
    # quick code to create final review flag
    df = df.withColumn(f'review_{column_name}_flag', F.when(F.col(f'{column_name}_attn_flag') | F.col(f'{column_name}_asterisks_flag')|F.col(f'{column_name}_care_of_flag')|F.col(f'{column_name}_tbd_flag') | F.col(f'{column_name}_test_flag'), True).otherwise(False))
    return df

# COMMAND ----------

# Apply the functions
try:
    cleaned_BStreet_df = clean_BStreet(df_account_BillingStreet_clean, 'billing_street_clean')
    logger.info(f"Processing completed for {target_table}\n")
except Exception as e:
    logger.error(f"Error occurred while processing for {target_table} : {str(e)}")
    logging.shutdown()
    udf_send_email('Account Data Refresh', 'address clean failure',{str(e)},source_catalog)
    print('into failure')

# COMMAND ----------

# Apply the functions
try:
    cleaned_SStreet_df = clean_SStreet(df_account_ShippingStreet_clean, 'shipping_street_clean')
    logger.info(f"Processing completed for {target_table}\n")
except Exception as e:
    logger.error(f"Error occurred while processing for {target_table} : {str(e)}")
    logging.shutdown()
    udf_send_email('Account Data Refresh', 'address clean failure', {str(e)},source_catalog)
    print('into failure')

# COMMAND ----------

# MAGIC %md
# MAGIC ## BillingCity / ShippingCity Clean

# COMMAND ----------

df_account_BillingCity_clean = df_account.select(col('sid_id'), col('billing_city'), col('billing_city').alias('billing_city_clean'))

# COMMAND ----------

df_account_ShippingCity_clean = df_account.select(col('sid_id'), col('shipping_city'), col('shipping_city').alias('shipping_city_clean'))

# COMMAND ----------


# Apply the functions
try:
    cleaned_BillingCity_df = check_is_null_string(df_account_BillingCity_clean, 'billing_city_clean')
    cleaned_BillingCity_df = check_numeric_string(cleaned_BillingCity_df, 'billing_city_clean')
    logger.info(f"Processing completed for {target_table}\n")
except Exception as e:
    logger.error(f"Error occurred while processing for {target_table} : {str(e)}")
    logging.shutdown()
    udf_send_email('Account Data Refresh', 'address clean failure', {str(e)},source_catalog)
    print('into failure')

# COMMAND ----------

# Apply the functions
try:
    cleaned_ShippingCity_df = check_is_null_string(df_account_ShippingCity_clean, 'shipping_city_clean')
    cleaned_ShippingCity_df = check_numeric_string(cleaned_ShippingCity_df, 'shipping_city_clean')
    logger.info(f"Processing completed for {target_table}\n")
except Exception as e:
    logger.error(f"Error occurred while processing for {target_table} : {str(e)}")
    logging.shutdown()
    udf_send_email('Account Data Refresh', 'address clean failure', {str(e)},source_catalog)
    print('into failure')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Billing/Shipping - State/Country/PostalCode Clean

# COMMAND ----------

from pyspark.sql.functions import col

df_account_BillingData_clean = df_account.select(
    col('sid_id'),
    col('billing_state'),
    col('billing_state').alias('billing_state_clean'),
    col('billing_country'),
    col('billing_country').alias('billing_country_clean'),
    col('billing_postal_code'),
    col('billing_postal_code').alias('billing_postal_code_clean')
)

# COMMAND ----------

from pyspark.sql.functions import col

df_account_ShippingData_clean = df_account.select(
    col('sid_id'),
    col('shipping_state'),
    col('shipping_state').alias('shipping_state_clean'),
    col('shipping_country'),
    col('shipping_country').alias('shipping_country_clean'),
    col('shipping_postal_code'),
    col('shipping_postal_code').alias('shipping_postal_code_clean')
)

# COMMAND ----------

# Apply the functions
try:
    cleaned_BillingData_df = check_is_null_string(df_account_BillingData_clean, 'billing_state_clean')
    cleaned_BillingData_df = check_numeric_string(cleaned_BillingData_df, 'billing_state_clean')
    logger.info(f"Processing completed for {target_table}\n")
except Exception as e:
    logger.error(f"Error occurred while processing for {target_table} : {str(e)}")
    logging.shutdown()
    udf_send_email('Account Data Refresh', 'address clean failure', {str(e)},source_catalog)
    print('into failure')

# COMMAND ----------

# Apply the functions
try:
    cleaned_ShippingData_df = check_is_null_string(df_account_ShippingData_clean, 'shipping_state_clean')
    cleaned_ShippingData_df = check_numeric_string(cleaned_ShippingData_df, 'shipping_state_clean')
    logger.info(f"Processing completed for {target_table}\n")
except Exception as e:
    logger.error(f"Error occurred while processing for {target_table} : {str(e)}")
    logging.shutdown()
    udf_send_email('Account Data Refresh', 'address clean failure', {str(e)},source_catalog)
    print('into failure')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Invalid State Check - CAN/AUS/USA

# COMMAND ----------

# List of USA abbreviations
try:
    valid_country_abbre= ["CA", "CAN", "CANADA"]
    billing_df = check_invalid_can_state(cleaned_BillingData_df, 'billing_state_clean', 'billing_country',valid_country_abbre)
    shipping_df = check_invalid_can_state(cleaned_ShippingData_df, 'shipping_state_clean', 'shipping_country', valid_country_abbre)
    logger.info(f"Processing completed for {target_table}\n")
except Exception as e:
    logger.error(f"Error occurred while processing for {target_table} : {str(e)}")
    logging.shutdown()
    udf_send_email('Account Data Refresh', 'address clean failure', {str(e)},source_catalog)
    print('into failure')

# COMMAND ----------

try:
    valid_country_abbre= ["AU", "AUS", "AUSTRALIA"]
    billing_df_aus=check_invalid_aus_state(billing_df, 'billing_state_clean', 'billing_country',valid_country_abbre)
    shipping_df_aus=check_invalid_aus_state(shipping_df, 'shipping_state_clean', 'shipping_country',valid_country_abbre)
    logger.info(f"Processing completed for {target_table}\n")
except Exception as e:
    logger.error(f"Error occurred while processing for {target_table} : {str(e)}")
    logging.shutdown()
    udf_send_email('Account Data Refresh', 'address clean failure', {str(e)},source_catalog)
    print('into failure')

# COMMAND ----------

try:
    valid_country_abbre=['US','USA','UNITED STATES','U.S. VIRGIN ISLANDS','AMERICA']
    billing_df_us = check_invalid_us_state(billing_df_aus, 'billing_state_clean', 'billing_country',valid_country_abbre)
    shipping_df_us = check_invalid_us_state(shipping_df_aus, 'shipping_state_clean', 'shipping_country',valid_country_abbre)
    logger.info(f"Processing completed for {target_table}\n")
except Exception as e:
    logger.error(f"Error occurred while processing for {target_table} : {str(e)}")
    logging.shutdown()
    udf_send_email('Account Data Refresh', 'address clean failure', {str(e)},source_catalog)
    print('into failure')

# COMMAND ----------

# quick code to create final review flag
billing_df_us = billing_df_us.withColumn('review_billing_state_clean_flag', 
                                            F.when(F.col('billing_state_clean_number_flag') |
                                                   F.col('invalid_ca_billing_state_flag') |
                                                   F.col('invalid_au_billing_state_flag') |
                                                   F.col('invalid_us_billing_state_flag') 
                                                   , True).otherwise(False))
                                           

# COMMAND ----------

# quick code to create final review flag
shipping_df_us = shipping_df_us.withColumn('review_shipping_state_clean_flag',
                                                   F.when(F.col('shipping_state_clean_number_flag')|
                                                   F.col('invalid_ca_shipping_state_flag') |
                                                   F.col('invalid_au_shipping_state_flag') |
                                                   F.col('invalid_us_shipping_state_flag') 
                                                   , True).otherwise(False))

# COMMAND ----------

# Apply the functions
try:
    cleaned_BillingCountry_df = check_is_null_string(billing_df_us, 'billing_country_clean')
    cleaned_BillingCountry_df = check_numeric_string(cleaned_BillingCountry_df, 'billing_country_clean')
    logger.info(f"Processing completed for {target_table}\n")
except Exception as e:
    logger.error(f"Error occurred while processing for {target_table} : {str(e)}")
    logging.shutdown()
    udf_send_email('Account Data Refresh', 'address clean failure', {str(e)},source_catalog)
    print('into failure')

# COMMAND ----------

# Apply the functions
try:
    cleaned_ShippingCountry_df = check_is_null_string(shipping_df_us, 'shipping_country_clean')
    cleaned_ShippingCountry_df = check_numeric_string(cleaned_ShippingCountry_df, 'shipping_country_clean')
    logger.info(f"Processing completed for {target_table}\n")
except Exception as e:
    logger.error(f"Error occurred while processing for {target_table} : {str(e)}")
    logging.shutdown()
    udf_send_email('Account Data Refresh', 'address clean failure', {str(e)},source_catalog)
    print('into failure')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Final Cleaned DF - Upto Country

# COMMAND ----------

# dfs = [cleaned_account_df, cleaned_BStreet_df, cleaned_SStreet_df]
merged_dframe = cleaned_account_df.join(cleaned_BStreet_df, on='sid_id').join(cleaned_SStreet_df, on='sid_id')

# COMMAND ----------

cleaned_df_upto_country=merged_dframe.join(cleaned_BillingCity_df,on='sid_id').join(cleaned_ShippingCity_df,on='sid_id').join(cleaned_BillingCountry_df,on='sid_id').join(cleaned_ShippingCountry_df,on='sid_id')

# COMMAND ----------

#consider the final dataframe as cleaned_df_upto_country, and write functions for postal codes on cleaned_df_upto_country dataframe

# COMMAND ----------

# MAGIC %md
# MAGIC ## Postal-Codes validation

# COMMAND ----------

# Define country-specific invalid postal codes
invalid_postal_codes_by_country = {
    'BE': ['1853 GRIMBERGEN','B1000' ],
    'BR': ['4578','4578000','3116000', '4007910' ,'4543' ,'4543121', '4562' ,'4601000', '6273070' ,'6419300', 
           '9781220','CEP 05348-000','O4578','SP','SP045-000'],
    'CN': ['10002' ,'20002' ,'200433' ,'45/F' ,'99907'],
    'IE': ['(01) 2083111','1234' ,'68' ,'904141', 'BT480-BF','D02' ,'D02 W', 'D08 E' ,'D2' ,'R95 P', 'T45P6'],
    'IT': ['00054 MACCARESE' ,'12' ,'148' ,'185' ,'9604' ,'PN 33'],
    'MX': ['1600','1790','2090','3700','3730','3900','4257','4310','5348','5410','6100','6700','6720','9360',
           'C.P. 44160'],
    'NZ': ['624'],
    'PL': ['40-11'],
    'SG': ['17910','49145','079914 SG','16920','18897','18936','22946','3919','48619','48623','57615' ,'60991' ,   
           '65456' ,'66964' ,'68902'],
    'ZA': ['39'],
    'ES': ['8019','15143 ARTEIXO','8174','CP 28223'],
    'SE': ['431 3','SE417']
}

# COMMAND ----------

# Country abbreviations mapped to country codes
country_groups = {
    'BE': ['BE', 'BEL', 'BELGIUM'],
    'BR': ['BR', 'BRA', 'BRAZIL'],
    'CN': ['CN', 'CHN', 'CHINA'],
    'IE': ['IE', 'IRL', 'IRELAND'],
    'IT': ['IT', 'ITA', 'ITALY'],
    'MX': ['MX', 'MEX', 'MEXICO'],
    'NZ': ['NZ', 'NZL', 'NEW ZEALAND'],
    'PL': ['PL', 'POL', 'POLAND'],
    'SG': ['SG', 'SGP', 'SINGAPORE'],
    'ZA': ['ZA', 'ZAF', 'SOUTH AFRICA'],
    'ES': ['ES', 'ESP', 'SPAIN'],
    'SE': ['SE', 'SWE', 'SWEDEN']
}

# COMMAND ----------

# Apply the function for Billing and Shipping
try:
    df_postal_check_billing = check_invalid_code(cleaned_df_upto_country, 'billing_postal_code_clean', 'billing_country')
    df_postal_check_shipping = check_invalid_code(df_postal_check_billing,'shipping_postal_code_clean',    'shipping_country')
    logger.info(f"Processing completed for {target_table}\n")
except Exception as e:
    logger.error(f"Error occurred while processing for {target_table} : {str(e)}")
    logging.shutdown()
    udf_send_email('Account Data Refresh', 'address clean failure', {str(e)},source_catalog)
    print('into failure')

# COMMAND ----------

# Apply the function for Billing and Shipping
try:
    df_postal_check_shipping = check_valid_code(df_postal_check_shipping, 'billing_postal_code_clean', 'billing_country')
    df_postal_check_shipping = check_valid_code(df_postal_check_shipping,'shipping_postal_code_clean',    'shipping_country')
    logger.info(f"Processing completed for {target_table}\n")
except Exception as e:
    logger.error(f"Error occurred while processing for {target_table} : {str(e)}")
    logging.shutdown()
    udf_send_email('Account Data Refresh', 'address clean failure', {str(e)},source_catalog)
    print('into failure')

# COMMAND ----------

# Ensure final review flag column exists, otherwise initialize with False
if 'final_review_postal_code_flag' not in df_postal_check_shipping.columns:
        df_postal_check_shipping = df_postal_check_shipping.withColumn('final_review_postal_code_flag', lit(False))
df_postal_check_shipping = df_postal_check_shipping.withColumn(
    "final_review_postal_code_flag",
    (col("review_invalid_billing_postal_code_clean_flag") | col("review_invalid_shipping_postal_code_clean_flag"))
)

# COMMAND ----------

#  add last flag column to df_postal_check_shipping df if any of the flag is true else false
# add additional columns mentioned in the ticket to newly created df
#  merge above df's
# copy the data to table

# COMMAND ----------

df_postal_check_shipping = df_postal_check_shipping.withColumn('review_billing_address_flag',
                                                   F.when(F.col('review_billing_street_clean_flag') |
                                                   F.col('billing_city_clean_number_flag') |
                                                   F.col('review_billing_state_clean_flag') |
                                                   F.col('billing_country_clean_number_flag') |
                                                   F.col('review_invalid_billing_postal_code_clean_flag')
                                                   , True).otherwise(False))

# COMMAND ----------

# quick code to create final review flag
df_postal_check_shipping = df_postal_check_shipping.withColumn('review_shipping_address_flag',
                                                   F.when(F.col('review_shipping_street_clean_flag') |
                                                   F.col('shipping_city_clean_number_flag') |
                                                   F.col('review_shipping_state_clean_flag') |
                                                   F.col('shipping_country_clean_number_flag') |
                                                   F.col('review_invalid_shipping_postal_code_clean_flag')
                                                   , True).otherwise(False))

# COMMAND ----------

# quick code to create final review flag
df_postal_check_shipping = df_postal_check_shipping.withColumn('final_review_all_address_columns_flag',
                                                   F.when(F.col('review_name_clean_flag') |
                                                   F.col('review_billing_street_clean_flag') |
                                                   F.col('review_shipping_street_clean_flag') |
                                                   F.col('billing_city_clean_number_flag') |
                                                   F.col('shipping_city_clean_number_flag') |
                                                   F.col('review_billing_state_clean_flag') |
                                                   F.col('review_shipping_state_clean_flag') |
                                                   F.col('billing_country_clean_number_flag') |
                                                   F.col('shipping_country_clean_number_flag') |
                                                   F.col('final_review_postal_code_flag') 
                                                   , True).otherwise(False))

# COMMAND ----------

#considering additional columns and making into one dataframe

# COMMAND ----------

from pyspark.sql.functions import col

df_additional_columns = df_account.select(
    col('sid_id'), col('system_modstamp'), col('account_id'), col('named_account'),
    col('billing_geolocation'), col('shipping_geolocation'), col('billing_address_complete'),
    col('billing_address_validation_status_msg'), col('billing_country_2char_code'),
    col('shipping_address_complete'), col('shipping_address_validation_status_msg'),
    col('shipping_country_2char_code'), col('master_record_id'), col('type'),
    col('billing_latitude'), col('billing_longitude'), col('billing_geocode_accuracy'),
    col('shipping_latitude'), col('shipping_longitude'), col('shipping_geocode_accuracy'),
    col('phone'), col('account_number'), col('website'), col('created_date'),
    col('created_by_id'), col('last_modified_date'), col('last_modified_by_id'),
    col('archived'), col('billing_county'), col('shipping_county'),
    col('billing_address_validation_status'), col('hash_key'), col('ingested_timestamp')
)

# COMMAND ----------

# merge all cleaned columns with additional columns

# COMMAND ----------

# MAGIC %md
# MAGIC ## Final Cleaned Dataframe

# COMMAND ----------

finally_cleaned_dataframe=df_postal_check_shipping.join(df_additional_columns,on='sid_id')

# COMMAND ----------

columns_to_move = [
    "name_clean_additional_data",
    "billing_street_clean_additional_data",
    "shipping_street_clean_additional_data",
    "review_billing_address_flag",
    "review_shipping_address_flag"
]

# Get the list of columns in the dataframe
all_columns = finally_cleaned_dataframe.columns

# Create a new column order with the specified columns moved to the end
new_column_order = [col for col in all_columns if col not in columns_to_move] + columns_to_move

# Reorder the dataframe columns
finally_cleaned_dataframe = finally_cleaned_dataframe.select(*new_column_order)
# display(finally_cleaned_dataframe)

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC ## Cache for Performance

# COMMAND ----------

#finally_cleaned_dataframe.explain(True)
finally_cleaned_dataframe.cache()
#finally_cleaned_dataframe.count()

# COMMAND ----------

from pyspark.sql.functions import lit
finally_cleaned_dataframe = finally_cleaned_dataframe.withColumn("final_clean_flag", lit(0))
finally_cleaned_dataframe.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Manual Review Process
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##updating empty values to null

# COMMAND ----------

cleaned_columns = [
    "billing_street_clean",
    "billing_city_clean",
    "billing_country_clean",
    "billing_state_clean",
    "billing_postal_code_clean",
    "shipping_street_clean",
    "shipping_city_clean",
    "shipping_country_clean",
    "shipping_state_clean",
    "shipping_postal_code_clean"
]
for column in cleaned_columns:
    finally_cleaned_dataframe = finally_cleaned_dataframe.withColumn(
        column,
        when(col(column) == "", None).otherwise(col(column))
    )

# COMMAND ----------

import pyspark.sql.functions as F

finally_cleaned_dataframe = finally_cleaned_dataframe.withColumn(
    "needs_manual_review",
    F.when(
        (F.col("billing_street").isNotNull() & F.col("billing_street_clean").isNull()) |
        (F.col("billing_city").isNotNull() & F.col("billing_city_clean").isNull()) |
        (F.col("billing_country").isNotNull() & F.col("billing_country_clean").isNull()) |
        (F.col("billing_state").isNotNull() & F.col("billing_state_clean").isNull()) |
        (F.col("billing_postal_code").isNotNull() & F.col("billing_postal_code_clean").isNull()) |
        (F.col("shipping_street").isNotNull() & F.col("shipping_street_clean").isNull()) |
        (F.col("shipping_city").isNotNull() & F.col("shipping_city_clean").isNull()) |
        (F.col("shipping_country").isNotNull() & F.col("shipping_country_clean").isNull()) |
        (F.col("shipping_state").isNotNull() & F.col("shipping_state_clean").isNull()) |
        (F.col("shipping_postal_code").isNotNull() & F.col("shipping_postal_code_clean").isNull()) |
        (~F.col("billing_street_clean").isNull() & ~F.col("billing_street_clean").rlike(r'[\d\uFF10-\uFF19]')) |
        (~F.col("shipping_street_clean").isNull() & ~F.col("shipping_street_clean").rlike(r'[\d\uFF10-\uFF19]')) |
        (F.col("review_billing_state_clean_flag") == True) |
        (F.col("review_shipping_state_clean_flag") == True) |
        (F.col("billing_country_clean_number_flag") == True) |
        (F.col("shipping_country_clean_number_flag") == True) |
        (F.col("final_review_postal_code_flag")==True)|
        ((F.col('billing_city_clean_number_flag') == True) & (~F.upper(F.col('billing_country')).isin('US', 'USA', 'AMERICA'))) |
        ((F.col('shipping_city_clean_number_flag') == True) & (~F.upper(F.col('shipping_country')).isin('US', 'USA', 'AMERICA'))),
        True
    ).otherwise(False)
).withColumn(
    "final_clean_flag",
    F.when(F.col("needs_manual_review") == True, 2).otherwise(F.col("final_clean_flag"))
)

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.functions import col

manual_review_true = finally_cleaned_dataframe.filter(F.col("needs_manual_review")==True)
manual_review_true.count()  #1383

# COMMAND ----------

from pyspark.sql.functions import col

manual_review_false = finally_cleaned_dataframe.filter(col("needs_manual_review")==False)
manual_review_false.count() --18888

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
manual_review_false = manual_review_false.withColumn("address_last_cleaned_timestamp", current_timestamp())

# COMMAND ----------

# copying manual_review records from 'mdm-dev.clean.sf_account_clean_address' to 'mdm_dev.tmp.sf_account_clean_address_for_review' table

# COMMAND ----------

#matching record awaiting review
#yes-> update matching record with new cleaned values in review table
#no-> insert record to review table

# COMMAND ----------

# MAGIC %md
# MAGIC ## Merge-2: Manual Review records

# COMMAND ----------

from delta.tables import DeltaTable

try:
    # Convert target table to Delta Table
    delta_target = DeltaTable.forName(spark, f'{source_catalog}.tmp.sf_account_clean_address_for_review')
    
    # Get the columns of the target Delta table
    target_columns = delta_target.toDF().columns
    
    # Perform Merge Operation on Delta table
    delta_target.alias("tgt").merge(
        manual_review_true.alias("src"),
        "tgt.sid_id = src.sid_id"
    ).whenMatchedUpdate(
        condition="tgt.hash_key != src.hash_key",  # Update only if hash_key differs (record changed)
        set={col: f"src.{col}" for col in manual_review_true.columns if col in target_columns}  # Replace only columns that exist in target table
    ).whenNotMatchedInsert(
        values={col: f"src.{col}" for col in manual_review_true.columns if col in target_columns}  # Insert only columns that exist in target table
    ).execute()

    '''
    delta_target.alias("tgt").merge(
        manual_review_false.alias("src"),
        "tgt.sid_id = src.sid_id"
    ).whenMatchedDelete(
        condition="tgt.hash_key != src.hash_key"
    ).execute()
    '''
    
    logger.info(f"Manual Review Records Completed for {target_table}\n")
    #udf_send_email('Account Data Refresh', 'address clean success', '',source_catalog)
except Exception as e:
    logger.error(f"Error occurred while processing for {target_table} : {str(e)}")    
    udf_send_email('Account Data Refresh', 'address clean failure', str(e),source_catalog)
    print('into failure')

# COMMAND ----------

from delta.tables import DeltaTable

try:
    # Convert target table to Delta Table
    delta_target = DeltaTable.forName(spark, f'{source_catalog}.clean.sf_account_clean_address_reviewed') 
       
    # Perform Append Operation on Delta table
    manual_review_true.write.format("delta").mode("append").saveAsTable(f'{source_catalog}.clean.sf_account_clean_address_reviewed')
    
    #Get matching IDs from target
    target_ids_df = delta_target.toDF().select('sid_id', 'hash_key').distinct()
    #print(target_ids)

    #Filter source records where ID exists in manual_review_false
    matching_df = manual_review_false.alias("src").join(
    target_ids_df.alias("tgt"),
    (col('src.sid_id') == col('tgt.sid_id')) & (col('src.hash_key') != col('tgt.hash_key')),
    'inner'
    ).select('src.*')   

    matching_df = matching_df.withColumn("review_status", lit("Reviewed")).withColumn("reviewed_by", lit("Salesforce")).withColumn("reviewed_date", current_timestamp())

    display(matching_df)
    #Insert matching records into target Delta table
    matching_df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(f'{source_catalog}.clean.sf_account_clean_address_reviewed')

    logger.info(f"Manual Review Records Added to sf_account_clean_address_reviewed Table\n")
    #udf_send_email('Account Data Refresh', 'address clean success', '',source_catalog)
except Exception as e:
    logger.error(f"Error occurred while processing for {target_table} : {str(e)}")
    #logging.shutdown()
    udf_send_email('Account Data Refresh', 'address clean failure', str(e),source_catalog)
    print('into failure')

# COMMAND ----------

#no-> matching record awaiting review -> y-> update matching review record
                                        #n-> insert record to clean table

# COMMAND ----------

# MAGIC %md
# MAGIC ## Merge-3: Non-Archived cleaned records - MR

# COMMAND ----------

from delta.tables import DeltaTable

try:
    # Convert target table to Delta Table
    delta_target = DeltaTable.forName(spark, f'{source_catalog}.tmp.sf_account_clean_address_for_review')
    
    # Get the columns of the target Delta table
    target_columns = delta_target.toDF().columns

    manual_review_false = manual_review_false.withColumn("review_status", lit("Reviewed")).withColumn("reviewed_by", lit("Salesforce")).withColumn("reviewed_date", current_timestamp())
    
    # Perform Merge Operation on Delta table
    delta_target.alias("tgt").merge(
        manual_review_false.alias("src"),
        "tgt.sid_id = src.sid_id"
    ).whenMatchedUpdate(
        condition="tgt.hash_key != src.hash_key",  # Update only if hash_key differs (record changed)
        set={col: f"src.{col}" for col in manual_review_false.columns if col in target_columns}  # Replace only columns that exist in target table
    ).execute()
    logger.info(f"Manual Review Records update Completed for {target_table}\n")
    #udf_send_email('Account Data Refresh', 'address clean success', '',source_catalog)
except Exception as e:
    logger.error(f"Error occurred while processing for {target_table} : {str(e)}")
    #logging.shutdown()
    udf_send_email('Account Data Refresh', 'address clean failure', str(e),source_catalog)
    print('into failure')

# COMMAND ----------

udf_log_clean_operation_metrics('mdm_dev.tmp.sf_account_clean_address_for_review')

# COMMAND ----------

manual_review_false = manual_review_false.drop('review_status','reviewed_by','reviewed_date')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Merge-4: Non-Archived cleaned records

# COMMAND ----------

from delta.tables import DeltaTable
import pyspark.sql.functions as F
from pyspark.sql.functions import col, unix_timestamp
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

narchive_insert_run_id  =  str(datetime.now().strftime('%Y%m%d%H%M')) + '-' + 'clean_narchive_address_insert'
narchive_update_run_id  =  str(datetime.now().strftime('%Y%m%d%H%M')) + '-' + 'clean_narchive_address_update'

try:
    source_audit_table = f'{source_catalog}.clean.sf_account_clean_address'
    dest_audit_table = f'{source_catalog}.landing.audit_logs'
    audit_df = spark.read.table(dest_audit_table)
    audit_tracking_clean_address(narchive_insert_run_id,narchive_update_run_id,None,None,None,'narchived')

    # Convert target table to Delta Table
    delta_target = DeltaTable.forName(spark, target_table)

    # Perform Merge Operation on Delta table
    delta_target.alias("tgt").merge(
        manual_review_false.alias("src"),
        "tgt.sid_id = src.sid_id"
    ).whenMatchedUpdate(
        condition="tgt.hash_key != src.hash_key",  # Update only if hash_key differs (record changed)
        set={col: f"src.{col}" for col in manual_review_false.columns} 
    ).whenNotMatchedInsert(
        values={col: f"src.{col}" for col in manual_review_false.columns}
    ).execute()
    logger.info(f"Non Archive Processing completed for {target_table}\n")

    # Audit Tracking 
    insert_start_time_row = audit_df.filter(col("run_id") == narchive_insert_run_id).select("start_time").collect()[0]
    insert_start_time = insert_start_time_row["start_time"]
    update_start_time_row = audit_df.filter(col("run_id") == narchive_update_run_id).select("start_time").collect()[0]
    update_start_time = update_start_time_row["start_time"]

    end_time = datetime.now()
    insert_run_time_value = (end_time - insert_start_time).total_seconds()
    update_run_time_value = (end_time - update_start_time).total_seconds()

    history_df = DeltaTable.forName(spark, f"{source_audit_table}").history(5)

    if history_df.count() > 0:
        #operation_metrics = history_df.select("operationMetrics").collect()[0][0]
        operation_metrics = history_df.select("operationMetrics").filter(col('operation')=="MERGE").orderBy(col('version').desc()).limit(1).collect()[0][0]        
        num_inserted = int(operation_metrics.get("numTargetRowsInserted") or 0)
        num_updated = int(operation_metrics.get("numTargetRowsUpdated") or 0)
     
        insert_query = f"""MERGE INTO {dest_audit_table} src USING (SELECT '{narchive_insert_run_id}' AS run_id) dst ON src.run_id = dst.run_id
        WHEN MATCHED THEN UPDATE SET src.end_time = '{end_time}', src.status = 'SUCCESS', error_message = '', src.record_count = {num_inserted}, src.run_time = {insert_run_time_value}"""
        update_query = f"""MERGE INTO {dest_audit_table} src USING (SELECT '{narchive_update_run_id}' AS run_id) dst ON src.run_id = dst.run_id
        WHEN MATCHED THEN UPDATE SET src.end_time = '{end_time}', src.status = 'SUCCESS', error_message = '', src.record_count = {num_updated}, src.run_time = {update_run_time_value}"""
        
        spark.sql(insert_query)
        spark.sql(update_query)
except Exception as e:
    logger.error(f"Error occurred while processing for {target_table} : {str(e)}")
    #logging.shutdown()
    udf_send_email(f'Account Data Refresh','address clean failure',{str(e)},source_catalog)

    insert_start_time_row = audit_df.filter(col("run_id") == narchive_insert_run_id).select("start_time").collect()[0]
    insert_start_time = insert_start_time_row["start_time"]
    update_start_time_row = audit_df.filter(col("run_id") == narchive_update_run_id).select("start_time").collect()[0]
    update_start_time = update_start_time_row["start_time"]

    end_time = datetime.now()
    insert_run_time_value = (end_time - insert_start_time).total_seconds()
    update_run_time_value = (end_time - update_start_time).total_seconds()
   
    error_message = str(e).replace("'", "''")
    audit_df = spark.read.table(dest_audit_table)

    insert_query = f"""MERGE INTO {dest_audit_table} src USING (SELECT '{narchive_insert_run_id}' AS run_id) dst ON src.run_id = dst.run_id
    WHEN MATCHED THEN UPDATE SET src.end_time = '{end_time}', src.status = 'FAILURE', error_message = '{error_message}', src.run_time = {insert_run_time_value}"""
    update_query = f"""MERGE INTO {dest_audit_table} src USING (SELECT '{narchive_update_run_id}' AS run_id) dst ON src.run_id = dst.run_id
    WHEN MATCHED THEN UPDATE SET src.end_time = '{end_time}', src.status = 'FAILURE', error_message = '{error_message}', src.run_time = {update_run_time_value}"""
 
    spark.sql(insert_query)
    spark.sql(update_query)
    raise

# COMMAND ----------

udf_log_clean_operation_metrics(target_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Merge-5: Archived records

# COMMAND ----------

from pyspark.sql.functions import col, unix_timestamp
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

insert_run_id  =  str(datetime.now().strftime('%Y%m%d%H%M')) + '-' + 'clean_address_insert'
update_run_id  =  str(datetime.now().strftime('%Y%m%d%H%M')) + '-' + 'clean_address_update'
delete_run_id  =  str(datetime.now().strftime('%Y%m%d%H%M')) + '-' + 'clean_address_delete'
clean_run_id   =  str(datetime.now().strftime('%Y%m%d%H%M')) + '-' + 'clean_address_clean'
review_run_id  =  str(datetime.now().strftime('%Y%m%d%H%M')) + '-' + 'clean_address_review'

try:
    mr_audit_table = f'{source_catalog}.tmp.sf_account_clean_address_for_review'  
    source_audit_table = f'{source_catalog}.clean.sf_account_clean_address' 
    dest_audit_table = f'{source_catalog}.landing.audit_logs'
    audit_df = spark.read.table(dest_audit_table)
    audit_tracking_clean_address(insert_run_id,update_run_id,delete_run_id,clean_run_id,review_run_id,'archived')

    # Convert target table to Delta Table
    delta_target = DeltaTable.forName(spark, target_table)
    
    # Get the columns of the target Delta table
    target_columns = delta_target.toDF().columns
    
    # Perform Merge Operation on Delta table
    delta_target.alias("tgt").merge(
        df_archived.alias("src"),
        "tgt.sid_id = src.sid_id"
    ).whenMatchedUpdate(
        condition="tgt.hash_key != src.hash_key",  # Update only if hash_key differs (record changed)
        set={col: f"src.{col}" for col in df_archived.columns if col in target_columns}  # Replace only columns that exist in target table
    ).whenNotMatchedInsert(
        values={col: f"src.{col}" for col in df_archived.columns if col in target_columns}  # Insert only columns that exist in target table
    ).execute()
    logger.info(f" Archived records Processing completed for {target_table}\n")

    # Audit Tracking 
    insert_start_time_row = audit_df.filter(col("run_id") == insert_run_id).select("start_time").collect()[0]
    insert_start_time = insert_start_time_row["start_time"]
    update_start_time_row = audit_df.filter(col("run_id") == update_run_id).select("start_time").collect()[0]
    update_start_time = update_start_time_row["start_time"]
    delete_start_time_row = audit_df.filter(col("run_id") == delete_run_id).select("start_time").collect()[0]
    delete_start_time = delete_start_time_row["start_time"]        
    clean_start_time_row = audit_df.filter(col("run_id") == clean_run_id).select("start_time").collect()[0]
    clean_start_time = clean_start_time_row["start_time"]        
    review_start_time_row = audit_df.filter(col("run_id") == review_run_id).select("start_time").collect()[0]
    review_start_time = review_start_time_row["start_time"]

    end_time = datetime.now()
    insert_run_time_value = (end_time - insert_start_time).total_seconds()
    update_run_time_value = (end_time - update_start_time).total_seconds()
    delete_run_time_value = (end_time - delete_start_time).total_seconds()
    clean_run_time_value  = (end_time - clean_start_time).total_seconds()
    review_run_time_value = (end_time - review_start_time).total_seconds()

    history_df = DeltaTable.forName(spark, f"{source_audit_table}").history(5)

    if history_df.count() > 0:
        #operation_metrics = history_df.select("operationMetrics").collect()[0][0]
        operation_metrics = history_df.select("operationMetrics").filter(col('operation')=="MERGE").orderBy(col('version').desc()).limit(1).collect()[0][0]
        num_inserted = int(operation_metrics.get("numTargetRowsInserted") or 0)
        num_updated = int(operation_metrics.get("numTargetRowsUpdated") or 0)
        num_deleted = int(operation_metrics.get("numTargetRowsDeleted") or 0)
        num_clean = spark.read.table(source_audit_table).filter(col("final_clean_flag") ==0).count()
        num_manual_review = spark.read.table(mr_audit_table).filter(col("needs_manual_review") == True).count()

        insert_query = f"""MERGE INTO {dest_audit_table} src USING (SELECT '{insert_run_id}' AS run_id) dst ON src.run_id = dst.run_id
        WHEN MATCHED THEN UPDATE SET src.end_time = '{end_time}', src.status = 'SUCCESS', error_message = '', src.record_count = {num_inserted}, src.run_time = {insert_run_time_value}"""
        update_query = f"""MERGE INTO {dest_audit_table} src USING (SELECT '{update_run_id}' AS run_id) dst ON src.run_id = dst.run_id
        WHEN MATCHED THEN UPDATE SET src.end_time = '{end_time}', src.status = 'SUCCESS', error_message = '', src.record_count = {num_updated}, src.run_time = {update_run_time_value}"""
        delete_query = f"""MERGE INTO {dest_audit_table} src USING (SELECT '{delete_run_id}' AS run_id) dst ON src.run_id = dst.run_id
        WHEN MATCHED THEN UPDATE SET src.end_time = '{end_time}', src.status = 'NOT PERFORMED', error_message = '', src.record_count = {num_deleted}"""
        clean_query = f"""MERGE INTO {dest_audit_table} src USING (SELECT '{clean_run_id}' AS run_id) dst ON src.run_id = dst.run_id
        WHEN MATCHED THEN UPDATE SET src.end_time = '{end_time}', src.status = 'SUCCESS', error_message = '', src.record_count = {num_clean}, src.run_time = {clean_run_time_value}"""
        review_query = f"""MERGE INTO {dest_audit_table} src USING (SELECT '{review_run_id}' AS run_id) dst ON src.run_id = dst.run_id
        WHEN MATCHED THEN UPDATE SET src.end_time = '{end_time}', src.status = 'SUCCESS', error_message = '', src.record_count = {num_manual_review}, src.run_time = {review_run_time_value}"""

        spark.sql(insert_query)
        spark.sql(update_query)
        spark.sql(delete_query)
        spark.sql(clean_query)
        spark.sql(review_query)

        udf_send_email('Account Data Refresh', 'address clean success', '',source_catalog)

except Exception as e:
    logger.error(f"Error occurred while processing for {target_table} : {str(e)}")
    logging.shutdown()
    udf_send_email(f'Account Data Refresh','address clean failure',{str(e)},source_catalog)

    insert_start_time_row = audit_df.filter(col("run_id") == insert_run_id).select("start_time").collect()[0]
    insert_start_time = insert_start_time_row["start_time"]
    update_start_time_row = audit_df.filter(col("run_id") == update_run_id).select("start_time").collect()[0]
    update_start_time = update_start_time_row["start_time"]
    delete_start_time_row = audit_df.filter(col("run_id") == delete_run_id).select("start_time").collect()[0]
    delete_start_time = delete_start_time_row["start_time"]        
    clean_start_time_row = audit_df.filter(col("run_id") == clean_run_id).select("start_time").collect()[0]
    clean_start_time = clean_start_time_row["start_time"]        
    review_start_time_row = audit_df.filter(col("run_id") == review_run_id).select("start_time").collect()[0]
    review_start_time = review_start_time_row["start_time"]

    end_time = datetime.now()
    insert_run_time_value = (end_time - insert_start_time).total_seconds()
    update_run_time_value = (end_time - update_start_time).total_seconds()
    delete_run_time_value = (end_time - delete_start_time).total_seconds()
    clean_run_time_value  = (end_time - clean_start_time).total_seconds()
    review_run_time_value = (end_time - review_start_time).total_seconds()
    
    error_message = str(e).replace("'", "''")
    audit_df = spark.read.table(dest_audit_table)

    insert_query = f"""MERGE INTO {dest_audit_table} src USING (SELECT '{insert_run_id}' AS run_id) dst ON src.run_id = dst.run_id
    WHEN MATCHED THEN UPDATE SET src.end_time = '{end_time}', src.status = 'FAILURE', error_message = '{error_message}', src.run_time = {insert_run_time_value}"""
    update_query = f"""MERGE INTO {dest_audit_table} src USING (SELECT '{update_run_id}' AS run_id) dst ON src.run_id = dst.run_id
    WHEN MATCHED THEN UPDATE SET src.end_time = '{end_time}', src.status = 'FAILURE', error_message = '{error_message}', src.run_time = {update_run_time_value}"""
    delete_query = f"""MERGE INTO {dest_audit_table} src USING (SELECT '{delete_run_id}' AS run_id) dst ON src.run_id = dst.run_id
    WHEN MATCHED THEN UPDATE SET src.end_time = '{end_time}', src.status = 'NOT PERFORMED', error_message = '{error_message}'"""
    clean_query = f"""MERGE INTO {dest_audit_table} src USING (SELECT '{clean_run_id}' AS run_id) dst ON src.run_id = dst.run_id
    WHEN MATCHED THEN UPDATE SET src.end_time = '{end_time}', src.status = 'FAILURE', error_message = '{error_message}', src.run_time = {clean_run_time_value}"""
    review_query = f"""MERGE INTO {dest_audit_table} src USING (SELECT '{review_run_id}' AS run_id) dst ON src.run_id = dst.run_id
    WHEN MATCHED THEN UPDATE SET src.end_time = '{end_time}', src.status = 'FAILURE', error_message = '{error_message}', src.run_time = {review_run_time_value}"""

    spark.sql(insert_query)
    spark.sql(update_query)
    spark.sql(delete_query)
    spark.sql(clean_query)
    spark.sql(review_query)
    raise

# COMMAND ----------

finally_cleaned_dataframe.unpersist()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Manual Review Notification

# COMMAND ----------

from delta.tables import DeltaTable
review_target_table = f"{source_catalog}.tmp.sf_account_clean_address_for_review"

try:
    mr_df = spark.read.table(f"{source_catalog}.tmp.sf_account_clean_address_for_review").filter(col('final_clean_flag')==2)
    mr_count = mr_df.count()
    if(mr_count > 0):
        udf_send_email('Account Data Refresh','review success','',source_catalog,mr_count,review_target_table)
    else:
        print("No records to review")
except Exception as e:
    udf_send_email('Account Data Refresh','review failure',{str(e)},source_catalog)
    print('into failure')

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC ## Logging Process

# COMMAND ----------

udf_log_clean_operation_metrics(target_table)

# COMMAND ----------

udf_log_clean_operation_metrics(f'{source_catalog}.tmp.sf_account_clean_address_for_review')

# COMMAND ----------

logging.shutdown()
