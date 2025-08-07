# Databricks notebook source
%pip install pyyaml

# COMMAND ----------

import pandas as pd
import json
import urllib.parse
import urllib.request
from datetime import datetime
import requests
import logging
import yaml
from pyspark.sql.functions import col, current_timestamp, when

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
log_directory = f"/Volumes/{catalog}/clean/clean_logs/{datetime.now().strftime('%Y%m%d')}"
log_file_path = f"{log_directory}/melissa_api_final_{datetime.now().strftime('%Y%m%d%H%M%S')}.log"
# creating a logger to log the execution and errors
logger = udf_setup_logging(log_directory, log_file_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Audit Tracking

# COMMAND ----------

def audit_tracking_melissa_clean(insert_run_id,update_run_id,delete_run_id,clean_run_id):
    source_table = f'{catalog}.clean.melissa_api_final'
    audit_table = f'{catalog}.landing.audit_logs'
    inter_table = f'{catalog}.clean.sf_account_clean_address'

    start_time = datetime.now()
    insert_run_id  =  insert_run_id
    update_run_id  =  update_run_id
    delete_run_id  =  delete_run_id
    clean_run_id   =  clean_run_id

    audit_data = [(source_table, "INSERT", start_time, None,"IN_PROGRESS","" ,insert_run_id,'melissa_clean_insert'),(source_table, "UPDATE", start_time, None,"IN_PROGRESS","" ,update_run_id,'melissa_clean_update'),(source_table, "DELETE", start_time, None,"IN_PROGRESS","" ,delete_run_id, 'melissa_clean_delete'),(source_table, "CLEAN", start_time, None,"IN_PROGRESS","" ,clean_run_id, 'melissa_clean_clean')]
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
    display(audit_df)
    audit_df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(audit_table)
    print(f"Audit record inserted for {source_table}")

# COMMAND ----------

from pyspark.sql.functions import col

# Load the tables into DataFrames
billing_df = spark.read.table(f"{catalog}.clean.melissa_output_billing_final")
shipping_df = spark.read.table(f"{catalog}.clean.melissa_output_shipping_final")

# Perform an INNER JOIN on the common column 'customer_id'
combined_df = billing_df.join(shipping_df, on=["sid_id","name_clean","hash_key"],how="left")



# COMMAND ----------

# Define columns to backfill
columns_to_fill = ["input_shipping_street_clean", "input_shipping_city_clean", "input_shipping_state_clean", "input_shipping_postal_code_clean", "input_shipping_country_clean", "validated_shipping_street_clean", "validated_shipping_city_clean", "validated_shipping_state_clean", "validated_shipping_postal_code_clean", "validated_shipping_country_clean","validated_shipping_full_address_clean", "validated_verification_level_shipping"]


from pyspark.sql.functions import col, when

# Define a predefined mapping for backfill (Shipping → Billing)
backfill_mapping = {
    "input_shipping_street_clean":"input_billing_street_clean",
    "input_shipping_city_clean":"input_billing_city_clean",
    "input_shipping_state_clean":"input_billing_state_clean",
    "input_shipping_postal_code_clean":"input_billing_postal_code_clean",
    "input_shipping_country_clean":"input_billing_country_clean",
    "validated_shipping_street_clean":"validated_billing_street_clean",
    "validated_shipping_city_clean":"validated_billing_city_clean",
    "validated_shipping_state_clean":"validated_billing_state_clean",
    "validated_shipping_postal_code_clean":"validated_billing_postal_code_clean",
    "validated_shipping_country_clean":"validated_billing_country_clean",
    "validated_shipping_full_address_clean":"validated_billing_full_address_clean",
    "validated_verification_level_shipping":"validated_verification_level_billing"
   }


from pyspark.sql.functions import col, when, trim

# Apply backfill based on the predefined mapping
for ship_col, bill_col in backfill_mapping.items():
    combined_df = combined_df.withColumn(
        ship_col, 
        when(col(ship_col).isNull() | (trim(col(ship_col)) == ""), col(bill_col))  # Use mapped billing column if shipping is NULL, empty, or blank
        .otherwise(col(ship_col))
    )


# Show the final DataFrame
display(combined_df)



# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import col, current_timestamp
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

insert_run_id  =  str(datetime.now().strftime('%Y%m%d%H%M')) + '-' + 'melissa_clean_insert'
update_run_id  =  str(datetime.now().strftime('%Y%m%d%H%M')) + '-' + 'melissa_clean_update'
delete_run_id  =  str(datetime.now().strftime('%Y%m%d%H%M')) + '-' + 'melissa_clean_delete'
clean_run_id   =  str(datetime.now().strftime('%Y%m%d%H%M')) + '-' + 'melissa_clean_clean'

# Define target table name
target_table_name = f"{catalog}.clean.melissa_api_final"

# Try to get the Delta table reference (Only execute merge if the table exists)
try:
    delta_target = DeltaTable.forName(spark, target_table_name)
    source_audit_table = f'{catalog}.clean.melissa_api_final'
    dest_audit_table = f'{catalog}.landing.audit_logs'
    audit_df = spark.read.table(dest_audit_table)
    audit_tracking_melissa_clean(insert_run_id,update_run_id,delete_run_id,clean_run_id)

    columns_to_check = [col_name for col_name in combined_df.columns if col_name != "sid_id"]
    condition = None
    for col_name in columns_to_check:
        new_condition = col(f"tgt.{col_name}") != col(f"src.{col_name}")
        condition = new_condition if condition is None else condition | new_condition

    update_set = {
        col_name: col(f"src.{col_name}") for col_name in combined_df.columns
    }

    update_set["date_of_cleaning"] = when(
    condition,  # Dynamically generated condition
    current_timestamp()  # Assign current date only to updated records
    ).otherwise(col("tgt.date_of_cleaning"))  # Retain old date for unchanged records

    #update_set["date_of_cleaning"] = current_timestamp()  # Update timestamp only for updated records

    insert_values = {
        col_name: col(f"src.{col_name}") for col_name in combined_df.columns
    }
    insert_values["date_of_cleaning"] = current_timestamp()  # Set timestamp for newly inserted records

    # Perform the merge (Insert or Update)
    delta_target.alias("tgt").merge(
        combined_df.alias("src"),
        "tgt.sid_id = src.sid_id"  # Replace 'sid_id' with the correct primary key column
    ).whenMatchedUpdate(
        set=update_set
    ).whenNotMatchedInsert(
        values=insert_values
    ).execute()

    print(f" Data successfully merged into {target_table_name}")

    logger.info(f"Processing completed for {target_table_name}\n")
    udf_send_email('Account Data Refresh', 'melissa data update success', '',catalog)

    # Audit Tracking 
    audit_df = spark.read.table(dest_audit_table)
    insert_start_time_row = audit_df.filter(col("run_id") == insert_run_id).select("start_time").collect()[0]
    insert_start_time = insert_start_time_row["start_time"]

    update_start_time_row = audit_df.filter(col("run_id") == update_run_id).select("start_time").collect()[0]
    update_start_time = update_start_time_row["start_time"]

    delete_start_time_row = audit_df.filter(col("run_id") == delete_run_id).select("start_time").collect()[0]
    delete_start_time = delete_start_time_row["start_time"]

    end_time = datetime.now()
    insert_run_time_value = (end_time - insert_start_time).total_seconds()
    update_run_time_value = (end_time - update_start_time).total_seconds()
    delete_run_time_value = (end_time - delete_start_time).total_seconds()

    history_df = DeltaTable.forName(spark, f"{source_audit_table}").history(5)

    if history_df.count() > 0:
        #operation_metrics = history_df.select("operationMetrics").collect()[0][0]
        operation_metrics = history_df.select("operationMetrics").filter(col('operation')=="MERGE").orderBy(col('version').desc()).limit(1).collect()[0][0]
        num_inserted = int(operation_metrics.get("numTargetRowsInserted") or 0)
        num_updated = int(operation_metrics.get("numTargetRowsUpdated") or 0)
        num_deleted = int(operation_metrics.get("numTargetRowsDeleted") or 0)

        insert_query = f"""MERGE INTO {dest_audit_table} src USING (SELECT '{insert_run_id}' AS run_id) dst ON src.run_id = dst.run_id
        WHEN MATCHED THEN UPDATE SET src.end_time = '{end_time}', src.status = 'SUCCESS', error_message = '', src.record_count = {num_inserted}, src.run_time = {insert_run_time_value}"""

        update_query = f"""MERGE INTO {dest_audit_table} src USING (SELECT '{update_run_id}' AS run_id) dst ON src.run_id = dst.run_id
        WHEN MATCHED THEN UPDATE SET src.end_time = '{end_time}', src.status = 'SUCCESS', error_message = '', src.record_count = {num_updated}, src.run_time = {update_run_time_value}"""

        delete_query = f"""MERGE INTO {dest_audit_table} src USING (SELECT '{delete_run_id}' AS run_id) dst ON src.run_id = dst.run_id
        WHEN MATCHED THEN UPDATE SET src.end_time = '{end_time}', src.status = 'NOT PERFORMED', error_message = '', src.record_count = {num_deleted}"""

        spark.sql(insert_query)
        spark.sql(update_query)
        spark.sql(delete_query)
        print('done')

except Exception as e:
    print(f" Target table {target_table_name} does not exist. Skipping merge operation.")
    logger.error(f"Error occurred while processing for {target_table_name} : {str(e)}")
    logging.shutdown()
    udf_send_email(f'Account Data Refresh','melissa data update failure',{str(e)},catalog)

    insert_start_time_row = audit_df.filter(col("run_id") == insert_run_id).select("start_time").collect()[0]
    insert_start_time = insert_start_time_row["start_time"]

    update_start_time_row = audit_df.filter(col("run_id") == update_run_id).select("start_time").collect()[0]
    update_start_time = update_start_time_row["start_time"]

    delete_start_time_row = audit_df.filter(col("run_id") == delete_run_id).select("start_time").collect()[0]
    delete_start_time = delete_start_time_row["start_time"]

    clean_start_time_row = audit_df.filter(col("run_id") == clean_run_id).select("start_time").collect()[0]
    clean_start_time = clean_start_time_row["start_time"]

    end_time = datetime.now()
    insert_run_time_value = (end_time - insert_start_time).total_seconds()
    update_run_time_value = (end_time - update_start_time).total_seconds()
    delete_run_time_value = (end_time - delete_start_time).total_seconds()
    clean_run_time_value  = (end_time - clean_start_time).total_seconds()

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

    spark.sql(insert_query)
    spark.sql(update_query)
    spark.sql(delete_query)
    spark.sql(clean_query)
    raise

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import col, when

# Load Delta Table
target_table_name = f"{catalog}.clean.melissa_api_final"
delta_table = DeltaTable.forName(spark, target_table_name)

# Define update_set with inline null-safe comparisons
update_set = {
    "flag_billing_demographic": when(
        (
            ((col("input_billing_city_clean") != col("validated_billing_city_clean")) |
             (col("input_billing_city_clean").isNull() & col("validated_billing_city_clean").isNotNull()) |
             (col("input_billing_city_clean").isNotNull() & col("validated_billing_city_clean").isNull()))
            &
            ((col("input_billing_postal_code_clean") != col("validated_billing_postal_code_clean")) |
             (col("input_billing_postal_code_clean").isNull() & col("validated_billing_postal_code_clean").isNotNull()) |
             (col("input_billing_postal_code_clean").isNotNull() & col("validated_billing_postal_code_clean").isNull()))
            &
            ((col("input_billing_street_clean") != col("validated_billing_street_clean")) |
             (col("input_billing_street_clean").isNull() & col("validated_billing_street_clean").isNotNull()) |
             (col("input_billing_street_clean").isNotNull() & col("validated_billing_street_clean").isNull()))
        ), 1
    ).otherwise(0),

    "flag_billing_state": when(
        (col("input_billing_state_clean") != col("validated_billing_state_clean")) |
        (col("input_billing_state_clean").isNull() & col("validated_billing_state_clean").isNotNull()) |
        (col("input_billing_state_clean").isNotNull() & col("validated_billing_state_clean").isNull()), 1
    ).otherwise(0),

    "flag_billing_country": when(
        (col("input_billing_country_clean") != col("validated_billing_country_clean")) |
        (col("input_billing_country_clean").isNull() & col("validated_billing_country_clean").isNotNull()) |
        (col("input_billing_country_clean").isNotNull() & col("validated_billing_country_clean").isNull()), 1
    ).otherwise(0),

    "flag_billing_null": when(
        (col("validated_billing_city_clean").isNull()) | (col("validated_billing_city_clean") == '') |
        (col("validated_billing_country_clean").isNull()) | (col("validated_billing_country_clean") == '') |
        (col("validated_billing_state_clean").isNull()) | (col("validated_billing_state_clean") == '') |
        (col("validated_billing_street_clean").isNull()) | (col("validated_billing_street_clean") == '') |
        (col("validated_billing_postal_code_clean").isNull()) | (col("validated_billing_postal_code_clean") == ''), 1
    ).otherwise(0),

    "flag_shipping_demographic": when(
        (
            ((col("input_shipping_city_clean") != col("validated_shipping_city_clean")) |
             (col("input_shipping_city_clean").isNull() & col("validated_shipping_city_clean").isNotNull()) |
             (col("input_shipping_city_clean").isNotNull() & col("validated_shipping_city_clean").isNull()))
            &
            ((col("input_shipping_postal_code_clean") != col("validated_shipping_postal_code_clean")) |
             (col("input_shipping_postal_code_clean").isNull() & col("validated_shipping_postal_code_clean").isNotNull()) |
             (col("input_shipping_postal_code_clean").isNotNull() & col("validated_shipping_postal_code_clean").isNull()))
            &
            ((col("input_shipping_street_clean") != col("validated_shipping_street_clean")) |
             (col("input_shipping_street_clean").isNull() & col("validated_shipping_street_clean").isNotNull()) |
             (col("input_shipping_street_clean").isNotNull() & col("validated_shipping_street_clean").isNull()))
        ), 1
    ).otherwise(0),

    "flag_shipping_state": when(
        (col("input_shipping_state_clean") != col("validated_shipping_state_clean")) |
        (col("input_shipping_state_clean").isNull() & col("validated_shipping_state_clean").isNotNull()) |
        (col("input_shipping_state_clean").isNotNull() & col("validated_shipping_state_clean").isNull()), 1
    ).otherwise(0),

    "flag_shipping_country": when(
        (col("input_shipping_country_clean") != col("validated_shipping_country_clean")) |
        (col("input_shipping_country_clean").isNull() & col("validated_shipping_country_clean").isNotNull()) |
        (col("input_shipping_country_clean").isNotNull() & col("validated_shipping_country_clean").isNull()), 1
    ).otherwise(0),

    "flag_shipping_null": when(
        (col("validated_shipping_city_clean").isNull()) | (col("validated_shipping_city_clean") == '') |
        (col("validated_shipping_country_clean").isNull()) | (col("validated_shipping_country_clean") == '') |
        (col("validated_shipping_state_clean").isNull()) | (col("validated_shipping_state_clean") == '') |
        (col("validated_shipping_street_clean").isNull()) | (col("validated_shipping_street_clean") == '') |
        (col("validated_shipping_postal_code_clean").isNull()) | (col("validated_shipping_postal_code_clean") == ''), 1
    ).otherwise(0),

}

# Run the update
delta_table.update(
    condition="TRUE",
    set=update_set
)

print("Table updated successfully.")


# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import col, when

# Load Delta Table
target_table_name = f"{catalog}.clean.melissa_api_final"
delta_table = DeltaTable.forName(spark, target_table_name)

update_set = {
"flag_revisit_billing": when(
        (col("flag_billing_demographic") == 1) |
        (col("flag_billing_state") == 1) |
        (col("flag_billing_country") == 1) |
        (col("flag_billing_null") == 1), 1
    ).otherwise(0),

    "flag_revisit_shipping": when(
        (col("flag_shipping_demographic") == 1) |
        (col("flag_shipping_state") == 1) |
        (col("flag_shipping_country") == 1) |
        (col("flag_shipping_null") == 1), 1
    ).otherwise(0),
    }


# Run the update
delta_table.update(
    condition="TRUE",
    set=update_set
)

print("Table updated successfully.")    

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean Flag update

# COMMAND ----------

def clean_audit_tracking(clean_run_id):
    dest_audit_table = f'{catalog}.landing.audit_logs'
    inter_table = f'{catalog}.clean.sf_account_clean_address'
    audit_df = spark.read.table(dest_audit_table)

    clean_start_time_row = audit_df.filter(col("run_id") == clean_run_id).select("start_time").collect()[0]
    clean_start_time = clean_start_time_row["start_time"]

    end_time = datetime.now()
    clean_run_time_value  = (end_time - clean_start_time).total_seconds()
    num_cleaned = spark.read.table(inter_table).filter(col("final_clean_flag") ==1).count()

    clean_query = f"""MERGE INTO {dest_audit_table} src USING (SELECT '{clean_run_id}' AS run_id) dst ON src.run_id = dst.run_id
    WHEN MATCHED THEN UPDATE SET src.end_time = '{end_time}', src.status = 'SUCCESS', error_message = '', src.record_count = {num_cleaned}, src.run_time = {clean_run_time_value}"""

    spark.sql(clean_query)
    print ('done')

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO ${catalog}.clean.sf_account_clean_address AS t
# MAGIC USING ${catalog}.clean.melissa_api_final  AS s
# MAGIC ON t.sid_id = s.sid_id
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE SET t.final_clean_flag = 1 

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE ${catalog}.clean.melissa_input
# MAGIC SET final_clean_flag = 1
# MAGIC WHERE sid_id IN (SELECT sid_id FROM ${catalog}.clean.sf_account_clean_address WHERE final_clean_flag = 1);

# COMMAND ----------

clean_audit_tracking(clean_run_id)

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import col

# Define target table name
df= spark.read.table(f"{catalog}.clean.melissa_api_final").filter(
    (col("flag_revisit_billing") == "1") | (col("flag_revisit_shipping") == "1") )
target_table_name = f"{catalog}.tmp.sf_account_melissa_review"

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

    print(f" Data successfully merged into {target_table_name}")

except Exception as e:
    logger.info(f"error: {str(e)}")
    logging.shutdown()


# COMMAND ----------

try:
    target_table_name = f"{catalog}.tmp.sf_account_melissa_review"
    target_df = spark.read.table(target_table_name)
    review_record_count = target_df.count()
    if review_record_count > 0:
        udf_send_email('Account Data Refresh','review success','',catalog,review_record_count,target_table_name)
    else:
        print("No records to review")
except Exception as e:
    udf_send_email('Account Data Refresh','review failure',{str(e)},catalog)
    print('into failure')

# COMMAND ----------

logging.shutdown()
