# Databricks notebook source
# MAGIC %md
# MAGIC ## Set up widgets

# COMMAND ----------

dbutils.widgets.text("dataset_config_path", "", "Dataset Config Path")
dbutils.widgets.text("catalog", "", "catalog")
dbutils.widgets.text("Load_Type", "", "Load Type")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import Libraries and Functions

# COMMAND ----------

#import yaml
import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType, StringType, StructType, StructField, TimestampType
from datetime import datetime
from pyspark.sql import SparkSession, SQLContext, DataFrame, Row
from delta.tables import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create spark session

# COMMAND ----------

spark = SparkSession.builder.appName("Salesforce Account Processing").getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ## To set up logger

# COMMAND ----------

# MAGIC %run "../functions/udf_setup_logging"

# COMMAND ----------

# MAGIC %md
# MAGIC ## To send email

# COMMAND ----------

# MAGIC %run "../functions/udf_send_email"

# COMMAND ----------

# MAGIC %md
# MAGIC ## To append the audit fields

# COMMAND ----------

# MAGIC %run "../functions/udf_append_audit_columns"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Function to load sf data - incremental

# COMMAND ----------

# MAGIC %run "../functions/udf_get_incremental_data_from_sf_api"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get Input Parameters

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
dataset_config_path=dbutils.widgets.get("dataset_config_path")
load_type = dbutils.widgets.get("Load_Type")    

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup Logger

# COMMAND ----------

# directory and file path for creating and storing the logs
#log_directory = f"/landing/logs/Salesforce/{datetime.now().strftime('%Y%m%d')}"
log_directory = f"/Volumes/{catalog}/landing/landing_logs/{datetime.now().strftime('%Y%m%d')}"
log_file_path = f"{log_directory}/sf_account_{datetime.now().strftime('%Y%m%d%H%M%S')}.log"

# creating a logger to log the execution and errors
logger = udf_setup_logging(log_directory, log_file_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Audit Tracking

# COMMAND ----------

def audit_tracking_landing(run_id):
    source_table = f'{catalog}.landing.sf_account'
    audit_table = f'{catalog}.landing.audit_logs'
    start_time = datetime.now()
    run_id = run_id
    operation = 'INSERT'
    audit_data = [(source_table, operation, start_time, None,"IN_PROGRESS","" ,run_id,"landing_insert")]
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
    #audit_df = spark.createDataFrame(audit_data,["table_name","operation","start_time","end_time","status","error_message","run_id","task_name"])
    audit_df = spark.createDataFrame(audit_data, schema)
    display(audit_df)
    audit_df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(audit_table)
    print(f"Success Audit record inserted for {source_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Salesforce Table Load

# COMMAND ----------

from pyspark.sql.functions import col, unix_timestamp
from datetime import datetime

try:
    notebook_path = dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    bundle_target = notebook_path.split("/")[3]
    bundle_name = notebook_path.split("/")[4]
    yaml_config_path = f"/Workspace/edp/.bundle/{bundle_target}/{bundle_name}/files/resources/metadata"
    dataset_config_path =dbutils.widgets.get("dataset_config_path")
    source_table = f'{catalog}.landing.sf_account'
    audit_table = f'{catalog}.landing.audit_logs'
    run_id  =  str(datetime.now().strftime('%Y%m%d%H%M')) + '-' + 'landing_insert'
    audit_df = spark.read.table(audit_table)
    audit_tracking_landing(run_id)
    start_time_row = audit_df.filter(col("run_id") == run_id).select("start_time").collect()[0]
    start_time = start_time_row["start_time"]
    display(yaml_config_path)
    display(dataset_config_path)
    result = udf_get_incremental_data_from_sf_api(yaml_config_path,dataset_config_path,catalog,load_type)
   
    if result=='Success':
        udf_send_email('Account Data Refresh','success','',catalog)
        end_time = datetime.now()
        run_time_value = (end_time - start_time).total_seconds()
        insert_record_count = spark.read.table(source_table).count()
        update_query = f"""MERGE INTO {audit_table} src USING (SELECT '{run_id}' AS run_id) dst ON src.run_id = dst.run_id
        WHEN MATCHED THEN UPDATE SET src.end_time = '{end_time}', src.status = 'SUCCESS', error_message = '', src.record_count = {insert_record_count}, src.run_time = {run_time_value}"""
        spark.sql(update_query)
        print ('done')

except Exception as e:
    logger.error(f"Error while processing: {str(e)}")
    logging.shutdown()
    udf_send_email(f'Account Data Refresh','failure',{str(e)},catalog)
    end_time = datetime.now()
    run_time_value = (end_time - start_time).total_seconds()
    error_message = str(e).replace("'", "''")
    update_query = f""" MERGE INTO {audit_table} src USING (SELECT '{run_id}' AS run_id) dst ON src.run_id = dst.run_id WHEN MATCHED THEN UPDATE SET src.end_time = '{end_time}',src.status = 'FAILURE',error_message = '{error_message}', src.run_time = {run_time_value}"""
    spark.sql(update_query)
    raise

# COMMAND ----------

logging.shutdown()
