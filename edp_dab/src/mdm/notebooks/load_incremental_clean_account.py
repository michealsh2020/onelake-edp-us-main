# Databricks notebook source
# MAGIC %pip install pyyaml

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set up widgets

# COMMAND ----------

dbutils.widgets.text("source_catalog", "", "Catalog")
dbutils.widgets.text("Configuration_Path", "", "Configuration Path")
dbutils.widgets.text("Source_Schema", "", "Source Schema")
dbutils.widgets.text("Target_Schema", "", "Target Schema")
dbutils.widgets.text("Table_Name", "", "Table Name")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read the input parameters

# COMMAND ----------

source_catalog = dbutils.widgets.get("source_catalog")
config_path = dbutils.widgets.get("Configuration_Path")
source_schema = dbutils.widgets.get("Source_Schema")
target_schema = dbutils.widgets.get("Target_Schema")
table_name = dbutils.widgets.get("Table_Name") 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import libraries and functions

# COMMAND ----------

from pyspark.sql import SparkSession, SQLContext, DataFrame, Row
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType, TimestampType, IntegerType,DataType
from pyspark.sql.functions import *
from delta.tables import *
from datetime import datetime
from pyspark.sql.window import Window
from pyspark.sql.functions import col, concat, row_number
import re
import json
import os
import logging
import yaml

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create spark session

# COMMAND ----------

spark = SparkSession.builder.appName("Clean Account Processing").getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Configuration

# COMMAND ----------

# MAGIC %run "../functions/udf_read_config"

# COMMAND ----------

# MAGIC %md
# MAGIC ## To update watermark

# COMMAND ----------

# MAGIC %run "../functions/udf_update_watermark_values"

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
# MAGIC ## To get source data

# COMMAND ----------

# MAGIC %run "../functions/udf_get_source_data"

# COMMAND ----------

# MAGIC %md
# MAGIC ## To get target data

# COMMAND ----------

# MAGIC %run "../functions/udf_get_target_data"

# COMMAND ----------

# MAGIC %md
# MAGIC ## To process incremental

# COMMAND ----------

# MAGIC %run "../functions/udf_process_incremental_data"

# COMMAND ----------

# MAGIC %md
# MAGIC ## To append the audit fields

# COMMAND ----------

# MAGIC %run "../functions/udf_append_audit_columns"

# COMMAND ----------

# MAGIC %md
# MAGIC ## To log record changes for clean table

# COMMAND ----------

# MAGIC %run "../functions/udf_log_clean_operation_metrics"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup Logger

# COMMAND ----------

# directory and file path for creating and storing the logs
log_directory = f"/Volumes/{source_catalog}/clean/clean_logs/{datetime.now().strftime('%Y%m%d')}"
log_file_path = f"{log_directory}/sf_account_{datetime.now().strftime('%Y%m%d%H%M%S')}.log"

# creating a logger to log the execution and errors
logger = udf_setup_logging(log_directory, log_file_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## To standardize the column names

# COMMAND ----------

def udf_standardize_salesforce_columns(df: DataFrame, table_name: str, rename_fields_dict: dict) -> DataFrame:
    try:

        logger.info(f"{table_name} - Salesforce columns standardized Started")
        column_name_mapping = {}

        # sort columns so that columns without '__c' suffix are processed first
        sorted_columns = sorted(
            df.columns, key=lambda x: (x.lower().endswith("__c"), x)
        )

        for original_column in sorted_columns:
            # rename id to sid
            standardized_column_name = rename_fields_dict[original_column] if original_column in rename_fields_dict.keys() else original_column

            # rename the fields as per naming convention
            standardized_column_name = (
                re.sub("([a-z])([A-Z])", r"\1_\2", standardized_column_name)
                .lower()
                .replace("[^a-z0-9_]", "")
            )

            column_name = re.sub("__c$", "", standardized_column_name)
            trimmed_column_name = re.sub("__", "_", column_name)

            # check if the column without '__c' already exists
            if trimmed_column_name in column_name_mapping.values():
                column_name_mapping[original_column] = standardized_column_name
            else:
                column_name_mapping[original_column] = trimmed_column_name

        # rename columns
        df = df.withColumnsRenamed(column_name_mapping)

        # Log success message
        logger.info(f"{table_name} - Salesforce columns standardized completed successfully")

        # return the dataframe with renamed column names
        return df

    except Exception as e:
        # Log error if standardization fails
        logger.error(f"{table_name} - Error occurred while standardizing Salesforce columns: {str(e)}")
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parse schema from YAML

# COMMAND ----------

def get_struct_schema(config):
    fields = []
    for column in config["dataset"]["columns"]:
        col_name = column["name"]
        col_type = column["type"]
        is_primary = column.get("is_primary_key")
        modified_column = column.get("modified_field_name")
        if col_type == "StringType":
            data_type = StringType()
        elif col_type == "DoubleType":
            data_type = DoubleType()
        elif col_type == "BooleanType":
            data_type = BooleanType()
        elif col_type == "TimestampType":
            data_type = TimestampType()
        elif col_type == "IntegerType":
            data_type = IntegerType()
        else:
            raise ValueError(f"Unsupported data type: {col_type}")
        fields.append(StructField(col_name, data_type, metadata={"is_primary": is_primary,"modified_field_name": modified_column}))

    return StructType(fields)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Audit Tracking

# COMMAND ----------

def audit_tracking_clean(insert_run_id,update_run_id,delete_run_id):
    source_table = f'{source_catalog}.clean.sf_account'
    audit_table = f'{source_catalog}.landing.audit_logs'
    start_time = datetime.now()
    insert_run_id  =  insert_run_id
    update_run_id  =  update_run_id
    delete_run_id  =  delete_run_id

    audit_data = [(source_table, "INSERT", start_time, None,"IN_PROGRESS","" ,insert_run_id,'clean_insert'),(source_table, "UPDATE", start_time, None,"IN_PROGRESS","" ,update_run_id,'clean_update'),(source_table, "DELETE", start_time, None,"IN_PROGRESS","" ,delete_run_id, 'clean_delete')]
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

# MAGIC %md
# MAGIC ##Clean Account Processing

# COMMAND ----------

from pyspark.sql.functions import col, unix_timestamp
from datetime import datetime

source_table =f"{source_catalog}.{source_schema}.{table_name}"
target_table =f"{source_catalog}.{target_schema}.{table_name}"

insert_run_id  =  str(datetime.now().strftime('%Y%m%d%H%M')) + '-' + 'clean_insert';
update_run_id  =  str(datetime.now().strftime('%Y%m%d%H%M')) + '-' + 'clean_update'
delete_run_id  =  str(datetime.now().strftime('%Y%m%d%H%M')) + '-' + 'clean_delete'

try:  
        #result = 'Success'
        source_audit_table = f'{source_catalog}.clean.sf_account'
        dest_audit_table = f'{source_catalog}.landing.audit_logs'
        audit_df = spark.read.table(dest_audit_table)

        display(insert_run_id)
        start_time = datetime.now(); 

        logger.info(f"Processing started for {target_table}")

        target_df = udf_get_target_data(target_table)

        # Get the maximum timestamp from clean table
        max_timestamp = target_df.agg({"ingested_timestamp": "max"}).collect()[0][0]
        max_timestamp = datetime(1970,1,1) if max_timestamp is None else max_timestamp

        display(max_timestamp)

        config = udf_read_config(config_path)
        watermark_id = config['dataset']['watermark_id']
        display(watermark_id)
        
        struct_schema = get_struct_schema(config)
        df_columns = [(field.name, field.metadata['modified_field_name'] if isinstance(field.metadata,dict) and 'modified_field_name'in field.metadata and field.metadata['modified_field_name'] is not None else field.name,field.metadata.get('is_primary',False) if isinstance(field.metadata,dict) else 0) for field in struct_schema.fields] 

        df_source_fields = spark.createDataFrame(df_columns).toDF('field_name','modified_field_name','is_primary')
        # display(df_source_fields)

        rename_fields_dict = {row["field_name"]:row["modified_field_name"] for row in df_source_fields.collect()}

        df_dataset_fields = df_source_fields.filter(col('is_primary') ==1) .select(col('modified_field_name').alias('field_name'))
        #display(df_dataset_fields)
 
        primary_key_list = df_dataset_fields.select(collect_list("field_name")).first()[0]
        #display(df_dataset_fields)

        source_df = spark.read.table(source_table)

        # Standardize Landing table column names
        source_df = udf_standardize_salesforce_columns(source_df, table_name,rename_fields_dict)

        source_df = udf_get_source_data(source_df, primary_key_list, max_timestamp)

        audit_tracking_clean(insert_run_id,update_run_id,delete_run_id)

        udf_process_incremental_data(source_df, target_df, primary_key_list,target_table)

        udf_log_clean_operation_metrics(target_table)

         # Audit Tracking 
        audit_df = spark.read.table(dest_audit_table)
        display(audit_df)    
        display(insert_run_id)  
        #spark.sql(f"REFRESH TABLE {dest_audit_table}")

        insert_start_time_row = audit_df.filter(col("run_id") == insert_run_id).select("start_time").collect()[0]
        display (insert_start_time_row)
        insert_start_time = insert_start_time_row["start_time"]
        display (insert_start_time)

        update_start_time_row = audit_df.filter(col("run_id") == update_run_id).select("start_time").collect()[0]
        update_start_time = update_start_time_row["start_time"]

        delete_start_time_row = audit_df.filter(col("run_id") == delete_run_id).select("start_time").collect()[0]
        delete_start_time = delete_start_time_row["start_time"]
        
        print ('into success')
        end_time = datetime.now()
        insert_run_time_value = (end_time - insert_start_time).total_seconds()
        update_run_time_value = (end_time - update_start_time).total_seconds()
        delete_run_time_value = (end_time - delete_start_time).total_seconds()

        history_df = DeltaTable.forName(spark, f"{target_table}").history(5)
        display(history_df.count())

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
            print ('done')

        logger.info(f"Watermark Values Update - Start \n") 

        df_watermark = spark.read.table(f"{source_catalog}.landing.watermark_config").select(
            "watermark_id",
            "watermark_column_1",
            "watermark_value_1",
            "watermark_column_2",
            "watermark_value_2",
        )
        watermark_info_list = []

        df_watermark_info = df_watermark.filter(col("watermark_id") == watermark_id)
        watermark_dataset_json = udf_update_watermark_values(df_watermark_info,target_table, watermark_id) if df_watermark_info.count()>0 else None

        if watermark_dataset_json is not None:
                watermark_info_list.append(watermark_dataset_json) 

        # Access specific values in the dictionary
        first_watermark_value = watermark_info_list[0]["watermark_value_1"]
        print(first_watermark_value)
        second_watermark_value = watermark_info_list[0]["watermark_value_2"]
        print(second_watermark_value)

        watermark_query = f'''update {source_catalog}.landing.watermark_config
                                set watermark_value_1 = '{first_watermark_value}',
	                            watermark_value_2 = '{second_watermark_value}',
                                record_updated_by = CURRENT_USER(),
                                record_updated_date = CURRENT_TIMESTAMP()
                            where watermark_id = '{watermark_id}'
                    '''
        spark.sql(watermark_query)
        logger.info(f"Watermark Values Update - Complete \n") 
        logger.info(f"Processing completed for {target_table}\n")
        udf_send_email('Account Data Refresh','clean success','',source_catalog)
except Exception as e:
    logger.error(f"Error occurred while processing for {target_table} : {str(e)}")
    logging.shutdown()
    udf_send_email(f'Account Data Refresh','clean failure',{str(e)},source_catalog)

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

    error_message = str(e).replace("'", "''")
    audit_df = spark.read.table(dest_audit_table)

    insert_query = f"""MERGE INTO {dest_audit_table} src USING (SELECT '{insert_run_id}' AS run_id) dst ON src.run_id = dst.run_id
    WHEN MATCHED THEN UPDATE SET src.end_time = '{end_time}', src.status = 'FAILURE', error_message = '{error_message}', src.run_time = {insert_run_time_value}"""

    update_query = f"""MERGE INTO {dest_audit_table} src USING (SELECT '{update_run_id}' AS run_id) dst ON src.run_id = dst.run_id
    WHEN MATCHED THEN UPDATE SET src.end_time = '{end_time}', src.status = 'FAILURE', error_message = '{error_message}', src.run_time = {update_run_time_value}"""

    delete_query = f"""MERGE INTO {dest_audit_table} src USING (SELECT '{delete_run_id}' AS run_id) dst ON src.run_id = dst.run_id
    WHEN MATCHED THEN UPDATE SET src.end_time = '{end_time}', src.status = 'NOT PERFORMED', error_message = '{error_message}', src.run_time = {delete_run_time_value}"""

    spark.sql(insert_query)
    spark.sql(update_query)
    spark.sql(delete_query)
    raise

# COMMAND ----------

watermark_json = {'watermark_info': watermark_info_list}
display(watermark_json)

# COMMAND ----------

logging.shutdown()
