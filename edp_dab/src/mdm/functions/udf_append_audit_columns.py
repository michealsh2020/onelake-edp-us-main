# Databricks notebook source
from pyspark.sql import SparkSession, SQLContext, DataFrame, Row
from pyspark.sql.functions import *
from pyspark.sql.types import StringType

# COMMAND ----------

def udf_append_audit_columns(df: DataFrame, table_name: str,include_extended: bool = False) -> DataFrame:
    try:
        # Add audit columns to the DataFrame 
        '''
        -> include_extended==True means we need to add additional columns record_expiry_period,record_effective_date,record_expiry_date,record_current_flag in clean.sf_account table
        -> By default include_extended==False, means we don't need to add additional columns i.e., record_expiry_period,record_effective_date,record_expiry_date,record_current_flag in landing.sf_account table
        '''
        # Base audit columns
        df_audit = (
            # add all base audit fields
            df.withColumn("ingested_period", date_format(current_date(), "yyyyMM"))
            .withColumn("ingested_date", current_date())
            .withColumn("ingested_timestamp", current_timestamp())
            .withColumn("hash_key", sha2(concat_ws("", *df.columns), 256))
        )
        if include_extended:
            # add additional fields
            df_audit = (
            df_audit.withColumn("record_expiry_period", lit(None))
            .withColumn("record_effective_date", current_timestamp())
            .withColumn("record_expiry_date", lit(None))
            .withColumn("record_current_flag", lit(1))
        )

        # Log success message
        logger.info(f"{table_name} - Audit columns added successfully")

        # return the dataframe with new columns
        return df_audit

    except Exception as e:
        # Log error message and raise the exception
        logger.error( f"{table_name} - Error occurred while adding audit columns: {str(e)}" )
        logging.shutdown()
        raise
