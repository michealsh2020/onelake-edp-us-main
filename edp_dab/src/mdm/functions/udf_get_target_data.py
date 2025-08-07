# Databricks notebook source
from pyspark.sql import SparkSession, SQLContext, DataFrame, Row
from pyspark.sql.functions import *
from delta.tables import *

# COMMAND ----------

def udf_get_target_data(target_table_name: str) -> DataFrame:
    # Read the Silver table and convert the Delta table to Dataframe
    try:
        target_df = (
            spark.read.table(target_table_name)
                 .filter( col("record_current_flag") == 1 )
        )

        return target_df

    except Exception as e:
        logger.error(f"Error reading the source table - {target_table_name}: {str(e)}")
        raise
