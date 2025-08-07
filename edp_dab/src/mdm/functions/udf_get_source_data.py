# Databricks notebook source
from pyspark.sql import SparkSession, SQLContext, DataFrame, Row
from pyspark.sql.functions import *
from delta.tables import *

# COMMAND ----------

def udf_get_source_data(source_table_name, primary_key_list, max_ingested_timestamp):
    # Read the Bronze table and store it as Dataframe
    try:
        source_df = source_table_name.filter(
            col("ingested_timestamp") >= max_ingested_timestamp
        )

        window = (
            Window.partitionBy(concat(*primary_key_list))
            .orderBy( col("ingested_timestamp").desc() )
        )

        source_df = (
            source_df.withColumn("rank", row_number().over(window))
            .filter(col("rank") == 1)
            .drop("rank")
        )

        return source_df

    except Exception as e:
        logger.error(f"Error reading the source table - {source_table_name}: {str(e)}")
        raise
