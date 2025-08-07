# Databricks notebook source
from pyspark.sql import SparkSession, SQLContext, DataFrame, Row
from pyspark.sql.functions import *
from delta.tables import *

# COMMAND ----------

def udf_log_clean_operation_metrics(clean_table):
    try:
        history_df = DeltaTable.forName(spark, f"{clean_table}").history(1)

        if history_df.count() > 0:
            operation_metrics = history_df.select("operationMetrics").collect()[0][0]
            num_inserted = operation_metrics.get("numTargetRowsInserted")
            num_updated = operation_metrics.get("numTargetRowsUpdated")
            num_deleted = operation_metrics.get("numTargetRowsDeleted")
            num_input_rows = operation_metrics.get("numSourceRows")

            # Log operation metrics
            logger.info(f"Number of rows inserted in {clean_table}: {num_inserted}")
            logger.info(f"Number of rows updated in {clean_table}: {num_updated}")
            logger.info(f"Number of rows deleted in {clean_table}: {num_deleted}")
            logger.info(f"Number of input rows from source: {num_input_rows}")

        else:
            raise ValueError(f"No history found for the DeltaTable {clean_table}")

    except Exception as e:
        logger.error(f"Error occurred while retrieving operation metrics for {clean_table}: {str(e)}")
        raise
