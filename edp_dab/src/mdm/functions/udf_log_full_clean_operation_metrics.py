# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from delta.tables import *

# COMMAND ----------

def udf_log_clean_operation_metrics(clean_table_name:str):
    try:
        stats = (
            DeltaTable.forName(spark, clean_table_name)
            .history(1)
            .select("operationMetrics")
            .collect()[0][0]
        )

        if "numOutputRows" in stats:
            inserted_record_count = stats["numOutputRows"]
            logger.info( f"Number of rows inserted in {clean_table_name} : {inserted_record_count}" )

        else:
            raise ValueError("No history found for the DeltaTable")

    except Exception as e:
        logger.error(f"Error occurred while retrieving operation metrics for {clean_table_name}: {str(e)}")
        logging.shutdown()
        raise
