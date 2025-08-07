# Databricks notebook source
from pyspark.sql import SparkSession, SQLContext, DataFrame, Row
from pyspark.sql.functions import *
from delta.tables import *

# COMMAND ----------

def udf_update_watermark_values(df_watermark_info, table_name, dataset_id):
    try:
        def convert_column_name(name):
            return re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name).lower()

        watermark_dataset_json = {}
        watermark_dataset_json["watermark_id"] = dataset_id

        for i in range(1, 3):
            original_column = f"watermark_column_{i}"
            if df_watermark_info.filter(col(original_column).isNotNull()).count() > 0:
                column_name = convert_column_name(
                    df_watermark_info.select(original_column).first()[0]
                )

                max_value = spark.sql( f"SELECT MAX({column_name}) FROM {table_name}").first()[0]
                max_value = ( max_value.strftime("%Y-%m-%d %H:%M:%S") if max_value else None )

                watermark_dataset_json[f"watermark_column_{i}"] = original_column
                watermark_dataset_json[f"watermark_value_{i}"] = max_value

        return watermark_dataset_json
    except Exception as e:
        logger.error(f"Error occurred while generating watermark values : {str(e)}")
        raise
