# Databricks notebook source
from pyspark.sql import SparkSession, SQLContext, DataFrame, Row
from pyspark.sql.functions import *
from delta.tables import *

# COMMAND ----------

def udf_process_incremental_data(source_df: DataFrame, target_df: DataFrame, primary_key_list:list,target_table):
    target_table  = target_table
    try:
    
        # Get the column names from source dataframe
        columns = source_df.columns

        source_key_fields = [col(f"src.{field_name}") for field_name in primary_key_list]
   
        target_key_fields = [col(f"tgt.{field_name}") for field_name in primary_key_list]
 

        # Prepare a dictionary having source column name as key and target column as values
        col_expr = {
            column: "src." + column
            for column in columns
            if column not in ["_rescued_data", "ingested_period", "ingested_date"]
        }

        # Column expression for Insert in Merge
        insert_no_match = {
            **col_expr,
            "record_effective_date": current_timestamp(),
            "record_expiry_date": lit(None),
            "record_current_flag": lit(1),
        }

        source_df.cache()
        target_df.cache()
 
        df_target_filtered = target_df.join(source_df.select(*primary_key_list), on=primary_key_list)
        
        df_target_filtered.cache()
       
        update_df = (
            source_df.alias("src")
                     .join(df_target_filtered.alias("tgt"), on=primary_key_list, how="inner")
                     .withColumn("source_merge_key", concat(*source_key_fields))      
                     .filter((col("src.hash_key") != col("tgt.hash_key")))
                     .select("source_merge_key", "src.*")        
        )       
        
        insert_df = (
            source_df.alias("src")    
                .join(df_target_filtered.alias("tgt"), on=primary_key_list, how = "left_outer")
                .withColumn("source_merge_key",lit(""))            
                .filter((col("src.hash_key") != col("tgt.hash_key")) | col("tgt.hash_key").isNull())
                .select("source_merge_key", "src.*")    
        )
        
        upsert_df = update_df.union(insert_df)
              
        source_df.unpersist()
        df_target_filtered.unpersist()
    
        target_table = DeltaTable.forName(spark, target_table)
        # Merge: Performing upsert on clean table using landing table
        (
            target_table.alias("tgt") 
            .merge(upsert_df.alias("src"), concat(*target_key_fields) == col("src.source_merge_key"))
            .whenMatchedUpdate(
                condition=f"tgt.record_current_flag = 1 AND src.hash_key <> tgt.hash_key",
                set={
                    "record_expiry_period" : date_format(current_date(),'yyyyMM'),
                    "record_expiry_date": current_timestamp(),
                    "record_current_flag": lit(0)
                }
            )
            .whenNotMatchedInsert(
                values=insert_no_match
            ).execute()
        )
 
    except Exception as e:
        logger.error(f"Error processing incremental data for - {target_table}: {str(e)}")
        raise
