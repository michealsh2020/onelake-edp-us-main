# Databricks notebook source
dbutils.widgets.text("source_catalog", "", "Catalog")
dbutils.widgets.text("update_type", "", "type")
source_catalog = dbutils.widgets.get("source_catalog")
update_type = dbutils.widgets.get("update_type")

# COMMAND ----------


# Define the SQL query to get max_dates
max_dates_query = """
WITH max_dates AS (
    SELECT CAST(max(created_date) AS TIMESTAMP) as max_created_date, 
           CAST(max(last_modified_date) AS TIMESTAMP) as max_last_modified_date 
    FROM mdm_prod.clean.sf_account
)
SELECT * FROM max_dates
"""

# Execute the max_dates query and get the result
max_dates_df = spark.sql(max_dates_query)
max_dates = max_dates_df.collect()[0]

# Define the update and insert queries
update_query = f"""
UPDATE {source_catalog}.landing.watermark_config
SET watermark_value_1 = '{max_dates.max_created_date}',
    watermark_value_2 = '{max_dates.max_last_modified_date}',
    record_updated_by = CURRENT_USER(),
    record_updated_date = CURRENT_TIMESTAMP()
WHERE watermark_id = 1
"""

insert_query = f"""
INSERT INTO {source_catalog}.landing.watermark_config(watermark_id, watermark_column_1, watermark_value_1, watermark_column_2, watermark_value_2, record_created_by, record_created_date, record_updated_by, record_updated_date)
VALUES (1, 'created_date', '{max_dates.max_created_date}', 'last_modified_date', '{max_dates.max_last_modified_date}', 'adminds2', CURRENT_TIMESTAMP(), 'adminds2', CURRENT_TIMESTAMP())
"""

# Execute the appropriate query based on update_type
if update_type == 'update':
    spark.sql(update_query)
else:
    spark.sql(insert_query)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM ${source_catalog}.landing.watermark_config;
