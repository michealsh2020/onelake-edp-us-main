# Databricks notebook source
dbutils.widgets.text("catalog", "", "catalog")
catalog=dbutils.widgets.get("catalog")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO ${catalog}.clean.melissa_input AS b
# MAGIC USING ${catalog}.clean.sf_account_clean_address AS a
# MAGIC   ON b.sid_id = a.sid_id
# MAGIC  WHEN MATCHED AND b.hash_key = a.hash_key
# MAGIC  AND b.ingested_timestamp != a.ingested_timestamp
# MAGIC   THEN
# MAGIC   UPDATE SET b.ingested_timestamp = a.ingested_timestamp;
