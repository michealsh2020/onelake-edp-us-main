# Databricks notebook source
dbutils.widgets.text("source_catalog", "", "Catalog")
source_catalog = dbutils.widgets.get("source_catalog")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pre-validate

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM ${source_catalog}.clean.sf_account_clean_address WHERE sid_id IN (SELECT DISTINCT sid_id FROM ${source_catalog}.clean.sf_account_clean_address_reviewed WHERE needs_manual_review=true AND sid_id NOT IN (SELECT DISTINCT sid_id FROM ${source_catalog}.tmp.sf_account_clean_address_for_review WHERE needs_manual_review=true)) AND needs_manual_review = false;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Update address cleaned timestamp

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC MERGE INTO ${source_catalog}.clean.sf_account_clean_address AS target
# MAGIC USING (
# MAGIC     SELECT DISTINCT sid_id
# MAGIC     FROM ${source_catalog}.clean.sf_account_clean_address_reviewed
# MAGIC     WHERE needs_manual_review = true
# MAGIC     AND sid_id NOT IN (
# MAGIC         SELECT DISTINCT sid_id
# MAGIC         FROM ${source_catalog}.tmp.sf_account_clean_address_for_review
# MAGIC         WHERE needs_manual_review = true
# MAGIC     )
# MAGIC ) AS source
# MAGIC ON target.sid_id = source.sid_id
# MAGIC WHEN MATCHED AND target.needs_manual_review = false THEN
# MAGIC UPDATE SET address_last_cleaned_timestamp = current_timestamp()
