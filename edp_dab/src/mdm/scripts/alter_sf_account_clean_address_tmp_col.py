# Databricks notebook source
dbutils.widgets.text("source_catalog", "", "Catalog")
source_catalog = dbutils.widgets.get("source_catalog")

# COMMAND ----------

# Add the Manual review auditcolumns(review_status,reviewed_by,review_date) into tmp.sf_account_clean_address_for_review table

%sql
ALTER TABLE ${source_catalog}.tmp.sf_account_clean_address_for_review
ADD COLUMN review_status STRING,reviewed_by STRING,reviewed_date DATE;
