# Databricks notebook source
# MAGIC %md #Creating widgets for input parameters

# COMMAND ----------

dbutils.widgets.text("catalog", "", "Catalog")
dbutils.widgets.text("storage_credential_name", "", "storage_credential_name")
dbutils.widgets.text("storage_account", "", "storage account")
dbutils.widgets.text("workspace_src_directory", "", "workspace_src_directory")
dbutils.widgets.text("project_directory", "", "project_directory")

# COMMAND ----------

# MAGIC %md #Read the input parameters

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
storage_credential_name = dbutils.widgets.get("storage_credential_name")
storage_account = dbutils.widgets.get("storage_account")
workspace_src_directory = dbutils.widgets.get("workspace_src_directory")
project_directory = dbutils.widgets.get("project_directory")

# COMMAND ----------

# MAGIC %md #Creating External Location

# COMMAND ----------

query = f"""
CREATE EXTERNAL LOCATION IF NOT EXISTS `{catalog}-loc` URL 'abfss://onelake@{storage_account}.dfs.core.windows.net/'
    WITH (STORAGE CREDENTIAL  `{storage_credential_name}`);
"""

spark.sql(query)

# COMMAND ----------

# MAGIC %md #Creating Catalog

# COMMAND ----------

query = f"""
CREATE CATALOG IF NOT EXISTS {catalog}
MANAGED LOCATION 'abfss://onelake@{storage_account}.dfs.core.windows.net/';
"""

spark.sql(query)

# COMMAND ----------

# MAGIC %md #Creating Schemas

# COMMAND ----------

# MAGIC %md ##Admin

# COMMAND ----------

query = f"""
CREATE SCHEMA IF NOT EXISTS {catalog}.admin 
MANAGED LOCATION 'abfss://onelake@{storage_account}.dfs.core.windows.net/bronze/';
"""

spark.sql(query)

# COMMAND ----------

# MAGIC %md ##Bronze Layer

# COMMAND ----------

query = f"""
CREATE SCHEMA IF NOT EXISTS {catalog}.bronze 
MANAGED LOCATION 'abfss://onelake@{storage_account}.dfs.core.windows.net/bronze/';
"""

spark.sql(query)

# COMMAND ----------

# MAGIC %md ##Silver Layer

# COMMAND ----------

query = f"""
CREATE SCHEMA IF NOT EXISTS {catalog}.silver
MANAGED LOCATION 'abfss://onelake@{storage_account}.dfs.core.windows.net/bronze/';
"""

spark.sql(query)

# COMMAND ----------

# MAGIC %md ##Gold Layer

# COMMAND ----------

query = f"""
CREATE SCHEMA IF NOT EXISTS {catalog}.gold
MANAGED LOCATION 'abfss://onelake@{storage_account}.dfs.core.windows.net/bronze/';
"""

spark.sql(query)

# COMMAND ----------

# MAGIC %md #Creating Volumes

# COMMAND ----------

# MAGIC %md ##Raw files

# COMMAND ----------

query = f"""
CREATE EXTERNAL VOLUME IF NOT EXISTS {catalog}.admin.raw
LOCATION 'abfss://onelake@{storage_account}.dfs.core.windows.net/raw';
"""

spark.sql(query)

# COMMAND ----------

# MAGIC %md ##Bronze Logs

# COMMAND ----------

query = f"""
CREATE EXTERNAL VOLUME IF NOT EXISTS {catalog}.admin.bronze_logs
LOCATION 'abfss://onelake@{storage_account}.dfs.core.windows.net/bronze/logs';
"""

spark.sql(query)

# COMMAND ----------

# MAGIC %md ##Silver Logs

# COMMAND ----------

query = f"""
CREATE EXTERNAL VOLUME IF NOT EXISTS {catalog}.admin.silver_logs
LOCATION 'abfss://onelake@{storage_account}.dfs.core.windows.net/silver/logs';
"""

spark.sql(query)


# COMMAND ----------

# MAGIC %md ##Gold Logs

# COMMAND ----------

query = f"""
CREATE EXTERNAL VOLUME IF NOT EXISTS {catalog}.admin.gold_logs
LOCATION 'abfss://onelake@{storage_account}.dfs.core.windows.net/gold/logs';
"""

spark.sql(query)


# COMMAND ----------

# MAGIC %md ##ML Model -Obs

# COMMAND ----------

query = f"""
CREATE EXTERNAL VOLUME IF NOT EXISTS {catalog}.admin.model_data
LOCATION 'abfss://onelake@{storage_account}.dfs.core.windows.net/gold/model_data';
"""
 
spark.sql(query)

# # COMMAND ----------

# # MAGIC %md
# # MAGIC ## ML Model

# # COMMAND ----------

# query = f"""
# CREATE EXTERNAL VOLUME IF NOT EXISTS {catalog}.admin.LeadScoringModel
# LOCATION 'abfss://onelake@{storage_account}.dfs.core.windows.net/raw/LeadScoringModel';
# """
 
# spark.sql(query)

# COMMAND ----------

# MAGIC %md #Droping Lead Scoring Tables

# COMMAND ----------

#dbutils.notebook.run(
#    f"{workspace_src_directory}/{project_directory}/scripts/drop_lead_scoring_tables",
#    timeout_seconds=0,
#    arguments={
#        "catalog": catalog
#    },
#)

# COMMAND ----------

# MAGIC %md #Creating Lead Scoring Tables

# COMMAND ----------

if dbutils.notebook.run(
    f"{workspace_src_directory}/{project_directory}/scripts/create_tables",
    timeout_seconds=0,
    arguments={
        "catalog": catalog,
        "storage_account": storage_account
    },
)
