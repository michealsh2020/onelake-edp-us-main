# Databricks notebook source
# MAGIC %md ###Creating widgets for input parameters

# COMMAND ----------

dbutils.widgets.text("catalog", "", "Catalog")
dbutils.widgets.text("storage_credential_name", "", "storage_credential_name")
dbutils.widgets.text("storage_account", "", "storage account")
dbutils.widgets.text("workspace_src_directory", "", "workspace_src_directory")
dbutils.widgets.text("project_directory", "mdm", "project_directory")

# COMMAND ----------

# MAGIC %md ###Read the input parameters

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
storage_credential_name = dbutils.widgets.get("storage_credential_name")
storage_account = dbutils.widgets.get("storage_account") 
workspace_src_directory = dbutils.widgets.get("workspace_src_directory")
project_directory = dbutils.widgets.get("project_directory")

# COMMAND ----------

# MAGIC %md ###Creating External Location

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
if catalog == 'dev':
    source_catalog='dev-mdm'
    mdm_catalog = 'mdm_dev'
elif catalog == 'prod':
    source_catalog='prod-mdm'
    mdm_catalog = 'mdm_prod'
elif catalog == 'qa':
    source_catalog='qa-mdm'
    mdm_catalog = 'mdm_qa'
else:
    source_catalog='dev-mdm'
    mdm_catalog = 'mdm_dev'

# COMMAND ----------

query = f"""
CREATE EXTERNAL LOCATION IF NOT EXISTS `{source_catalog}-loc` URL 'abfss://mdm@{storage_account}.dfs.core.windows.net/'
    WITH (STORAGE CREDENTIAL  `{storage_credential_name}`);
"""

spark.sql(query)

# COMMAND ----------

# MAGIC %md ###Creating Catalog

# COMMAND ----------

query = f"""
CREATE CATALOG IF NOT EXISTS {mdm_catalog}
MANAGED LOCATION 'abfss://mdm@{storage_account}.dfs.core.windows.net/';
"""

spark.sql(query)

# COMMAND ----------

# MAGIC %md ###Creating Schemas

# COMMAND ----------

# MAGIC %md ###landing

# COMMAND ----------

query = f"""
CREATE SCHEMA IF NOT EXISTS {mdm_catalog}.landing 
MANAGED LOCATION 'abfss://mdm@{storage_account}.dfs.core.windows.net/landing/';
"""

spark.sql(query)

# COMMAND ----------

# MAGIC %md ###clean

# COMMAND ----------

query = f"""
CREATE SCHEMA IF NOT EXISTS {mdm_catalog}.clean 
MANAGED LOCATION 'abfss://mdm@{storage_account}.dfs.core.windows.net/clean/';
"""

spark.sql(query)

# COMMAND ----------

# MAGIC %md ###enriched

# COMMAND ----------

query = f"""
CREATE SCHEMA IF NOT EXISTS {mdm_catalog}.enriched
MANAGED LOCATION 'abfss://mdm@{storage_account}.dfs.core.windows.net/enriched/';
"""

spark.sql(query)


# COMMAND ----------

# MAGIC %md ###tmp

# COMMAND ----------

query = f"""
CREATE SCHEMA IF NOT EXISTS {mdm_catalog}.tmp
MANAGED LOCATION 'abfss://mdm@{storage_account}.dfs.core.windows.net/tmp/';
"""

spark.sql(query)


# COMMAND ----------

# MAGIC %md ###creation of external volumes

# COMMAND ----------

query = f"""
CREATE EXTERNAL VOLUME IF NOT EXISTS {mdm_catalog}.landing.landing_logs
LOCATION 'abfss://mdm@{storage_account}.dfs.core.windows.net/landing/logs';
"""

spark.sql(query)

# COMMAND ----------

query = f"""
CREATE EXTERNAL VOLUME IF NOT EXISTS {mdm_catalog}.clean.clean_logs
LOCATION 'abfss://mdm@{storage_account}.dfs.core.windows.net/clean/logs';
"""

spark.sql(query)

# COMMAND ----------

query = f"""
CREATE EXTERNAL VOLUME IF NOT EXISTS {mdm_catalog}.enriched.enriched_logs
LOCATION 'abfss://mdm@{storage_account}.dfs.core.windows.net/enriched/logs';
"""

spark.sql(query)

# COMMAND ----------

query = f"""
CREATE EXTERNAL VOLUME IF NOT EXISTS {mdm_catalog}.tmp.tmp_logs
LOCATION 'abfss://mdm@{storage_account}.dfs.core.windows.net/tmp/logs';
"""

spark.sql(query)

# COMMAND ----------

# MAGIC %md ###dropping tables

# COMMAND ----------

# dbutils.notebook.run(
#    f"{workspace_src_directory}/{project_directory}/scripts/drop_salesforce_account_tables",
#    timeout_seconds=0,
#    arguments={
#        "catalog": mdm_catalog
#    },
# )

# COMMAND ----------

# MAGIC %md ###Creating salesforce Tables

# COMMAND ----------

dbutils.notebook.run(
    f"{workspace_src_directory}/{project_directory}/scripts/create_salesforce_account_tables",
    timeout_seconds=0,
    arguments={
        "catalog": mdm_catalog,
        "storage_account": storage_account
    },
)
