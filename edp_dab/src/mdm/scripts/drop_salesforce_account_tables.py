# Databricks notebook source
# MAGIC %md #Creating widgets for input parameters

# COMMAND ----------

dbutils.widgets.text("catalog", "", "Catalog")

# COMMAND ----------

# MAGIC %md #Read the input parameters

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

# MAGIC %md #Drop Tables

# COMMAND ----------

# MAGIC %md ##Salesforce Tables

# COMMAND ----------

def drop_tables(catalog, schema, table_name=None):
    if table_name:
        # Drop a specific table
        query = f"DROP TABLE IF EXISTS {catalog}.{schema}.{table_name}"
        spark.sql(query)
        print(f"Table '{table_name}' dropped from schema '{schema}'")
    else:
        tables_info_query = """
        SELECT 
            table_catalog,
            table_schema, 
            table_name
        FROM {}.information_schema.tables
        WHERE table_schema = '{}'
        ORDER BY
            table_name
        """.format(catalog, schema)

        df_tables_info = spark.sql(tables_info_query).collect()

        for row in df_tables_info:
            query = "DROP TABLE IF EXISTS {0}.{1}.{2}".format(row['table_catalog'], row['table_schema'], row['table_name'])
            spark.sql(query)
            print(f"{row['table_name']} dropped from {row['table_schema']}")

# COMMAND ----------

drop_tables(catalog, 'landing')

# COMMAND ----------

drop_tables(catalog, 'clean')

# COMMAND ----------

drop_tables(catalog, 'tmp')
