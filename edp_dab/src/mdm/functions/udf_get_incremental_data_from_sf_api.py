# Databricks notebook source
# MAGIC %pip install simple_salesforce
# MAGIC %pip install pyyaml

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import libraries and functions

# COMMAND ----------

import requests
import json
from simple_salesforce import Salesforce
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType, TimestampType, IntegerType,DataType
from pyspark.sql.functions import udf,col
import yaml

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create spark session

# COMMAND ----------

# Initialize Spark session
spark = SparkSession.builder.appName("Salesforce to Databricks").getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load YAML Configuration

# COMMAND ----------

def load_config(file_path):
    with open(file_path, 'r') as file:
        config = yaml.safe_load(file)
    return config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Connect to Salesforce

# COMMAND ----------

def connect_to_salesforce(system_config,env):    
    salesforce_username=dbutils.secrets.get("key-vault-secret", system_config['environments'][env]['variables']['kv_username'])
    salesforce_db_pwd=dbutils.secrets.get("key-vault-secret", system_config['environments'][env]['variables']['kv_password'])
    salesforce_db_token=dbutils.secrets.get("key-vault-secret", system_config['environments'][env]['variables']['kv_token'])
    salesforce_url = dbutils.secrets.get("key-vault-secret", system_config['environments'][env]['variables']['kv_url'])
    
    sf = Salesforce(username=salesforce_username, password=salesforce_db_pwd, security_token=salesforce_db_token, instance_url=salesforce_url)
    return sf

# COMMAND ----------

# MAGIC %md
# MAGIC ## Querying data from Salesforce

# COMMAND ----------

def query_salesforce_data(sf,config,schema,load_type,created_date,modified_date):
  records = []
  object_name = config['dataset']['dataset_name']
  columns = ",".join([col['name'] for col in config['dataset']['columns']])

  if load_type == "incremental":
      query = f"SELECT {columns} FROM {object_name} WHERE ( CreatedDate >= {created_date} OR LastModifiedDate >= {modified_date})"
  else:
      query = f"SELECT {columns} FROM {object_name}"
  
  result = sf.query_all(query)
  records.extend(result.get("records",[]))
  #records = result["records"]

  # Convert string timestamps to actual timestamp objects
  for record in records:
    for key in ['CreatedDate', 'LastActivityDate', 'LastModifiedDate', 'Billing_Address_Validation_Timestamp__c', 'SystemModstamp']:
        if key in record and record[key]:
            try:
                record[key] = datetime.strptime(record[key], '%Y-%m-%dT%H:%M:%S.%f%z')
            except ValueError:
                record[key] = datetime.strptime(record[key], '%Y-%m-%d')
  
  # Convert float values to integers where necessary
  for record in records:
    for key in ['NumberOfEmployees', 'of_Opportunities__c', 'Nbr_of_Account_Team_Members__c', 'Won_Opportunities__c', 'No_Opportunities_Closed__c', 'DiscoverOrg_Employees__c', 'CountOfOpportunitiesOpen__c', 'Count_of_SumTotal_CSMs__c', 'Count_of_SumTotal_CSDs__c']:
        if key in record and record[key] is not None:
            record[key] = int(record[key])

  return records

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parse schema from YAML

# COMMAND ----------

def get_struct_schema(config):
    fields = []
    for column in config["dataset"]["columns"]:
        col_name = column["name"]
        col_type = column["type"]
        col_nullable = column["nullable"]
        if col_type == "StringType":
            data_type = StringType()
        elif col_type == "DoubleType":
            data_type = DoubleType()
        elif col_type == "BooleanType":
            data_type = BooleanType()
        elif col_type == "TimestampType":
            data_type = TimestampType()
        elif col_type == "IntegerType":
            data_type = IntegerType()
        else:
            raise ValueError(f"Unsupported data type: {col_type}")
        fields.append(StructField(col_name, data_type, col_nullable))
    return StructType(fields)

# COMMAND ----------

# MAGIC %md
# MAGIC ## To append the audit fields

# COMMAND ----------

# MAGIC %run "../functions/udf_append_audit_columns"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load data into Delta table

# COMMAND ----------

def load_data_to_delta(records,dataset_config,config_path,schema,catalog):

  catalog_name= catalog
  schema_name = dataset_config['dataset']['sink']['schema']
  new_table_name = dataset_config['dataset']['sink']['table']
  databricks_config = load_config(f'{config_path}/configurations/databricks.yaml')
  chunk_size = (databricks_config['configuration']['chunk_size'])
  full_table_name = f"{catalog_name}.{schema_name}.{new_table_name}"

  for i in range(0, len(records), chunk_size):
      chunk = records[i:i + chunk_size]
      spark_df = spark.createDataFrame(chunk,schema= schema)

      # Add the audit columns to the source dataframe
      spark_df =udf_append_audit_columns(spark_df, full_table_name)
    
      if i == 0:
        spark_df.write.format("delta").option("mergeSchema", "true").mode("overwrite").saveAsTable(full_table_name)
      else:
        spark_df.write.format("delta").option("mergeSchema", "true").mode("append").saveAsTable(full_table_name)
  
  print(f"Data loaded to Delta table: {full_table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Main Function

# COMMAND ----------

def udf_get_incremental_data_from_sf_api(config_path,dataset_config_path,catalog,load_type):
    try:
        #loading account.yaml
        dataset_config = load_config(f'{config_path}/datasets/{dataset_config_path}')
        display(dataset_config)

        # loading salesforce.yaml
        source_system = dataset_config['dataset']['source_system'] 
        system_config = load_config(f'{config_path}/systems/{source_system}.yaml')
        display(system_config)  
        
        watermark_id = dataset_config['dataset']['watermark_id']
        display(watermark_id)

        #Get Latest watermark value
        watermark_df = spark.read.table(f'{catalog}.landing.watermark_config')
        watermark_df = watermark_df.filter(col('watermark_id')==watermark_id)
        display(watermark_df)

        createddate = watermark_df.select('watermark_value_1').collect()[0]['watermark_value_1']
        modifieddate = watermark_df.select('watermark_value_2').collect()[0]['watermark_value_2']
        createmilliseconds = createddate.microsecond // 1000
        modifymilliseconds = modifieddate.microsecond // 1000
        created_date = createddate.strftime('%Y-%m-%dT%H:%M:%S')+ f'.{createmilliseconds:03d}Z'
        modified_date = modifieddate.strftime('%Y-%m-%dT%H:%M:%S')+ f'.{modifymilliseconds:03d}Z'
        display(created_date)
        display(modified_date)
    
        # Connect to Salesforce
        env = catalog.split('_')[-1] if '_' in catalog else catalog
        sf = connect_to_salesforce(system_config,env)
        #display("connected to Salesforce")

        #Get schema from YAML
        schema = get_struct_schema(dataset_config)
        #display("schema done")
    
        #Query data from Salesforce
        records = query_salesforce_data(sf,dataset_config,schema,load_type,created_date,modified_date)  
        #df = spark.createDataFrame(records,schema=schema)
        #display("records done")

        #Load data into Delta table        
        load_data_to_delta(records,dataset_config,config_path,schema,catalog)

        return "Success"

    except Exception as e:
        raise e
