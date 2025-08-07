# Databricks notebook source
# MAGIC %md
# MAGIC ## Import Libraries and Functions

# COMMAND ----------

# MAGIC %pip install pyyaml
# MAGIC %pip install yaql

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import col,lower,from_json
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col
import requests
import json
import yaml
import yaql

# COMMAND ----------

# Get the current notebook path
notebook_path = dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().get()
display(notebook_path)

# Derive the base path for the bundle
bundle_base_path = "/".join(notebook_path.split("/")[:-4])
display(bundle_base_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load YAML Configuration - email

# COMMAND ----------

# Construct the relative path to the target file
relative_path = f"/Workspace{bundle_base_path}/resources/metadata/emails.yaml"
display(relative_path)
data_source = yaml.load(open(relative_path, 'r'), Loader=yaml.Loader)
print(data_source)

# COMMAND ----------

processing_group_name = data_source['processing_group_name']
configured_emails = data_source['configured_emails']

rows = [
    Row(
        name=inner_dict.get('name'),
        description=inner_dict.get('description'),
        send_from=inner_dict.get('send_from'),
        send_to=inner_dict.get('send_to'),
        prod_send_to=inner_dict.get('prod_send_to'),
        subject=inner_dict.get('subject'),
        body=inner_dict.get('body'),
        send_email_flag=inner_dict.get('send_email_flag'),
        attachment_required_flag=inner_dict.get('attachment_required_flag'),
        processing_group_name=processing_group_name
    )
    for email in data_source['configured_emails']
    for outer_key, inner_dict in email.items()
]

# Convert filtered_data to a Spark DataFrame
schema = StructType([
    StructField("name", StringType(), True),
    StructField("description", StringType(), True),
    StructField("send_from", StringType(), True),
    StructField("send_to", StringType(), True),
    StructField("prod_send_to", StringType(), True),
    StructField("subject", StringType(), True),
    StructField("body", StringType(), True),
    StructField("send_email_flag", IntegerType(), True),
    StructField("attachment_required_flag", IntegerType(), True),
    StructField("processing_group_name", StringType(), True)
])
 
filtered_df = spark.createDataFrame(rows, schema)
display(filtered_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load YAML Configuration - Systems Variables

# COMMAND ----------

# Construct the relative path to the target file
relative_path = f"/Workspace{bundle_base_path}/resources/metadata/system_variables.yaml"
display(relative_path)
data_source = yaml.load(open(relative_path, 'r'), Loader=yaml.Loader)
print(data_source)

# COMMAND ----------

configured_system_variables_yaml = data_source['configured_system_variables_yaml']
rows = [
    Row(
        url=data.get("url"),
        access_key=data.get("access_key")
    )
    for system_variables in data_source['configured_system_variables_yaml']
    for key, data in system_variables.items()
]
# Convert filtered_data to a Spark DataFrame
schema = StructType([
    StructField("url", StringType(), True),
    StructField("access_key", StringType(), True)
])
sys_var_filtered_df = spark.createDataFrame(rows, schema)
display(sys_var_filtered_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Email Function

# COMMAND ----------

def udf_send_email(processing_group_name, notebook_status, error_message,catalog_name=None,review_record_count=None,target_table=None):
    notebook_path = (
            dbutils.notebook.entry_point.getDbutils()
            .notebook()
            .getContext()
            .notebookPath()
            .get()
        )
    notebook_name = notebook_path.split("/")[-1]

    df_config = filtered_df
    df_system_variables =sys_var_filtered_df

    notebook_status = notebook_status.lower()

    df_config = (
        df_config.filter((col("processing_group_name")==processing_group_name) &
                        (lower(col("name")).like(f"%{notebook_status}%")) &
                        (col("send_email_flag")==1)
                        ).collect()[0]
    )

    api_url = None
    secret_access_key = None
    error_message = error_message[:500] + "..." if len(error_message) > 500 else error_message   

    df_system_variables =sys_var_filtered_df
    df_api_config = (
        df_system_variables
        .select(
            col('url'),
            col('access_key')
        )
        )  
    # display(df_api_config)

    api_config_list = df_api_config.collect()
    from_email_id = df_config['send_from']

    if(catalog_name == None):
        catalog = 'mdm_dev'
    else:
        catalog = catalog_name
   
    if (catalog == 'mdm_dev'):
        esubject = 'rg-dwh-dev-02 | '
        api_url = api_config_list[0][0]
        secret_access_key = api_config_list[0][1]
        to_email_list = df_config['send_to'].split(';')
    elif(catalog == 'mdm_qa'):
        esubject = 'rg-dwh-qa-01 | '
        api_url = api_config_list[1][0]
        secret_access_key = api_config_list[1][1]
        to_email_list = df_config['send_to'].split(';')
    elif(catalog == 'mdm_prod'):
        esubject = 'rg-dwh-prod-01 | '
        api_url = api_config_list[2][0]
        secret_access_key = api_config_list[2][1]
        to_email_list = df_config['prod_send_to'].split(';')
    else:
        raise Exception("Catalog not found")

    email_subject = df_config['subject']
    email_body = df_config['body']
    to_email_dict = [ {"email": email_id} for email_id in to_email_list]
   
    if 'failure' in notebook_status:
        email_body = f"""<br>A Databricks notebook has failed. The details are below<br><br>
        <table stype="padding:4px">
            <tr><b>Notebook Name : </b></tr><br>
            <tr>{notebook_name}</tr><br><br>
            <tr><b>Error Message :</b></tr><br>
            <tr>{error_message}</tr><br>
        </table>
        """
    if 'review success' in notebook_status and review_record_count > 0:
        email_body = f"""
        <p>Hi Squad,</p>
        <p>
        <b>{review_record_count}</b> records in <b>{target_table}</b> table needs manual review
        </p>
        <p>Thank you</p>
        """
        email_subject = f"Action Required: Records Need Manual Review"

    if 'merge success' in notebook_status and review_record_count > 0:
        email_body = f"""
        <p>Hi Squad,</p>
        <p>
        <b>{review_record_count}</b> records merged into <b>{target_table}</b> table 
        </p>
        <p>Thank you</p>
        """
        email_subject = f"Manual Review Merge: Success"
        
    url = api_url
    access_key = config_db = dbutils.secrets.get(scope="key-vault-secret", key=secret_access_key)

    headers = {
        "Authorization": f"Bearer {access_key}",
        "Content-Type": "application/json"
    }

    data = {
        "personalizations": [
            {
                "to": to_email_dict,
                "subject": f"{esubject}{email_subject}"
            }
        ],
        "content": [
            {"type": "text/html", "value": f"{email_body}"}
        ],
        "from": {"email": f"{from_email_id}"}
    }

    response = requests.post(url, headers=headers, json=data)

    if response.status_code != 202:
        raise requests.HTTPError(f"Unexpected status code: {response.status_code}. Response content: {response.content}")
