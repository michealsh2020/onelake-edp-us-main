# Databricks notebook source
%pip install pyyaml

# COMMAND ----------

import pandas as pd
import json
import urllib.parse
import urllib.request
from datetime import datetime
import requests
import logging
import yaml

# COMMAND ----------

dbutils.widgets.text("catalog", "", "catalog")
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

from pyspark.sql.functions import col
df_shipping = spark.read.table(f"{catalog}.clean.melissa_input").filter(col("address_match_flag") == 1).filter(col("final_clean_flag") == 0)

# COMMAND ----------

# Check if the DataFrame is empty
if df_shipping.count() == 0:
    dbutils.notebook.exit("No Shipping records are Present.")

# COMMAND ----------

# MAGIC %run "../functions/udf_read_config"

# COMMAND ----------

# MAGIC %run "../functions/udf_setup_logging"

# COMMAND ----------

# MAGIC %run "../functions/udf_log_clean_operation_metrics"

# COMMAND ----------

# MAGIC %run "../functions/udf_send_email"

# COMMAND ----------

# directory and file path for creating and storing the logs
log_directory = f"/Volumes/{catalog}/clean/clean_logs/{datetime.now().strftime('%Y%m%d')}"
log_file_path = f"{log_directory}/melissa_output_shipping_final_{datetime.now().strftime('%Y%m%d%H%M%S')}.log"
# creating a logger to log the execution and errors
logger = udf_setup_logging(log_directory, log_file_path)

# COMMAND ----------

#   SHIPPING ADDRESSES

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
df1_shipping = spark.read.table(f"{catalog}.clean.melissa_input").select("sid_id",
"name_clean",
"shipping_street_clean",
"shipping_city_clean",
"shipping_state_clean",
"shipping_country_clean",
"shipping_postal_code_clean",
"address_match_flag","hash_key").filter(col("address_match_flag") == 1).filter(col("final_clean_flag") == 0)
display(df1_shipping)

# Check if DataFrame is empty

if df1_shipping.isEmpty():
    # Define Schema (match original columns)
    schema = StructType([
        StructField("sid_id", StringType(), True),
        StructField("name_clean", StringType(), True),
        StructField("shipping_street_clean", StringType(), True),
        StructField("shipping_city_clean", StringType(), True),
        StructField("shipping_state_clean", StringType(), True),
        StructField("shipping_country_clean", StringType(), True),
        StructField("shipping_postal_code_clean", StringType(), True),
        StructField("address_match_flag", IntegerType(), True),
        StructField("hash_key", StringType(), True),
    ])
    
# Create an empty DataFrame
    df1_shipping = spark.createDataFrame([], schema)

# COMMAND ----------

license_key = dbutils.secrets.get(scope="key-vault-secret", key='melissadata-global-address-license-key')
url = dbutils.secrets.get(scope="key-vault-secret", key='melissadata-global-address-url')

# COMMAND ----------

import requests
import pandas as pd
from pyspark.sql.types import StructType, StructField, StringType

# List of address columns
address_cols_shipping = ['shipping_street_clean']
country_cols_shipping = ['shipping_country_clean']

# List of all columns to fetch
columns = ['sid_id','name_clean', 'shipping_street_clean', 'shipping_city_clean', 'shipping_state_clean','shipping_country_clean', 'shipping_postal_code_clean','hash_key']

# Collect all rows and convert them into a list of dictionaries
input_data_shipping = [row.asDict() for row in df1_shipping.select(*columns).collect()]

valid_results_shipping = []
invalid_results_shipping = []
skipped_records_shipping = []  # New list for skipped records

# Iterate over rows in the DataFrame
for index, record in enumerate(input_data_shipping):
    try:
        # Skip records where 'Id' is missing or empty
        if not record.get('sid_id'):
            print(f"Skipping record {index} due to missing ID.")
            record["Error_Message"] = "Missing ID"
            skipped_records_shipping.append(record)  # Capture skipped record
            continue

        # Skip records where address fields are empty
        if not any(record.get(col) for col in address_cols_shipping):
            print(f"Skipping record {index} due to missing address fields.")
            record["Error_Message"] = "Missing Address Fields"
            skipped_records_shipping.append(record)  # Capture skipped record
            continue

        # Construct API request parameters
        input_param_dict = {
        'id': license_key,  # Replace with your actual license key variable
        **{f'a{i + 1}': record[col] for i, col in enumerate(address_cols_shipping)},  # Map address fields to a1, a2, a3, etc.
        'postal': record['shipping_postal_code_clean'],
        'loc': record['shipping_city_clean'],
        'admarea': record['shipping_state_clean'],
        'ctry': next((record[col] for col in country_cols_shipping if record[col]), ''),  # Select the first non-empty country value
        'format': 'JSON'
    }

        # Make the API request
        response = requests.get(url, params=input_param_dict)

        if response.status_code == 200:
            response_json = response.json()

            # Check if TotalRecords is 0 (invalid response)
            if response_json.get("TotalRecords") == "0":
                print(f"Invalid API response for record {index} (ID: {record.get('Id')}). Capturing it separately.")
                record["Error_Message"] = "Invalid API Response (TotalRecords = 0)"
                invalid_results_shipping.append(record)  # Store invalid responses
                continue  # Skip this record

            # Merge API response with input data
            response_json.update(record)
            valid_results_shipping.append(response_json)

        else:
            print(f"API Error for record {index} (ID: {record.get('Id')}): {response.status_code} - {response.text}")
            record["Error_Message"] = f"API Error {response.status_code} - {response.text}"
            invalid_results_shipping.append(record)  # Store error responses

    except Exception as e:
        print(f"Unexpected Error for record {index} (ID: {record.get('Id')}): {str(e)}")
        record["Error_Message"] = f"Unexpected Error: {str(e)}"
        invalid_results_shipping.append(record)  # Store exception cases

# Convert the valid results into a DataFrame
if valid_results_shipping:
    valid_results_df_shipping = pd.json_normalize(valid_results_shipping)
    
    display(valid_results_df_shipping)
else:
    print("No valid results received from the API.")

# Convert the invalid results into a DataFrame
if invalid_results_shipping:
    invalid_results_df_shipping = pd.DataFrame(invalid_results_shipping)
    #invalid_results_df.write.mode("append").saveAsTable("devpoc.mdm.melissa_output_billing_invalid_results")
    display(invalid_results_df_shipping)
else:
    print("No invalid responses captured.")

# Define schema for skipped records DataFrame
schema = StructType([
    StructField("Id", StringType(), True),
    StructField("name_clean", StringType(), True),
    StructField("shipping_street_clean", StringType(), True),
    StructField("shipping_city_clean", StringType(), True),
    StructField("shipping_state_clean", StringType(), True),
    StructField("shipping_country_clean", StringType(), True),
    StructField("shipping_postal_code_clean", StringType(), True),
    StructField("hash_key", StringType(), True),
    StructField("Error_Message", StringType(), True)
])

# Convert the skipped records into a DataFrame
if skipped_records_shipping:
    skipped_records_df_shipping = spark.createDataFrame(skipped_records_shipping, schema=schema)
    display(skipped_records_df_shipping)
else:
    print("No records were skipped.")

# COMMAND ----------

from pyspark.sql.utils import AnalysisException

target_table_name = f"{catalog}.clean.melissa_output_shipping_invalid_results"

# Function to check if the table exists
def table_exists(target_table_name):
    try:
        spark.read.table(target_table_name)
        return True
    except AnalysisException:
        return False

try:   
    if 'invalid_results_df_shipping' in locals() and invalid_results_df_shipping is not None and invalid_results_df_shipping.count() > 0:
        if table_exists(target_table_name):  # Check if the table exists
            delta_target = DeltaTable.forName(spark, target_table_name)

            # Perform the merge (Insert or Update)
            delta_target.alias("tgt").merge(
                invalid_results_df_shipping.alias("src"),
                "tgt.sid_id = src.sid_id"  # Replace 'id' with the correct primary key column
            ).whenMatchedUpdate(
                set={col_name: col(f"src.{col_name}") for col_name in invalid_results_df_shipping.columns}  # Update all columns
            ).whenNotMatchedInsert(
                values={col_name: col(f"src.{col_name}") for col_name in invalid_results_df_shipping.columns}  # Insert new records
            ).execute()

            print(f"Data successfully merged into {target_table_name}")
            
            invalid_results_df_shipping.select("sid_id").distinct().createOrReplaceTempView("updated_ids")

            spark.sql(f"""
            UPDATE {catalog}.clean.melissa_input
            SET final_clean_flag = 1
            WHERE sid_id IN (SELECT sid_id FROM updated_ids)
            """)
            print("Data updated")
            udf_send_email('Account Data Refresh', 'melissa shipping invalid results', '', catalog)  
        else:
            invalid_results_df_shipping.write.format("delta").mode("overwrite").saveAsTable(target_table_name)  # Create table if it doesn’t exist
            print(f"Data written to {target_table_name}")

except Exception as e:
    logger.info(f"error: {str(e)}")
    logging.shutdown()

# COMMAND ----------


if 'valid_results_df_shipping' not in globals():
    dbutils.notebook.exit("No Valid resulta. Exiting notebook.")

else:
    valid_results_billing = spark.createDataFrame(valid_results_df_shipping)
    display(valid_results_billing)

# COMMAND ----------

json_str = valid_results_df_shipping.to_json(orient='records')
json_data = json.loads(json_str)
df_flattern_shipping = pd.json_normalize(
    json_data,
    record_path='Records',
    meta=['sid_id','name_clean','shipping_street_clean', 'shipping_city_clean', 'shipping_state_clean','shipping_postal_code_clean','shipping_country_clean','hash_key'],
    sep='_'
)

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import col

# Define target table name
df_raw_shipping = spark.createDataFrame(df_flattern_shipping)
target_table_name = f"{catalog}.clean.melissa_output_shipping_raw"

# Try to get the Delta table reference (Only execute merge if the table exists)
try:
    delta_target = DeltaTable.forName(spark, target_table_name)

    # Perform the merge (Insert or Update)
    delta_target.alias("tgt").merge(
        df_raw_shipping  .alias("src"),
        "tgt.sid_id = src.sid_id"  # Replace 'id' with the correct primary key column
    ).whenMatchedUpdate(
        set={col_name: col(f"src.{col_name}") for col_name in df_raw_shipping .columns}  # Update all columns
    ).whenNotMatchedInsert(
        values={col_name: col(f"src.{col_name}") for col_name in df_raw_shipping .columns}  # Insert new records
    ).execute()

    print(f" Data successfully merged into {target_table_name}")

except Exception as e:
    logger.info(f"error: {str(e)}")
    logging.shutdown()



# COMMAND ----------

#from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, split, expr, size, lit, array_join

df_expand_shipping = df_raw_shipping.withColumn("codes_array", split(col("Results"), ","))

# Create separate columns based on starting letters
df_expand_shipping = df_expand_shipping.withColumn("AC_codes", expr("filter(codes_array, x -> x like 'AC%')")) \
       .withColumn("AE_codes", expr("filter(codes_array, x -> x like 'AE%')")) \
       .withColumn("AS_codes", expr("filter(codes_array, x -> x like 'AS%')")) \
       .withColumn("GS_codes", expr("filter(codes_array, x -> x like 'GS%')")) \
       .withColumn("AV_codes", expr("filter(codes_array, x -> x like 'AV%')")) \
       .withColumn("GE_codes", expr("filter(codes_array, x -> x like 'GE%')"))

# Replace empty arrays with null
df_expand_shipping = df_expand_shipping.withColumn("AC_codes", when(size(col("AC_codes")) == 0, lit(None)).otherwise(col("AC_codes"))) \
       .withColumn("AE_codes", when(size(col("AE_codes")) == 0, lit(None)).otherwise(col("AE_codes"))) \
       .withColumn("AS_codes", when(size(col("AS_codes")) == 0, lit(None)).otherwise(col("AS_codes"))) \
       .withColumn("AV_codes", when(size(col("AV_codes")) == 0, lit(None)).otherwise(col("AV_codes"))) \
       .withColumn("GS_codes", when(size(col("GS_codes")) == 0, lit(None)).otherwise(col("GS_codes"))) \
       .withColumn("GE_codes", when(size(col("GE_codes")) == 0, lit(None)).otherwise(col("GE_codes")))

# Add a new column based on AV values
df_expand_shipping= df_expand_shipping.withColumn(
    "Verification_Level",
    when(array_join(col("AV_codes"), ",").rlike(r"AV(11|12|13|14)"), "PV")
    .when(array_join(col("AV_codes"), ",").rlike(r"AV(21|22|23|24|25)"), "V")
    .otherwise(None)
)       

df_expand_shipping = df_expand_shipping.withColumn(
    "AC_code_description",
    expr("""
        transform(
            AC_codes,
            x -> CASE
                WHEN x = 'AC01' THEN 'Postal Code Change'
                WHEN x = 'AC02' THEN 'Administrative Area Change'
                WHEN x = 'AC03' THEN 'Locality Change'
                WHEN x = 'AC09' THEN 'Dependent Locality Change'
                WHEN x = 'AC10' THEN 'Thoroughfare Name Change'
                WHEN x = 'AC11' THEN 'Thoroughfare Type Change'
                WHEN x = 'AC12' THEN 'Thoroughfare Directional Change'
                WHEN x = 'AC13' THEN 'Sub Premise Type Change'
                WHEN x = 'AC14' THEN 'Sub Premise Number Change'
                WHEN x = 'AC15' THEN 'Double Dependent Locality Change'
                WHEN x = 'AC16' THEN 'SubAdministrative Area Change'
                WHEN x = 'AC17' THEN 'SubNational Area Change'
                WHEN x = 'AC18' THEN 'PO Box Change'
                WHEN x = 'AC19' THEN 'Premise Type Change'
                WHEN x = 'AC20' THEN 'House Number Change'
                WHEN x = 'AC22' THEN 'Organization Change'
                ELSE null
            END
        )
    """)
)

# Drop the intermediate 'codes_array' column if not needed
df_expand_shipping = df_expand_shipping.drop("codes_array")




# COMMAND ----------


from delta.tables import DeltaTable
from pyspark.sql.functions import col

# Define target table name
target_table_name = f"{catalog}.clean.melissa_output_shipping_all"

# Try to get the Delta table reference (Only execute merge if the table exists)
try:
    delta_target = DeltaTable.forName(spark, target_table_name)

    # Perform the merge (Insert or Update)
    delta_target.alias("tgt").merge(
       df_expand_shipping.alias("src"),
        "tgt.sid_id = src.sid_id"  # Replace 'id' with the correct primary key column
    ).whenMatchedUpdate(
        set={col_name: col(f"src.{col_name}") for col_name in df_expand_shipping.columns}  # Update all columns
    ).whenNotMatchedInsert(
        values={col_name: col(f"src.{col_name}") for col_name in df_expand_shipping.columns}  # Insert new records
    ).execute()

    print(f" Data successfully merged into {target_table_name}")

except Exception as e:
    logger.info(f"error: {str(e)}")
    logging.shutdown()

# COMMAND ----------

df_expand_shipping= df_expand_shipping.withColumn("flag_backfill_shipping", lit(None)) 
display(df_expand_shipping)

# COMMAND ----------

from pyspark.sql.functions import col, when, trim

# Define the columns to backfill (mapping of columns to replace and their source values)
columns_to_backfill = {
    "AddressLine1": "shipping_street_clean",
    "Locality": "shipping_city_clean",
    "AdministrativeArea": "shipping_state_clean",
    "PostalCode": "shipping_postal_code_clean",
    "CountryISO3166_1_Alpha2": "shipping_country_clean"
}

# Apply the update only when the original column was NULL
for target_col, source_col in columns_to_backfill.items():
    df_expand_shipping = df_expand_shipping.withColumn(
        target_col, when(col(target_col).isNull() | (trim(col(target_col)) == ""), col(source_col)).otherwise(col(target_col))
    ).withColumn(
        "flag_backfill_shipping", 
        when(col(target_col).isNull() | (trim(col(target_col)) == "") & col(source_col).isNotNull(), 1).otherwise(col("flag_backfill_shipping"))
    )



# COMMAND ----------

from pyspark.sql.functions import col

df_selected_shipping = df_expand_shipping.select(
    col("sid_id"),
    col("name_clean"),
    col("shipping_street_clean").alias("input_shipping_street_clean"),
    col("AddressLine1").alias("validated_shipping_street_clean"),
    col("shipping_city_clean").alias("input_shipping_city_clean"),
    col("Locality").alias("validated_shipping_city_clean"),
    col("shipping_state_clean").alias("input_shipping_state_clean"),
    col("AdministrativeArea").alias("validated_shipping_state_clean"),
    col("shipping_postal_code_clean").alias("input_shipping_postal_code_clean"),
    col("PostalCode").alias("validated_shipping_postal_code_clean"),
    col("shipping_country_clean").alias("input_shipping_country_clean"),
    col("CountryISO3166_1_Alpha2").alias("validated_shipping_country_clean"),
    col("FormattedAddress").alias("validated_shipping_full_address_clean"),
    col("Verification_Level").alias("validated_verification_level_shipping"),
    col("hash_key")
)

display(df_selected_shipping)

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import col

# Define target table name

target_table_name = f"{catalog}.clean.melissa_output_shipping_final"

# Try to get the Delta table reference (Only execute merge if the table exists)
try:
    delta_target = DeltaTable.forName(spark, target_table_name)

    # Perform the merge (Insert or Update)
    delta_target.alias("tgt").merge(
        df_selected_shipping.alias("src"),
        "tgt.sid_id = src.sid_id"  # Replace 'id' with the correct primary key column
    ).whenMatchedUpdate(
        set={col_name: col(f"src.{col_name}") for col_name in df_selected_shipping.columns}  # Update all columns
    ).whenNotMatchedInsert(
        values={col_name: col(f"src.{col_name}") for col_name in df_selected_shipping.columns}  # Insert new records
    ).execute()

    print(f" Data successfully merged into {target_table_name}")

except Exception as e:
    print(f" Target table {target_table_name} does not exist. Skipping merge operation.")
    logger.error(f"Error occurred while processing for {target_table} : {str(e)}")
    logging.shutdown()
    udf_send_email(f'Account Data Refresh','melissa shipping address failure',{str(e)},catalog)

# COMMAND ----------

logging.shutdown()
