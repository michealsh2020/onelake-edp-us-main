# Databricks notebook source
import pandas as pd
import json
import urllib.parse
import urllib.request
from datetime import datetime
import requests
import logging


# COMMAND ----------

dbutils.widgets.text("catalog", "", "catalog")
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

query = f"""drop table {catalog}.clean.melissa_output_shipping_all"""
spark.sql(query)

# COMMAND ----------

query = f"""
CREATE TABLE IF NOT EXISTS {catalog}.clean.melissa_output_shipping_all (
RecordID STRING,
Results STRING,
FormattedAddress STRING,
Organization STRING,
AddressLine1 STRING,
AddressLine2 STRING,
AddressLine3 STRING,
AddressLine4 STRING,
AddressLine5 STRING,
AddressLine6 STRING,
AddressLine7 STRING,
AddressLine8 STRING,
SubPremises STRING,
DoubleDependentLocality STRING,
DependentLocality STRING,
Locality STRING,
SubAdministrativeArea STRING,
AdministrativeArea STRING,
PostalCode STRING,
PostalCodeType STRING,
AddressType STRING,
AddressKey STRING,
SubNationalArea STRING,
CountryName STRING,
CountryISO3166_1_Alpha2 STRING,
CountryISO3166_1_Alpha3 STRING,
CountryISO3166_1_Numeric STRING,
CountrySubdivisionCode STRING,
Thoroughfare STRING,
ThoroughfarePreDirection STRING,
ThoroughfareLeadingType STRING,
ThoroughfareName STRING,
ThoroughfareTrailingType STRING,
ThoroughfarePostDirection STRING,
DependentThoroughfare STRING,
DependentThoroughfarePreDirection STRING,
DependentThoroughfareLeadingType STRING,
DependentThoroughfareName STRING,
DependentThoroughfareTrailingType STRING,
DependentThoroughfarePostDirection STRING,
Building STRING,
PremisesType STRING,
PremisesNumber STRING,
SubPremisesType STRING,
SubPremisesNumber STRING,
PostBox STRING,
Latitude STRING,
Longitude STRING,
DeliveryIndicator STRING,
MelissaAddressKey STRING,
MelissaAddressKeyBase STRING,
PostOfficeLocation STRING,
SubPremiseLevel STRING,
SubPremiseLevelType STRING,
SubPremiseLevelNumber STRING,
SubBuilding STRING,
SubBuildingType STRING,
SubBuildingNumber STRING,
UTC STRING,
DST STRING,
DeliveryPointSuffix STRING,
CensusKey STRING,
sid_id STRING,
name_clean STRING,
shipping_street_clean STRING,
shipping_city_clean STRING,
shipping_state_clean STRING,
shipping_postal_code_clean STRING,
shipping_country_clean STRING,
AC_codes ARRAY<STRING>,
AE_codes ARRAY<STRING>,
AS_codes ARRAY<STRING>,
GS_codes ARRAY<STRING>,
AV_codes ARRAY<STRING>,
GE_codes ARRAY<STRING>,
Verification_Level STRING,
AC_code_description ARRAY<STRING>,
hash_key STRING)
USING delta         
"""
spark.sql(query)


# COMMAND ----------

from pyspark.sql.functions import col
df_raw_shipping = spark.read.table(f"{catalog}.clean.melissa_output_shipping_raw")
display(df_raw_shipping)

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

# Show the results
display(df_expand_shipping)


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
    print(f" Target table {target_table_name} does not exist. Skipping merge operation. {str(e)}")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ${catalog}.clean.melissa_output_shipping_all
