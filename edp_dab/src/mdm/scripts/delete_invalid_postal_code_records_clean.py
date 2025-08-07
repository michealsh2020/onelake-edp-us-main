# Databricks notebook source
dbutils.widgets.text("source_catalog", "", "Catalog")
source_catalog = dbutils.widgets.get("source_catalog")

# COMMAND ----------

from pyspark.sql.functions import col, when, upper, lower, lit, regexp_replace, trim, explode, split, udf
from pyspark.sql.types import StringType

# COMMAND ----------

# --- UDF Definition ---
# Converts a postal code string (e.g., "AB1 2CD") to its pattern ("AAN NAN")
# This UDF is required for the validation logic.
def _generate_pattern_from_postal_code_py(postal_code_value: str) -> str:
    """
    Converts a postal code string into a pattern using 'A' for letters and 'N' for numbers.
    Other characters are preserved. Returns None for None input.
    """
    if postal_code_value is None:
        return None
    pattern_chars = []
    for char_val in postal_code_value:
        if char_val.isdigit():
            pattern_chars.append('N')
        elif char_val.isalpha():
            pattern_chars.append('A') # 'A' for any letter
        else:
            pattern_chars.append(char_val) # Keep spaces, hyphens, etc.
    return "".join(pattern_chars)

# Register the Python function as a Spark UDF
generate_pattern_from_postal_code_udf = udf(_generate_pattern_from_postal_code_py, StringType())

# COMMAND ----------

def check_invalid_code(df, postal_code_column, country_column):
    """
     Loop through list of each country to check whether the postal code is in list of invalid postal codes,if it is there then make the flag to True and remove the postal code value
    
    Args:
        df (spark dataframe)    : The dataframe containing the data to check invalid postal codes.
        postal_code_column (str): postal code column to check values of.
        country_column(str)     : country column to check values of.

    Returns:
        df : Original dataframe with cleaned column_name,
            additional column 'review_invalid_{postal_code_column}_flag' indicating for set of countries in country_groups check the postal_code_column is in list of invalid postal codes(invalid_codes)then make the flag to True and remove the postal_code_column value if the condition satisfies.
    """
    review_flag_col = f'review_invalid_{postal_code_column}_flag'

    # Ensure the flag column exists, otherwise initialize with False
    if review_flag_col not in df.columns:
        df = df.withColumn(review_flag_col, lit(False))

    # Loop through list of each country to check whether the postal code is in list of invalid postal codes,if it is there then make the flag to True and remove the postal code value
    for country_key, country_aliases in country_groups.items():
        invalid_codes = invalid_postal_codes_by_country[country_key]

        df = df.withColumn(
            review_flag_col,
            when(
                (upper(col(country_column)).isin([abbr.upper() for abbr in country_aliases])) &
                (upper(col(postal_code_column)).isin([code.upper() for code in invalid_codes])),
                True
            ).otherwise(col(review_flag_col))
        ).withColumn(
            postal_code_column,
            when(
                (upper(col(country_column)).isin([abbr.upper() for abbr in country_aliases])) &
                (upper(col(postal_code_column)).isin([code.upper() for code in invalid_codes])),
                ""
            ).otherwise(col(postal_code_column))
        )
    return df

# COMMAND ----------

def check_valid_code(df, postal_code_column, country_column):
    """
    Validates postal codes against a tmp.sf_account_clean_address_for_review table ONLY for countries present in 
    this table.
    Updates the review flag and postal code columns accordingly for those countries, preserving original values for others.
    postal_code_column, country_column, and a 'review_invalid_<postal_code_column>_flag' column already exist and updates them based on validation results for relevant countries.

    Args:
        df (spark dataframe)    : The dataframe containing the data to check valid postal codes.
        postal_code_column (str): postal code column to check values of.
        country_column(str)     : country column to check values of.

    Returns:
        df : Original dataframe with cleaned column_name,
            additional column 'review_invalid_{postal_code_column}_flag' indicating for set of countries in tmp.sf_account_clean_address_for_review table to check the postal code format in postal_code_column is in valid_postal_code_formats for a particular country then make the flag to False,else remove the postal_code_column value and make the flag to True.
    """
    # Determine the name of the review flag column
    review_flag_col = f'review_invalid_{postal_code_column}_flag'
    # 1. Assume postal_code_column, country_column, and review_flag_col exist.
    # 2. Do not check for their existence within this function.
    # 3. Update review_flag_col and postal_code_column only for countries in the lookup table.

    valid_formats_table = f"{source_catalog}.clean.valid_postal_code_formats" # Table with country formats

    # 1. Prepare the Lookup Table of Valid Format Patterns
    valid_formats_df = spark.table(valid_formats_table)
    # Prepare the lookup table for joining
    lookup_prepared_df = (valid_formats_df
        .withColumn("_temp_country_alias", explode(split(col("country"), "\\s*/\\s*")))
        .withColumn("_temp_format_str", explode(split(col("valid_postal_code_format"), "\\s*/\\s*")))
        .select(
            upper(trim(col("_temp_country_alias"))).alias("_lookup_country_key"), # trim and convert into upper case
            trim(col("_temp_format_str")).alias("_lookup_format_str")
        )
        .filter(col("_lookup_format_str") != "") # Filter out empty format strings
        .distinct()
    )

    # Extract just the unique country keys from the lookup table.
    # This will be used to identify if a country from the input is in the lookup at all.
    lookup_country_keys_df = lookup_prepared_df.select("_lookup_country_key").distinct()

    # 2. Prepare the input DataFrame
    # Add a column with the NA-pattern generated from the postal code value.(_pc_pattern_generated)
    # Add a column with the uppercase country for joining.(_country_upper_for_join)
    # We assume postal_code_column and country_column exist in df.
    df_prepared = (df
        # Add a temporary column with the generated NA-pattern
        .withColumn("_pc_pattern_generated", generate_pattern_from_postal_code_udf(trim(col(postal_code_column))))
        # Add a temporary column with the trimmed and uppercased country for joining
        .withColumn("_country_upper_for_join", upper(trim(col(country_column))))
    )

    # 3. Check if the country from the input DataFrame is in the lookup table
    # Left join df_prepared with the unique country keys from the lookup table.
    # Rows in df_prepared whose country exists in the lookup table will have a non-NULL '_country_in_lookup_key' after this join.
    df_with_country_presence = df_prepared.join(
        lookup_country_keys_df.selectExpr("_lookup_country_key as _country_in_lookup_key"),
        df_prepared["_country_upper_for_join"] == col("_country_in_lookup_key"),
        "left"
    # Add a boolean flag indicating if the country was found in the lookup
    ).withColumn("_is_country_in_lookup", col("_country_in_lookup_key").isNotNull())

    # 4. Check if the postal code pattern matches any valid format for the country
    # Left join df_with_country_presence with the full lookup_prepared_df on BOTH country and pattern.
    # Rows will have a non-NULL '_match_format' if the country + pattern matched a valid entry.
    # This happens *only* if the pattern is valid *for a country that is in the lookup* and has formats.
    # We use all columns from df_with_country_presence in the join 'on' condition to preserve rows and columns.
    df_validated = df_with_country_presence.join(
        lookup_prepared_df.selectExpr("_lookup_country_key as _match_key", "_lookup_format_str as _match_format"),
        (df_with_country_presence["_country_upper_for_join"] == col("_match_key")) & \
        (df_with_country_presence["_pc_pattern_generated"] == col("_match_format")),
        "left"
    # Add a boolean flag indicating if the pattern matched a valid format for the country
    ).withColumn("_pattern_has_valid_match", col("_match_format").isNotNull())

    # 5. Update the review flag and postal code columns based on the validation flags
    # We use the flags _is_country_in_lookup and _pattern_has_valid_match.This is where the logic "only process records for countries in lookup" is applied.
    # If _is_country_in_lookup is False, the OTHERWISE clause is executed,which keeps the original column value.

    # Update the review flag column
    df_with_updated_flag = df_validated.withColumn(
        review_flag_col,
        when(col("_is_country_in_lookup"),  # Condition: Is the country present in the lookup table?
                when(col("_pattern_has_valid_match"), lit(False)) # If YES (country in lookup): Did the pattern match a valid format? -> False if Yes
                .otherwise(lit(True)) # If YES (country in lookup): But pattern did NOT match -> True
             )
        .otherwise(col(review_flag_col)) # If NO (country not in lookup) -> Keep the original flag value
    )

    # Update the postal code column
    df_with_updated_cols = df_with_updated_flag.withColumn(
        postal_code_column,
        when(col("_is_country_in_lookup"),  # Condition: Is the country present in the lookup table?
                when(col("_pattern_has_valid_match"), col(postal_code_column)) # If YES (country in lookup): Did the pattern match a valid format? -> Keep original PC if Yes
                .otherwise(lit("")) # If YES (country in lookup): But pattern did NOT match -> Empty PC ("")
             )
        .otherwise(col(postal_code_column)) # If NO (country not in lookup) -> Keep the original PC value
    )

    # 6. Clean up - Drop temporary columns
    df_cleaned = df_with_updated_cols.drop( # Drop from the latest DataFrame
        "_pc_pattern_generated",      # Temporary pattern column
        "_country_upper_for_join",    # Temporary upper case country column
        "_country_in_lookup_key",     # Temporary column from country presence check join
        "_is_country_in_lookup",      # Temporary boolean flag for country presence
        "_match_key",                 # Temporary column from pattern match join
        "_match_format",              # Temporary column from pattern match join
        "_pattern_has_valid_match",   # Temporary boolean flag for pattern match
        "_temp_country_alias",        # Temporary column from lookup_df processing
        "_temp_format_str"            # Temporary column from lookup_df processing
    )
    return df_cleaned

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delete Invalid Postal Code Records existing in clean and tmp 

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Move the invalid postal code records from sf_account_clean_address table to history table(sf_account_clean_address_reviewed)
# MAGIC -- Delete invalid postal code records from sf_account_clean_address table
# MAGIC -- The total count must match with delete count
# MAGIC
# MAGIC
# MAGIC select count(distinct src.sid_id) as Source_Count, count(distinct dest.sid_id) as Dest_Count
# MAGIC from ${source_catalog}.tmp.sf_account_clean_address_for_review src
# MAGIC     inner join ${source_catalog}.clean.sf_account_clean_address dest
# MAGIC     on src.sid_id = dest.sid_id
# MAGIC     and src.hash_key != dest.hash_key
# MAGIC     where (src.needs_manual_review = true and src.final_review_postal_code_flag=true and src.ingested_timestamp> dest.ingested_timestamp 
# MAGIC            and dest.needs_manual_review = false);
# MAGIC
# MAGIC insert into ${source_catalog}.clean.sf_account_clean_address_reviewed(sid_id,name,name_clean,name_clean_acct_flag,name_clean_on_behalf_flag,name_clean_aka_flag,name_clean_asterisks_flag,name_clean_care_of_flag,name_clean_dba_flag,name_clean_donot_flag,name_clean_duplicate_flag,name_clean_fbo_flag,name_clean_length_flag,name_clean_none_na_flag,name_clean_student_flag,name_clean_test_flag,name_clean_trust_flag,name_clean_tbd_flag,review_name_clean_flag,billing_street,billing_street_clean,billing_street_clean_attn_flag,billing_street_clean_asterisks_flag,billing_street_clean_care_of_flag,billing_street_clean_tbd_flag,billing_street_clean_test_flag,review_billing_street_clean_flag,shipping_street,shipping_street_clean,shipping_street_clean_attn_flag,shipping_street_clean_asterisks_flag,shipping_street_clean_care_of_flag,
# MAGIC shipping_street_clean_tbd_flag,shipping_street_clean_test_flag,review_shipping_street_clean_flag,billing_city,billing_city_clean,billing_city_clean_null_flag,billing_city_clean_number_flag,shipping_city,shipping_city_clean,shipping_city_clean_null_flag,shipping_city_clean_number_flag,billing_state,billing_state_clean,billing_country,billing_country_clean,billing_postal_code,billing_postal_code_clean,billing_state_clean_null_flag,billing_state_clean_number_flag,invalid_ca_billing_state_flag,invalid_au_billing_state_flag,invalid_us_billing_state_flag,review_billing_state_clean_flag,billing_country_clean_null_flag,billing_country_clean_number_flag,shipping_state,shipping_state_clean,shipping_country,shipping_country_clean,shipping_postal_code,shipping_postal_code_clean,shipping_state_clean_null_flag,shipping_state_clean_number_flag,invalid_ca_shipping_state_flag,invalid_au_shipping_state_flag,invalid_us_shipping_state_flag,review_shipping_state_clean_flag,shipping_country_clean_null_flag,shipping_country_clean_number_flag,review_invalid_billing_postal_code_clean_flag,review_invalid_shipping_postal_code_clean_flag,final_review_postal_code_flag,final_review_all_address_columns_flag,system_modstamp,account_id,named_account,billing_geolocation,shipping_geolocation,billing_address_complete,billing_address_validation_status_msg,shipping_address_complete,shipping_address_validation_status_msg,master_record_id,type,billing_latitude,billing_longitude,billing_geocode_accuracy,shipping_latitude,shipping_longitude,shipping_geocode_accuracy,phone,account_number,website,created_date,created_by_id,last_modified_date,last_modified_by_id,archived,billing_county,shipping_county,billing_address_validation_status,hash_key,ingested_timestamp,final_clean_flag,address_last_cleaned_timestamp,name_clean_additional_data,billing_street_clean_additional_data,shipping_street_clean_additional_data,review_billing_address_flag,review_shipping_address_flag,needs_manual_review)
# MAGIC
# MAGIC select sid_id,name,name_clean,name_clean_acct_flag,name_clean_on_behalf_flag,name_clean_aka_flag,name_clean_asterisks_flag,name_clean_care_of_flag,name_clean_dba_flag,name_clean_donot_flag,name_clean_duplicate_flag,name_clean_fbo_flag,name_clean_length_flag,name_clean_none_na_flag,name_clean_student_flag,name_clean_test_flag,name_clean_trust_flag,name_clean_tbd_flag,review_name_clean_flag,billing_street,billing_street_clean,billing_street_clean_attn_flag,billing_street_clean_asterisks_flag,billing_street_clean_care_of_flag,billing_street_clean_tbd_flag,billing_street_clean_test_flag,review_billing_street_clean_flag,shipping_street,shipping_street_clean,shipping_street_clean_attn_flag,shipping_street_clean_asterisks_flag,shipping_street_clean_care_of_flag,
# MAGIC shipping_street_clean_tbd_flag,shipping_street_clean_test_flag,review_shipping_street_clean_flag,billing_city,billing_city_clean,billing_city_clean_null_flag,billing_city_clean_number_flag,shipping_city,shipping_city_clean,shipping_city_clean_null_flag,shipping_city_clean_number_flag,billing_state,billing_state_clean,billing_country,billing_country_clean,billing_postal_code,billing_postal_code_clean,billing_state_clean_null_flag,billing_state_clean_number_flag,invalid_ca_billing_state_flag,invalid_au_billing_state_flag,invalid_us_billing_state_flag,review_billing_state_clean_flag,billing_country_clean_null_flag,billing_country_clean_number_flag,shipping_state,shipping_state_clean,shipping_country,shipping_country_clean,shipping_postal_code,shipping_postal_code_clean,shipping_state_clean_null_flag,shipping_state_clean_number_flag,invalid_ca_shipping_state_flag,invalid_au_shipping_state_flag,invalid_us_shipping_state_flag,review_shipping_state_clean_flag,shipping_country_clean_null_flag,shipping_country_clean_number_flag,review_invalid_billing_postal_code_clean_flag,review_invalid_shipping_postal_code_clean_flag,final_review_postal_code_flag,final_review_all_address_columns_flag,system_modstamp,account_id,named_account,billing_geolocation,shipping_geolocation,billing_address_complete,billing_address_validation_status_msg,shipping_address_complete,shipping_address_validation_status_msg,master_record_id,type,billing_latitude,billing_longitude,billing_geocode_accuracy,shipping_latitude,shipping_longitude,shipping_geocode_accuracy,phone,account_number,website,created_date,created_by_id,last_modified_date,last_modified_by_id,archived,billing_county,shipping_county,billing_address_validation_status,hash_key,ingested_timestamp,final_clean_flag,address_last_cleaned_timestamp,name_clean_additional_data,billing_street_clean_additional_data,shipping_street_clean_additional_data,review_billing_address_flag,review_shipping_address_flag,needs_manual_review 
# MAGIC from ${source_catalog}.clean.sf_account_clean_address where sid_id in (select distinct sid_id from ${source_catalog}.tmp.sf_account_clean_address_for_review where needs_manual_review = true and final_review_postal_code_flag=true) and needs_manual_review=false;
# MAGIC
# MAGIC delete from ${source_catalog}.clean.sf_account_clean_address where sid_id in (select distinct sid_id from ${source_catalog}.tmp.sf_account_clean_address_for_review where needs_manual_review = true and final_review_postal_code_flag=true) and needs_manual_review=false;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delete Invalid Postal Code Records existing in clean

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Move the invalid postal code records from sf_account_clean_address table to tmp table(sf_account_clean_address_for_review)
# MAGIC -- Delete invalid postal code records from sf_account_clean_address table
# MAGIC -- The total count must match with delete count
# MAGIC
# MAGIC select count(distinct src.sid_id) as Source_Count, count(distinct dest.sid_id) as Dest_Count
# MAGIC from ${source_catalog}.clean.sf_account_clean_address src
# MAGIC     left join ${source_catalog}.tmp.sf_account_clean_address_for_review dest
# MAGIC     on src.sid_id = dest.sid_id
# MAGIC     and src.hash_key != dest.hash_key
# MAGIC     where (src.needs_manual_review = false and src.final_review_postal_code_flag=true);
# MAGIC
# MAGIC insert into ${source_catalog}.clean.sf_account_clean_address_reviewed(sid_id,name,name_clean,name_clean_acct_flag,name_clean_on_behalf_flag,name_clean_aka_flag,name_clean_asterisks_flag,name_clean_care_of_flag,name_clean_dba_flag,name_clean_donot_flag,name_clean_duplicate_flag,name_clean_fbo_flag,name_clean_length_flag,name_clean_none_na_flag,name_clean_student_flag,name_clean_test_flag,name_clean_trust_flag,name_clean_tbd_flag,review_name_clean_flag,billing_street,billing_street_clean,billing_street_clean_attn_flag,billing_street_clean_asterisks_flag,billing_street_clean_care_of_flag,billing_street_clean_tbd_flag,billing_street_clean_test_flag,review_billing_street_clean_flag,shipping_street,shipping_street_clean,shipping_street_clean_attn_flag,shipping_street_clean_asterisks_flag,shipping_street_clean_care_of_flag,
# MAGIC shipping_street_clean_tbd_flag,shipping_street_clean_test_flag,review_shipping_street_clean_flag,billing_city,billing_city_clean,billing_city_clean_null_flag,billing_city_clean_number_flag,shipping_city,shipping_city_clean,shipping_city_clean_null_flag,shipping_city_clean_number_flag,billing_state,billing_state_clean,billing_country,billing_country_clean,billing_postal_code,billing_postal_code_clean,billing_state_clean_null_flag,billing_state_clean_number_flag,invalid_ca_billing_state_flag,invalid_au_billing_state_flag,invalid_us_billing_state_flag,review_billing_state_clean_flag,billing_country_clean_null_flag,billing_country_clean_number_flag,shipping_state,shipping_state_clean,shipping_country,shipping_country_clean,shipping_postal_code,shipping_postal_code_clean,shipping_state_clean_null_flag,shipping_state_clean_number_flag,invalid_ca_shipping_state_flag,invalid_au_shipping_state_flag,invalid_us_shipping_state_flag,review_shipping_state_clean_flag,shipping_country_clean_null_flag,shipping_country_clean_number_flag,review_invalid_billing_postal_code_clean_flag,review_invalid_shipping_postal_code_clean_flag,final_review_postal_code_flag,final_review_all_address_columns_flag,system_modstamp,account_id,named_account,billing_geolocation,shipping_geolocation,billing_address_complete,billing_address_validation_status_msg,shipping_address_complete,shipping_address_validation_status_msg,master_record_id,type,billing_latitude,billing_longitude,billing_geocode_accuracy,shipping_latitude,shipping_longitude,shipping_geocode_accuracy,phone,account_number,website,created_date,created_by_id,last_modified_date,last_modified_by_id,archived,billing_county,shipping_county,billing_address_validation_status,hash_key,ingested_timestamp,final_clean_flag,address_last_cleaned_timestamp,name_clean_additional_data,billing_street_clean_additional_data,shipping_street_clean_additional_data,review_billing_address_flag,review_shipping_address_flag,needs_manual_review)
# MAGIC
# MAGIC select sid_id,name,name_clean,name_clean_acct_flag,name_clean_on_behalf_flag,name_clean_aka_flag,name_clean_asterisks_flag,name_clean_care_of_flag,name_clean_dba_flag,name_clean_donot_flag,name_clean_duplicate_flag,name_clean_fbo_flag,name_clean_length_flag,name_clean_none_na_flag,name_clean_student_flag,name_clean_test_flag,name_clean_trust_flag,name_clean_tbd_flag,review_name_clean_flag,billing_street,billing_street_clean,billing_street_clean_attn_flag,billing_street_clean_asterisks_flag,billing_street_clean_care_of_flag,billing_street_clean_tbd_flag,billing_street_clean_test_flag,review_billing_street_clean_flag,shipping_street,shipping_street_clean,shipping_street_clean_attn_flag,shipping_street_clean_asterisks_flag,shipping_street_clean_care_of_flag,
# MAGIC shipping_street_clean_tbd_flag,shipping_street_clean_test_flag,review_shipping_street_clean_flag,billing_city,billing_city_clean,billing_city_clean_null_flag,billing_city_clean_number_flag,shipping_city,shipping_city_clean,shipping_city_clean_null_flag,shipping_city_clean_number_flag,billing_state,billing_state_clean,billing_country,billing_country_clean,billing_postal_code,billing_postal_code_clean,billing_state_clean_null_flag,billing_state_clean_number_flag,invalid_ca_billing_state_flag,invalid_au_billing_state_flag,invalid_us_billing_state_flag,review_billing_state_clean_flag,billing_country_clean_null_flag,billing_country_clean_number_flag,shipping_state,shipping_state_clean,shipping_country,shipping_country_clean,shipping_postal_code,shipping_postal_code_clean,shipping_state_clean_null_flag,shipping_state_clean_number_flag,invalid_ca_shipping_state_flag,invalid_au_shipping_state_flag,invalid_us_shipping_state_flag,review_shipping_state_clean_flag,shipping_country_clean_null_flag,shipping_country_clean_number_flag,review_invalid_billing_postal_code_clean_flag,review_invalid_shipping_postal_code_clean_flag,final_review_postal_code_flag,final_review_all_address_columns_flag,system_modstamp,account_id,named_account,billing_geolocation,shipping_geolocation,billing_address_complete,billing_address_validation_status_msg,shipping_address_complete,shipping_address_validation_status_msg,master_record_id,type,billing_latitude,billing_longitude,billing_geocode_accuracy,shipping_latitude,shipping_longitude,shipping_geocode_accuracy,phone,account_number,website,created_date,created_by_id,last_modified_date,last_modified_by_id,archived,billing_county,shipping_county,billing_address_validation_status,hash_key,ingested_timestamp,2,address_last_cleaned_timestamp,name_clean_additional_data,billing_street_clean_additional_data,shipping_street_clean_additional_data,review_billing_address_flag,review_shipping_address_flag,true 
# MAGIC from ${source_catalog}.clean.sf_account_clean_address where final_review_postal_code_flag=true and needs_manual_review=false and final_clean_flag=0 and sid_id not in (select distinct sid_id from ${source_catalog}.tmp.sf_account_clean_address_for_review where needs_manual_review = true);
# MAGIC
# MAGIC insert into ${source_catalog}.tmp.sf_account_clean_address_for_review(sid_id,name,name_clean,name_clean_acct_flag,name_clean_on_behalf_flag,name_clean_aka_flag,name_clean_asterisks_flag,name_clean_care_of_flag,name_clean_dba_flag,name_clean_donot_flag,name_clean_duplicate_flag,name_clean_fbo_flag,name_clean_length_flag,name_clean_none_na_flag,name_clean_student_flag,name_clean_test_flag,name_clean_trust_flag,name_clean_tbd_flag,review_name_clean_flag,billing_street,billing_street_clean,billing_street_clean_attn_flag,billing_street_clean_asterisks_flag,billing_street_clean_care_of_flag,billing_street_clean_tbd_flag,billing_street_clean_test_flag,review_billing_street_clean_flag,shipping_street,shipping_street_clean,shipping_street_clean_attn_flag,shipping_street_clean_asterisks_flag,shipping_street_clean_care_of_flag,
# MAGIC shipping_street_clean_tbd_flag,shipping_street_clean_test_flag,review_shipping_street_clean_flag,billing_city,billing_city_clean,billing_city_clean_null_flag,billing_city_clean_number_flag,shipping_city,shipping_city_clean,shipping_city_clean_null_flag,shipping_city_clean_number_flag,billing_state,billing_state_clean,billing_country,billing_country_clean,billing_postal_code,billing_postal_code_clean,billing_state_clean_null_flag,billing_state_clean_number_flag,invalid_ca_billing_state_flag,invalid_au_billing_state_flag,invalid_us_billing_state_flag,review_billing_state_clean_flag,billing_country_clean_null_flag,billing_country_clean_number_flag,shipping_state,shipping_state_clean,shipping_country,shipping_country_clean,shipping_postal_code,shipping_postal_code_clean,shipping_state_clean_null_flag,shipping_state_clean_number_flag,invalid_ca_shipping_state_flag,invalid_au_shipping_state_flag,invalid_us_shipping_state_flag,review_shipping_state_clean_flag,shipping_country_clean_null_flag,shipping_country_clean_number_flag,review_invalid_billing_postal_code_clean_flag,review_invalid_shipping_postal_code_clean_flag,final_review_postal_code_flag,final_review_all_address_columns_flag,system_modstamp,account_id,named_account,billing_geolocation,shipping_geolocation,billing_address_complete,billing_address_validation_status_msg,shipping_address_complete,shipping_address_validation_status_msg,master_record_id,type,billing_latitude,billing_longitude,billing_geocode_accuracy,shipping_latitude,shipping_longitude,shipping_geocode_accuracy,phone,account_number,website,created_date,created_by_id,last_modified_date,last_modified_by_id,archived,billing_county,shipping_county,billing_address_validation_status,hash_key,ingested_timestamp,final_clean_flag,address_last_cleaned_timestamp,name_clean_additional_data,billing_street_clean_additional_data,shipping_street_clean_additional_data,review_billing_address_flag,review_shipping_address_flag,needs_manual_review)
# MAGIC
# MAGIC select sid_id,name,name_clean,name_clean_acct_flag,name_clean_on_behalf_flag,name_clean_aka_flag,name_clean_asterisks_flag,name_clean_care_of_flag,name_clean_dba_flag,name_clean_donot_flag,name_clean_duplicate_flag,name_clean_fbo_flag,name_clean_length_flag,name_clean_none_na_flag,name_clean_student_flag,name_clean_test_flag,name_clean_trust_flag,name_clean_tbd_flag,review_name_clean_flag,billing_street,billing_street_clean,billing_street_clean_attn_flag,billing_street_clean_asterisks_flag,billing_street_clean_care_of_flag,billing_street_clean_tbd_flag,billing_street_clean_test_flag,review_billing_street_clean_flag,shipping_street,shipping_street_clean,shipping_street_clean_attn_flag,shipping_street_clean_asterisks_flag,shipping_street_clean_care_of_flag,
# MAGIC shipping_street_clean_tbd_flag,shipping_street_clean_test_flag,review_shipping_street_clean_flag,billing_city,billing_city_clean,billing_city_clean_null_flag,billing_city_clean_number_flag,shipping_city,shipping_city_clean,shipping_city_clean_null_flag,shipping_city_clean_number_flag,billing_state,billing_state_clean,billing_country,billing_country_clean,billing_postal_code,billing_postal_code_clean,billing_state_clean_null_flag,billing_state_clean_number_flag,invalid_ca_billing_state_flag,invalid_au_billing_state_flag,invalid_us_billing_state_flag,review_billing_state_clean_flag,billing_country_clean_null_flag,billing_country_clean_number_flag,shipping_state,shipping_state_clean,shipping_country,shipping_country_clean,shipping_postal_code,shipping_postal_code_clean,shipping_state_clean_null_flag,shipping_state_clean_number_flag,invalid_ca_shipping_state_flag,invalid_au_shipping_state_flag,invalid_us_shipping_state_flag,review_shipping_state_clean_flag,shipping_country_clean_null_flag,shipping_country_clean_number_flag,review_invalid_billing_postal_code_clean_flag,review_invalid_shipping_postal_code_clean_flag,final_review_postal_code_flag,final_review_all_address_columns_flag,system_modstamp,account_id,named_account,billing_geolocation,shipping_geolocation,billing_address_complete,billing_address_validation_status_msg,shipping_address_complete,shipping_address_validation_status_msg,master_record_id,type,billing_latitude,billing_longitude,billing_geocode_accuracy,shipping_latitude,shipping_longitude,shipping_geocode_accuracy,phone,account_number,website,created_date,created_by_id,last_modified_date,last_modified_by_id,archived,billing_county,shipping_county,billing_address_validation_status,hash_key,ingested_timestamp,2,address_last_cleaned_timestamp,name_clean_additional_data,billing_street_clean_additional_data,shipping_street_clean_additional_data,review_billing_address_flag,review_shipping_address_flag,true 
# MAGIC from ${source_catalog}.clean.sf_account_clean_address where final_review_postal_code_flag=true and needs_manual_review=false and final_clean_flag=0 and sid_id not in (select distinct sid_id from ${source_catalog}.tmp.sf_account_clean_address_for_review where needs_manual_review = true);
# MAGIC
# MAGIC delete from ${source_catalog}.clean.sf_account_clean_address where final_review_postal_code_flag=true and needs_manual_review=false and final_clean_flag=0;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from ${source_catalog}.clean.sf_account_clean_address where sid_id in (select distinct sid_id from ${source_catalog}.tmp.sf_account_clean_address_for_review where needs_manual_review = true and final_review_postal_code_flag=true) and needs_manual_review=false

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from ${source_catalog}.clean.sf_account_clean_address where final_review_postal_code_flag=true and needs_manual_review=false and final_clean_flag=0;

# COMMAND ----------

# Define country-specific invalid postal codes
invalid_postal_codes_by_country = {
    'BE': ['1853 GRIMBERGEN','B1000' ],
    'BR': ['4578','4578000','3116000', '4007910' ,'4543' ,'4543121', '4562' ,'4601000', '6273070' ,'6419300', 
           '9781220','CEP 05348-000','O4578','SP','SP045-000'],
    'CN': ['10002' ,'20002' ,'200433' ,'45/F' ,'99907'],
    'IE': ['(01) 2083111','1234' ,'68' ,'904141', 'BT480-BF','D02' ,'D02 W', 'D08 E' ,'D2' ,'R95 P', 'T45P6'],
    'IT': ['00054 MACCARESE' ,'12' ,'148' ,'185' ,'9604' ,'PN 33'],
    'MX': ['1600','1790','2090','3700','3730','3900','4257','4310','5348','5410','6100','6700','6720','9360',
           'C.P. 44160'],
    'NZ': ['624'],
    'PL': ['40-11'],
    'SG': ['17910','49145','079914 SG','16920','18897','18936','22946','3919','48619','48623','57615' ,'60991' ,   
           '65456' ,'66964' ,'68902'],
    'ZA': ['39'],
    'ES': ['8019','15143 ARTEIXO','8174','CP 28223'],
    'SE': ['431 3','SE417']
}

# Country abbreviations mapped to country codes
country_groups = {
    'BE': ['BE', 'BEL', 'BELGIUM'],
    'BR': ['BR', 'BRA', 'BRAZIL'],
    'CN': ['CN', 'CHN', 'CHINA'],
    'IE': ['IE', 'IRL', 'IRELAND'],
    'IT': ['IT', 'ITA', 'ITALY'],
    'MX': ['MX', 'MEX', 'MEXICO'],
    'NZ': ['NZ', 'NZL', 'NEW ZEALAND'],
    'PL': ['PL', 'POL', 'POLAND'],
    'SG': ['SG', 'SGP', 'SINGAPORE'],
    'ZA': ['ZA', 'ZAF', 'SOUTH AFRICA'],
    'ES': ['ES', 'ESP', 'SPAIN'],
    'SE': ['SE', 'SWE', 'SWEDEN']
}



# COMMAND ----------

cleaned_df_upto_country = spark.sql(f"""
select * from {source_catalog}.clean.sf_account_clean_address  where final_clean_flag=0
""")

display(cleaned_df_upto_country.count())

# COMMAND ----------

# Apply the function for Billing and Shipping
try:
    df_postal_check_billing = check_invalid_code(cleaned_df_upto_country, 'billing_postal_code_clean', 'billing_country')
    df_postal_check_shipping = check_invalid_code(df_postal_check_billing,'shipping_postal_code_clean',    'shipping_country')
except Exception as e:
    print({str(e)})

# COMMAND ----------

# Apply the function for Billing and Shipping
try:
    df_postal_check_shipping = check_valid_code(df_postal_check_shipping, 'billing_postal_code_clean', 'billing_country')
    df_postal_check_shipping = check_valid_code(df_postal_check_shipping,'shipping_postal_code_clean',    'shipping_country')
except Exception as e:
   print({str(e)})

# COMMAND ----------

# Ensure final review flag column exists, otherwise initialize with False
if 'final_review_postal_code_flag' not in df_postal_check_shipping.columns:
        df_postal_check_shipping = df_postal_check_shipping.withColumn('final_review_postal_code_flag', lit(False))
df_postal_check_shipping = df_postal_check_shipping.withColumn(
    "final_review_postal_code_flag",
    (col("review_invalid_billing_postal_code_clean_flag").cast('boolean') | col("review_invalid_shipping_postal_code_clean_flag").cast('boolean'))
)
df_postal_check_shipping = df_postal_check_shipping.filter(col("final_review_postal_code_flag")==True)

display(df_postal_check_shipping)


# COMMAND ----------

import pyspark.sql.functions as F

df_postal_check_shipping = df_postal_check_shipping.withColumn(
    "needs_manual_review",
    F.when(
        (F.col("billing_postal_code").isNotNull() & F.col("billing_postal_code_clean").isNull()) |
        (F.col("shipping_postal_code").isNotNull() & F.col("shipping_postal_code_clean").isNull()) |
        (F.col("final_review_postal_code_flag")==True),
        True
    ).otherwise(False)
).withColumn(
    "final_clean_flag",
    F.when(F.col("needs_manual_review") == True, 2).otherwise(F.col("final_clean_flag"))
)

display(df_postal_check_shipping.count()) #7016
display(df_postal_check_shipping) #7016

# COMMAND ----------

from delta.tables import DeltaTable

target_dt = DeltaTable.forName(spark, f'{source_catalog}.tmp.sf_account_clean_address_for_review')

target_columns = target_dt.toDF().columns
#display(target_df.count())

# Perform Merge Operation on Delta table
target_dt.alias("tgt").merge(
    df_postal_check_shipping.alias("src"),
    "tgt.sid_id = src.sid_id"
).whenNotMatchedInsert(
    values={col: f"src.{col}" for col in df_postal_check_shipping.columns if col in target_columns}  # Insert only columns that exist in target table
).execute()

# COMMAND ----------

from delta.tables import DeltaTable

target_dt =  DeltaTable.forName(spark,f'{source_catalog}.clean.sf_account_clean_address_reviewed')
                                                                                                      
# Perform Merge Operation on Delta table
target_dt.alias("tgt").merge(
        df_postal_check_shipping.alias("src"),
        "tgt.sid_id = src.sid_id"
    ).whenMatchedUpdate(
        condition="tgt.hash_key != src.hash_key",  # Update only if hash_key differs 
        set={col: f"src.{col}" for col in df_postal_check_shipping.columns if col in target_columns}  # Replace only columns that exist in target table
    ).whenNotMatchedInsert(
        values={col: f"src.{col}" for col in df_postal_check_shipping.columns if col in target_columns}  # Insert only columns that exist in target table
    ).execute()

# COMMAND ----------

ids_to_delete = [row.sid_id for row in df_postal_check_shipping.select("sid_id").collect()]
display(len(ids_to_delete))

# COMMAND ----------

ids_to_delete_str = ','.join([f"'{id}'" for id in ids_to_delete])
delete_sql_query = f"DELETE FROM {source_catalog}.clean.sf_account_clean_address  WHERE sid_id IN ({ids_to_delete_str})"
spark.sql(delete_sql_query)
