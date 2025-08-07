# Databricks notebook source
dbutils.widgets.text("source_catalog", "", "Catalog")
source_catalog = dbutils.widgets.get("source_catalog")

# COMMAND ----------

# MAGIC %md
# MAGIC #####city

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from ${source_catalog}.clean.sf_account_clean_address where
# MAGIC (
# MAGIC (billing_city is not null and billing_city_clean='') or
# MAGIC (shipping_city is not null and shipping_city_clean='')
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE ${source_catalog}.clean.sf_account_clean_address
# MAGIC SET
# MAGIC     billing_city_clean = CASE
# MAGIC         WHEN billing_city IS NOT NULL AND billing_city_clean = '' THEN NULL
# MAGIC         ELSE billing_city_clean
# MAGIC     END,
# MAGIC     shipping_city_clean = CASE
# MAGIC         WHEN shipping_city IS NOT NULL AND shipping_city_clean = '' THEN NULL
# MAGIC         ELSE shipping_city_clean
# MAGIC     END
# MAGIC WHERE
# MAGIC     (billing_city IS NOT NULL AND billing_city_clean = '' )
# MAGIC     OR
# MAGIC     (shipping_city IS NOT NULL AND shipping_city_clean = '');

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from ${source_catalog}.tmp.sf_account_clean_address_for_review where
# MAGIC (
# MAGIC (billing_city is not null and billing_city_clean='') or
# MAGIC (shipping_city is not null and shipping_city_clean='')
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE ${source_catalog}.tmp.sf_account_clean_address_for_review
# MAGIC SET
# MAGIC     billing_city_clean = CASE
# MAGIC         WHEN billing_city IS NOT NULL AND billing_city_clean = '' THEN NULL
# MAGIC         ELSE billing_city_clean
# MAGIC     END,
# MAGIC     shipping_city_clean = CASE
# MAGIC         WHEN shipping_city IS NOT NULL AND shipping_city_clean = '' THEN NULL
# MAGIC         ELSE shipping_city_clean
# MAGIC     END
# MAGIC WHERE
# MAGIC     (billing_city IS NOT NULL AND billing_city_clean = '' )
# MAGIC     OR
# MAGIC     (shipping_city IS NOT NULL AND shipping_city_clean = '');

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from ${source_catalog}.clean.melissa_api_final where input_billing_city_clean='' or input_shipping_city_clean=''

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE ${source_catalog}.clean.melissa_api_final
# MAGIC SET
# MAGIC     input_billing_city_clean = CASE
# MAGIC         WHEN input_billing_city_clean = '' THEN NULL
# MAGIC         ELSE input_billing_city_clean
# MAGIC     END,
# MAGIC     input_shipping_city_clean = CASE
# MAGIC         WHEN input_shipping_city_clean = '' THEN NULL
# MAGIC         ELSE input_shipping_city_clean
# MAGIC     END
# MAGIC WHERE
# MAGIC     input_billing_city_clean = ''
# MAGIC     OR
# MAGIC     input_shipping_city_clean = '';

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from ${source_catalog}.tmp.sf_account_melissa_review where input_billing_city_clean='' or input_shipping_city_clean=''

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE ${source_catalog}.tmp.sf_account_melissa_review
# MAGIC SET
# MAGIC     input_billing_city_clean = CASE
# MAGIC         WHEN input_billing_city_clean = '' THEN NULL
# MAGIC         ELSE input_billing_city_clean
# MAGIC     END,
# MAGIC     input_shipping_city_clean = CASE
# MAGIC         WHEN input_shipping_city_clean = '' THEN NULL
# MAGIC         ELSE input_shipping_city_clean
# MAGIC     END
# MAGIC WHERE
# MAGIC     input_billing_city_clean = ''
# MAGIC     OR
# MAGIC     input_shipping_city_clean = '';

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE ${source_catalog}.clean.melissa_input
# MAGIC SET
# MAGIC     billing_city_clean = CASE
# MAGIC         WHEN billing_city_clean = '' THEN NULL
# MAGIC         ELSE billing_city_clean
# MAGIC     END,
# MAGIC     shipping_city_clean = CASE
# MAGIC         WHEN shipping_city_clean = '' THEN NULL
# MAGIC         ELSE shipping_city_clean
# MAGIC     END
# MAGIC WHERE
# MAGIC     billing_city_clean = ''
# MAGIC     OR
# MAGIC     shipping_city_clean = '';

# COMMAND ----------

# MAGIC %sql
# MAGIC update ${source_catalog}.clean.melissa_output_billing_raw set billing_city_clean=null 
# MAGIC where billing_city_clean=''

# COMMAND ----------

# MAGIC %sql
# MAGIC update ${source_catalog}.clean.melissa_output_billing_all set billing_city_clean=null 
# MAGIC where billing_city_clean=''

# COMMAND ----------

# MAGIC %sql
# MAGIC update ${source_catalog}.clean.melissa_output_billing_final set input_billing_city_clean=null 
# MAGIC where input_billing_city_clean=''

# COMMAND ----------

# MAGIC %sql
# MAGIC update ${source_catalog}.clean.melissa_output_shipping_raw set shipping_city_clean=null 
# MAGIC where shipping_city_clean=''

# COMMAND ----------

# MAGIC %sql
# MAGIC update ${source_catalog}.clean.melissa_output_shipping_all set shipping_city_clean=null 
# MAGIC where shipping_city_clean=''

# COMMAND ----------

# MAGIC %sql
# MAGIC update ${source_catalog}.clean.melissa_output_shipping_final set input_shipping_city_clean=null 
# MAGIC where input_shipping_city_clean=''

# COMMAND ----------

# MAGIC %md
# MAGIC #####postal code

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from ${source_catalog}.clean.sf_account_clean_address where
# MAGIC (
# MAGIC (billing_postal_code is not null and billing_postal_code_clean='') 
# MAGIC or
# MAGIC (shipping_postal_code is not null and shipping_postal_code_clean='')
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE ${source_catalog}.clean.sf_account_clean_address
# MAGIC SET
# MAGIC     billing_postal_code_clean = CASE
# MAGIC         WHEN billing_postal_code IS NOT NULL AND billing_postal_code_clean = '' THEN NULL
# MAGIC         ELSE billing_postal_code_clean
# MAGIC     END,
# MAGIC     shipping_postal_code_clean = CASE
# MAGIC         WHEN shipping_postal_code IS NOT NULL AND shipping_postal_code_clean = '' THEN NULL
# MAGIC         ELSE shipping_postal_code_clean
# MAGIC     END
# MAGIC WHERE
# MAGIC     (billing_postal_code IS NOT NULL AND billing_postal_code_clean = '')
# MAGIC     OR
# MAGIC     (shipping_postal_code IS NOT NULL AND shipping_postal_code_clean = '');

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from ${source_catalog}.tmp.sf_account_clean_address_for_review where
# MAGIC (
# MAGIC (billing_postal_code is not null and billing_postal_code_clean='') or
# MAGIC (shipping_postal_code is not null and shipping_postal_code_clean='')
# MAGIC ) 

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE ${source_catalog}.tmp.sf_account_clean_address_for_review
# MAGIC SET
# MAGIC     billing_postal_code_clean = CASE
# MAGIC         WHEN billing_postal_code IS NOT NULL AND billing_postal_code_clean = '' THEN NULL
# MAGIC         ELSE billing_postal_code_clean
# MAGIC     END,
# MAGIC     shipping_postal_code_clean = CASE
# MAGIC         WHEN shipping_postal_code IS NOT NULL AND shipping_postal_code_clean = '' THEN NULL
# MAGIC         ELSE shipping_postal_code_clean
# MAGIC     END
# MAGIC WHERE
# MAGIC     (billing_postal_code IS NOT NULL AND billing_postal_code_clean = '')
# MAGIC     OR
# MAGIC     (shipping_postal_code IS NOT NULL AND shipping_postal_code_clean = '');

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from ${source_catalog}.clean.melissa_api_final where input_billing_postal_code_clean='' or input_shipping_postal_code_clean=''

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE ${source_catalog}.clean.melissa_api_final
# MAGIC SET
# MAGIC     input_billing_postal_code_clean = CASE
# MAGIC         WHEN input_billing_postal_code_clean = '' THEN NULL
# MAGIC         ELSE input_billing_postal_code_clean
# MAGIC     END,
# MAGIC     input_shipping_postal_code_clean = CASE
# MAGIC         WHEN input_shipping_postal_code_clean = '' THEN NULL
# MAGIC         ELSE input_shipping_postal_code_clean
# MAGIC     END
# MAGIC WHERE
# MAGIC     input_billing_postal_code_clean = ''
# MAGIC     OR
# MAGIC     input_shipping_postal_code_clean = '';

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from ${source_catalog}.tmp.sf_account_melissa_review where input_billing_postal_code_clean='' 
# MAGIC or input_shipping_postal_code_clean=''

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE ${source_catalog}.tmp.sf_account_melissa_review
# MAGIC SET
# MAGIC     input_billing_postal_code_clean = CASE
# MAGIC         WHEN input_billing_postal_code_clean = '' THEN NULL
# MAGIC         ELSE input_billing_postal_code_clean
# MAGIC     END,
# MAGIC     input_shipping_postal_code_clean = CASE
# MAGIC         WHEN input_shipping_postal_code_clean = '' THEN NULL
# MAGIC         ELSE input_shipping_postal_code_clean
# MAGIC     END
# MAGIC WHERE
# MAGIC     input_billing_postal_code_clean = ''
# MAGIC     OR
# MAGIC     input_shipping_postal_code_clean = '';

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from ${source_catalog}.clean.melissa_input where 
# MAGIC billing_postal_code_clean='' 
# MAGIC or 
# MAGIC shipping_postal_code_clean=''

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE ${source_catalog}.clean.melissa_input
# MAGIC SET
# MAGIC     billing_postal_code_clean = CASE
# MAGIC         WHEN billing_postal_code_clean = '' THEN NULL
# MAGIC         ELSE billing_postal_code_clean
# MAGIC     END,
# MAGIC     shipping_postal_code_clean = CASE
# MAGIC         WHEN shipping_postal_code_clean = '' THEN NULL
# MAGIC         ELSE shipping_postal_code_clean
# MAGIC     END
# MAGIC WHERE
# MAGIC     billing_postal_code_clean = ''
# MAGIC     OR
# MAGIC     shipping_postal_code_clean = '';

# COMMAND ----------

# MAGIC %sql
# MAGIC update ${source_catalog}.clean.melissa_output_billing_raw set billing_postal_code_clean=null 
# MAGIC where billing_postal_code_clean=''

# COMMAND ----------

# MAGIC %sql
# MAGIC update ${source_catalog}.clean.melissa_output_billing_all set billing_postal_code_clean=null 
# MAGIC where billing_postal_code_clean=''

# COMMAND ----------

# MAGIC %sql
# MAGIC update ${source_catalog}.clean.melissa_output_billing_final set input_billing_postal_code_clean=null 
# MAGIC where input_billing_postal_code_clean=''

# COMMAND ----------

# MAGIC %sql
# MAGIC update ${source_catalog}.clean.melissa_output_shipping_raw set shipping_postal_code_clean=null 
# MAGIC where shipping_postal_code_clean=''

# COMMAND ----------

# MAGIC %sql
# MAGIC update ${source_catalog}.clean.melissa_output_shipping_all set shipping_postal_code_clean=null 
# MAGIC where shipping_postal_code_clean=''

# COMMAND ----------

# MAGIC %sql
# MAGIC update ${source_catalog}.clean.melissa_output_shipping_final set input_shipping_postal_code_clean=null 
# MAGIC where input_shipping_postal_code_clean=''

# COMMAND ----------

# MAGIC %md
# MAGIC #####state

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from ${source_catalog}.clean.sf_account_clean_address where((billing_state is not null and billing_state_clean='') or(shipping_state is not null and shipping_state_clean=''))

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from ${source_catalog}.tmp.sf_account_clean_address_for_review where
# MAGIC (
# MAGIC (billing_state is not null and billing_state_clean='') or
# MAGIC (shipping_state is not null and shipping_state_clean='')
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE ${source_catalog}.clean.sf_account_clean_address
# MAGIC SET
# MAGIC     billing_state_clean = CASE
# MAGIC         WHEN billing_state IS NOT NULL AND billing_state_clean = '' THEN NULL
# MAGIC         ELSE billing_state_clean
# MAGIC     END,
# MAGIC     shipping_state_clean = CASE
# MAGIC         WHEN shipping_state IS NOT NULL AND shipping_state_clean = '' THEN NULL
# MAGIC         ELSE shipping_state_clean
# MAGIC     END
# MAGIC WHERE
# MAGIC     (billing_state IS NOT NULL AND billing_state_clean = '')
# MAGIC     OR
# MAGIC     (shipping_state IS NOT NULL AND shipping_state_clean = '')

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE ${source_catalog}.tmp.sf_account_clean_address_for_review
# MAGIC SET
# MAGIC     billing_state_clean = CASE
# MAGIC         WHEN billing_state IS NOT NULL AND billing_state_clean = '' THEN NULL
# MAGIC         ELSE billing_state_clean
# MAGIC     END,
# MAGIC     shipping_state_clean = CASE
# MAGIC         WHEN shipping_state IS NOT NULL AND shipping_state_clean = '' THEN NULL
# MAGIC         ELSE shipping_state_clean
# MAGIC     END
# MAGIC WHERE
# MAGIC     (billing_state IS NOT NULL AND billing_state_clean = '')
# MAGIC     OR
# MAGIC     (shipping_state IS NOT NULL AND shipping_state_clean = '')

# COMMAND ----------

# MAGIC %md
# MAGIC #####street

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from ${source_catalog}.clean.sf_account_clean_address where
# MAGIC (
# MAGIC (billing_street is not null and billing_street_clean='') or
# MAGIC (shipping_street is not null and shipping_street_clean='')
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from ${source_catalog}.tmp.sf_account_clean_address_for_review where 
# MAGIC (
# MAGIC (billing_street is not null and billing_street_clean='') or
# MAGIC (shipping_street is not null and shipping_street_clean='')
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE ${source_catalog}.clean.sf_account_clean_address
# MAGIC SET
# MAGIC     billing_street_clean = CASE
# MAGIC         WHEN billing_street IS NOT NULL AND billing_street_clean = '' THEN NULL
# MAGIC         ELSE billing_street_clean
# MAGIC     END,
# MAGIC     shipping_street_clean = CASE
# MAGIC         WHEN shipping_street IS NOT NULL AND shipping_street_clean = '' THEN NULL
# MAGIC         ELSE shipping_street_clean
# MAGIC     END
# MAGIC WHERE
# MAGIC     (billing_street IS NOT NULL AND billing_street_clean = '')
# MAGIC     OR
# MAGIC     (shipping_street IS NOT NULL AND shipping_street_clean = '')

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE ${source_catalog}.tmp.sf_account_clean_address_for_review
# MAGIC SET
# MAGIC     billing_street_clean = CASE
# MAGIC         WHEN billing_street IS NOT NULL AND billing_street_clean = '' THEN NULL
# MAGIC         ELSE billing_street_clean
# MAGIC     END,
# MAGIC     shipping_street_clean = CASE
# MAGIC         WHEN shipping_street IS NOT NULL AND shipping_street_clean = '' THEN NULL
# MAGIC         ELSE shipping_street_clean
# MAGIC     END
# MAGIC WHERE
# MAGIC     (billing_street IS NOT NULL AND billing_street_clean = '')
# MAGIC     OR
# MAGIC     (shipping_street IS NOT NULL AND shipping_street_clean = '')

# COMMAND ----------

# MAGIC %md
# MAGIC #####country 

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from ${source_catalog}.clean.sf_account_clean_address where
# MAGIC (
# MAGIC (billing_country is not null and billing_country_clean='') or
# MAGIC (shipping_country is not null and shipping_country_clean='')
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE ${source_catalog}.clean.sf_account_clean_address
# MAGIC SET
# MAGIC     billing_country_clean = CASE
# MAGIC         WHEN billing_country IS NOT NULL AND billing_country_clean = '' THEN NULL
# MAGIC         ELSE billing_country_clean
# MAGIC     END,
# MAGIC     shipping_country_clean = CASE
# MAGIC         WHEN shipping_country IS NOT NULL AND shipping_country_clean = '' THEN NULL
# MAGIC         ELSE shipping_country_clean
# MAGIC     END
# MAGIC WHERE
# MAGIC     (billing_country IS NOT NULL AND billing_country_clean = '')
# MAGIC     OR
# MAGIC     (shipping_country IS NOT NULL AND shipping_country_clean = '')

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE ${source_catalog}.tmp.sf_account_clean_address_for_review
# MAGIC SET
# MAGIC     billing_country_clean = CASE
# MAGIC         WHEN billing_country IS NOT NULL AND billing_country_clean = '' THEN NULL
# MAGIC         ELSE billing_country_clean
# MAGIC     END,
# MAGIC     shipping_country_clean = CASE
# MAGIC         WHEN shipping_country IS NOT NULL AND shipping_country_clean = '' THEN NULL
# MAGIC         ELSE shipping_country_clean
# MAGIC     END
# MAGIC WHERE
# MAGIC     (billing_country IS NOT NULL AND billing_country_clean = '')
# MAGIC     OR
# MAGIC     (shipping_country IS NOT NULL AND shipping_country_clean = '')
