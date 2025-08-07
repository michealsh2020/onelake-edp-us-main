# Databricks notebook source
# MAGIC %md ##Creating widgets for input parameters

# COMMAND ----------

dbutils.widgets.text("catalog", "", "Catalog")
dbutils.widgets.text("storage_account", "", "storage account")

# COMMAND ----------

# MAGIC %md ##Read the input parameters

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
storage_account = dbutils.widgets.get("storage_account")

# COMMAND ----------

# MAGIC %md ##Creating Salesforce Account Tables

# COMMAND ----------

# MAGIC %md ###Creating landing Schema tables

# COMMAND ----------

# MAGIC %md ###landing.sf_account

# COMMAND ----------

query = f"""
CREATE TABLE IF NOT EXISTS {catalog}.landing.sf_account (
Id STRING NOT NULL,
Account_Id__c STRING,
Account_Plans__c DOUBLE,
Account_Type__c STRING,
Active_Prospecting_By__c STRING,
Alignment_Score__c DOUBLE,
AnnualRevenue DOUBLE,
Archived__c STRING,
BillingCity STRING,
BillingCountry STRING,
BillingPostalCode STRING,
BillingState STRING,
BillingStreet STRING,
BO_ID__c STRING,
Company_Size__c STRING,
Contact_and_Referral_Score__c DOUBLE,
CreatedById STRING,
CreatedDate timestamp,
CurrencyIsoCode STRING,
DUNS_NUMBER__c STRING,
Expires__c DOUBLE,
Fortune_500__c BOOLEAN,
Fortune_500_Y_or_N__c STRING,
Global_2000__c BOOLEAN,
Global_2000_Y_N__c STRING,
ILT__c STRING,
Knowledge_Center_Editor__c STRING,
Knowledge_Centers__c STRING,
LastActivityDate timestamp,
LastModifiedById STRING,
LastModifiedDate timestamp,
License_Management__c STRING,
Lock_Account_Type__c STRING,
MasterRecordId STRING,
Name STRING,
NumberOfEmployees INT,
of_Opportunities__c INT,
OLSA__c STRING,
Opportunity_Stage__c STRING,
Original_Pilot__c STRING,
Original_Pilot_Yes_No__c STRING,
Other_Partner__c STRING,
OwnerId STRING,
Ownership STRING,
ParentId STRING,
Phone STRING,
Population_Score__c DOUBLE,
Priority_Score__c DOUBLE,
Project_Defined__c STRING,
PSCRM_COMPANY_ID__c STRING,
RecordTypeId STRING,
Renewal_Evaluation_Required2__c STRING,
Revenue_Gen__c STRING,
RRFs__c STRING,
RRFs_Complete__c STRING,
ShippingCity STRING,
ShippingCountry STRING,
ShippingPostalCode STRING,
ShippingState STRING,
ShippingStreet STRING,
Sic STRING,
Skillport__c STRING,
SLA__c STRING,
Status__c STRING,
SystemModstamp timestamp,
TickerSymbol STRING,
TPLMS__c STRING,
Type STRING,
Website STRING,
ss_NDA__c STRING,
ss_Reference_Account__c STRING,
Top_100__c STRING,
Account_Industry_Account__c STRING,
Band_calculated__c STRING,
Geography__c STRING,
Premier_Support_Customer__c STRING,
Type_Platform__c STRING,
Type_Toolbook__c STRING,
Band__c STRING,
IsCustomerPortal BOOLEAN,
Sumt_DBA__c STRING,
SumTotal_Account_Id__c STRING,
SumTotal_Client__c STRING,
IsPartner BOOLEAN,
Support_Account__c STRING,
Nbr_of_Account_Team_Members__c INT,
PS_SPIF_Ineligible__c STRING,
Customer_Key__c STRING,
Industry STRING,
Won_Opportunities__c INT,
No_Opportunities_Closed__c INT,
Named_Account__c STRING,
Market_Segment_Value__c STRING,
Market_Segment__c STRING,
DiscoverOrg_Employees__c INT,
DB_State_Province_Code__c STRING,
DB_Revenue_Value__c DOUBLE,
DB_Major_Industry__c STRING,
DB_Employees_Here__c DOUBLE,
DB_Duplicate__c STRING,
DB_Duns__c STRING,
DB_Country_Code__c STRING,
DB_Business_Name__c STRING,
CountOfOpportunitiesOpen__c INT,
Count_of_SumTotal_CSMs__c INT,
Count_of_SumTotal_CSDs__c INT,
Conga_Output_Format__c STRING,
DOZISF__ZoomInfo_Id__c STRING,
Current_Deployment__c STRING,
G250__c STRING,
Account_Segment__c STRING,
Customer_Persona__c STRING,
IsDeleted BOOLEAN,
Billing_Address_Complete__c STRING,
Billing_Address_Validation_Status__c STRING,
Billing_Address_Validation_Status_Msg__c STRING,
Billing_Country_2Char_Code__c STRING,
Billing_County__c STRING,
BillingAddress STRING,
BillingGeocodeAccuracy STRING,
BillingGeolocation__c STRING,
BillingLatitude DOUBLE,
BillingLongitude DOUBLE,
Fax STRING,
Shipping_Address_Complete__c STRING,
GK_Account__c STRING,
GK_Account_Classification__c STRING,
GK_Salesforce_ID__c STRING,
GKNA_Created_By_User__c STRING,
GKNA_Created_By_User_ID__c STRING,
Shipping_Address_Validation_Status_Msg__c STRING,
ShippingGeolocation__c STRING,
Shipping_Country_2Char_Code__c BOOLEAN,
ShippingLatitude DOUBLE,
ShippingLongitude DOUBLE,
ShippingGeocodeAccuracy STRING,
AccountNumber STRING,
Shipping_County__c STRING,
ingested_period STRING,
ingested_date DATE,
ingested_timestamp TIMESTAMP,
hash_key STRING)
USING delta
"""

spark.sql(query)

# COMMAND ----------

# MAGIC %md ###landing.audit_logs

# COMMAND ----------

query = f"""
CREATE TABLE IF NOT EXISTS {catalog}.landing.audit_logs (
table_name STRING,
operation STRING,
record_count BIGINT,
start_time TIMESTAMP,
end_time TIMESTAMP,
status STRING,
error_message STRING,
run_id STRING,
task_name STRING,
run_time DOUBLE)
USING delta
"""

spark.sql(query)

# COMMAND ----------

# MAGIC %md
# MAGIC ## landing.watermark_config

# COMMAND ----------

query = f"""
CREATE TABLE IF NOT EXISTS {catalog}.landing.watermark_config (
id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
watermark_id BIGINT,
watermark_column_1 STRING,
watermark_value_1 TIMESTAMP,
watermark_column_2 STRING,
watermark_value_2 TIMESTAMP,
record_created_by STRING,
record_created_date TIMESTAMP,
record_updated_by STRING,
record_updated_date TIMESTAMP
)
USING delta
"""

spark.sql(query)

# COMMAND ----------

# MAGIC %md ###Creating Clean Schema Tables

# COMMAND ----------

# MAGIC %md ###clean.sf_account

# COMMAND ----------

query = f"""
CREATE TABLE IF NOT EXISTS {catalog}.clean.sf_account (
sid_id STRING NOT NULL,
account_id STRING,
account_plans DOUBLE,
account_type STRING,
active_prospecting_by STRING,
alignment_score DOUBLE,
annual_revenue DOUBLE,
archived STRING,
billing_city STRING,
billing_country STRING,
billing_postal_code STRING,
billing_state STRING,
billing_street STRING,
bo_id STRING,
company_size STRING,
contact_and_referral_score DOUBLE,
created_by_id STRING,
created_date TIMESTAMP,
currency_iso_code STRING,
duns_number STRING,
expires DOUBLE,
fortune_500 BOOLEAN,
fortune_500_y_or_n STRING,
global_2000 BOOLEAN,
global_2000_y_n STRING,
ilt STRING,
knowledge_center_editor STRING,
knowledge_centers STRING,
last_activity_date TIMESTAMP,
last_modified_by_id STRING,
last_modified_date TIMESTAMP,
license_management STRING,
lock_account_type STRING,
master_record_id STRING,
name STRING,
number_of_employees INT,
of_opportunities INT,
olsa STRING,
opportunity_stage STRING,
original_pilot STRING,
original_pilot_yes_no STRING,
other_partner STRING,
owner_id STRING,
ownership STRING,
parent_id STRING,
phone STRING,
population_score DOUBLE,
priority_score DOUBLE,
project_defined STRING,
pscrm_company_id STRING,
record_type_id STRING,
renewal_evaluation_required2 STRING,
revenue_gen STRING,
rrfs STRING,
rrfs_complete STRING,
shipping_city STRING,
shipping_country STRING,
shipping_postal_code STRING,
shipping_state STRING,
shipping_street STRING,
sic STRING,
skillport STRING,
sla STRING,
status STRING,
system_modstamp TIMESTAMP,
ticker_symbol STRING,
tplms STRING,
type STRING,
website STRING,
ss_nda STRING,
ss_reference_account STRING,
top_100 STRING,
account_industry_account STRING,
band_calculated STRING,
geography STRING,
premier_support_customer STRING,
type_platform STRING,
type_toolbook STRING,
band STRING,
is_customer_portal BOOLEAN,
sumt_dba STRING,
sum_total_account_id STRING,
sum_total_client STRING,
is_partner BOOLEAN,
support_account STRING,
nbr_of_account_team_members INT,
ps_spif_ineligible STRING,
customer_key STRING,
industry STRING,
won_opportunities INT,
no_opportunities_closed INT,
named_account STRING,
market_segment_value STRING,
market_segment STRING,
discover_org_employees INT,
db_state_province_code STRING,
db_revenue_value DOUBLE,
db_major_industry STRING,
db_employees_here DOUBLE,
db_duplicate STRING,
db_duns STRING,
db_country_code STRING,
db_business_name STRING,
count_of_opportunities_open INT,
count_of_sum_total_csms INT,
count_of_sum_total_csds INT,
conga_output_format STRING,
dozisf_zoom_info_id STRING,
current_deployment STRING,
g250 STRING,
account_segment STRING,
customer_persona STRING,
is_deleted BOOLEAN,
billing_address_complete STRING,
billing_address_validation_status STRING,
billing_address_validation_status_msg STRING,
billing_country_2char_code STRING,
billing_county STRING,
billing_address STRING,
billing_geocode_accuracy STRING,
billing_geolocation STRING,
billing_latitude DOUBLE,
billing_longitude DOUBLE,
fax STRING,
shipping_address_complete STRING,
gk_account STRING,
gk_account_classification STRING,
gk_salesforce_id STRING,
gkna_created_by_user STRING,
gkna_created_by_user_id STRING,
shipping_address_validation_status_msg STRING,
shipping_geolocation STRING,
shipping_country_2char_code BOOLEAN,
shipping_latitude DOUBLE,
shipping_longitude DOUBLE,
shipping_geocode_accuracy STRING,
account_number STRING,
shipping_county STRING,
ingested_timestamp TIMESTAMP,
hash_key STRING,
record_expiry_period STRING,
record_effective_date TIMESTAMP,
record_expiry_date TIMESTAMP,
record_current_flag INT)
USING delta         
"""

spark.sql(query)

# COMMAND ----------

# MAGIC %md ###clean.sf_account_clean_address

# COMMAND ----------

query = f"""
CREATE TABLE IF NOT EXISTS {catalog}.clean.sf_account_clean_address (
sid_id STRING NOT NULL,
name STRING,
name_clean STRING,
name_clean_acct_flag BOOLEAN,
name_clean_on_behalf_flag BOOLEAN,
name_clean_aka_flag BOOLEAN,
name_clean_asterisks_flag BOOLEAN,
name_clean_care_of_flag BOOLEAN,
name_clean_dba_flag BOOLEAN,
name_clean_donot_flag BOOLEAN,
name_clean_duplicate_flag BOOLEAN,
name_clean_fbo_flag BOOLEAN,
name_clean_length_flag BOOLEAN,
name_clean_none_na_flag BOOLEAN,
name_clean_student_flag BOOLEAN,
name_clean_test_flag BOOLEAN,
name_clean_trust_flag BOOLEAN,
name_clean_tbd_flag BOOLEAN,
review_name_clean_flag BOOLEAN,
billing_street STRING,
billing_street_clean STRING,
billing_street_clean_attn_flag BOOLEAN,
billing_street_clean_asterisks_flag BOOLEAN,
billing_street_clean_care_of_flag BOOLEAN,
billing_street_clean_tbd_flag BOOLEAN,
billing_street_clean_test_flag BOOLEAN,
review_billing_street_clean_flag BOOLEAN,
shipping_street STRING,
shipping_street_clean STRING,
shipping_street_clean_attn_flag BOOLEAN,
shipping_street_clean_asterisks_flag BOOLEAN,
shipping_street_clean_care_of_flag BOOLEAN,
shipping_street_clean_tbd_flag BOOLEAN,
shipping_street_clean_test_flag BOOLEAN,
review_shipping_street_clean_flag BOOLEAN,
billing_city STRING,
billing_city_clean STRING,
billing_city_clean_null_flag BOOLEAN,
billing_city_clean_number_flag BOOLEAN,
shipping_city STRING,
shipping_city_clean STRING,
shipping_city_clean_null_flag BOOLEAN,
shipping_city_clean_number_flag BOOLEAN,
billing_state STRING,
billing_state_clean STRING,
billing_country STRING,
billing_country_clean STRING,
billing_postal_code STRING,
billing_postal_code_clean STRING,
billing_state_clean_null_flag BOOLEAN,
billing_state_clean_number_flag BOOLEAN,
invalid_ca_billing_state_flag BOOLEAN,
invalid_au_billing_state_flag BOOLEAN,
invalid_us_billing_state_flag BOOLEAN,
review_billing_state_clean_flag BOOLEAN,
billing_country_clean_null_flag BOOLEAN,
billing_country_clean_number_flag BOOLEAN,
shipping_state STRING,
shipping_state_clean STRING,
shipping_country STRING,
shipping_country_clean STRING,
shipping_postal_code STRING,
shipping_postal_code_clean STRING,
shipping_state_clean_null_flag BOOLEAN,
shipping_state_clean_number_flag BOOLEAN,
invalid_ca_shipping_state_flag BOOLEAN,
invalid_au_shipping_state_flag BOOLEAN,
invalid_us_shipping_state_flag BOOLEAN,
review_shipping_state_clean_flag BOOLEAN,
shipping_country_clean_null_flag BOOLEAN,
shipping_country_clean_number_flag BOOLEAN,
review_invalid_billing_postal_code_clean_flag BOOLEAN,
review_invalid_shipping_postal_code_clean_flag BOOLEAN,
final_review_postal_code_flag BOOLEAN,
final_review_all_address_columns_flag BOOLEAN,
system_modstamp TIMESTAMP,
account_id STRING,
named_account STRING,
billing_geolocation STRING,
shipping_geolocation STRING,
billing_address_complete STRING,
billing_address_validation_status_msg STRING,
billing_country_2char_code STRING,
shipping_address_complete STRING,
shipping_address_validation_status_msg STRING,
shipping_country_2char_code BOOLEAN,
master_record_id STRING,
type STRING,
billing_latitude DOUBLE,
billing_longitude DOUBLE,
billing_geocode_accuracy STRING,
shipping_latitude DOUBLE,
shipping_longitude DOUBLE,
shipping_geocode_accuracy STRING,
phone STRING,
account_number STRING,
website STRING,
created_date TIMESTAMP,
created_by_id STRING,
last_modified_date TIMESTAMP,
last_modified_by_id STRING,
archived STRING,
billing_county STRING,
shipping_county STRING,
billing_address_validation_status STRING,
hash_key STRING,
ingested_timestamp TIMESTAMP,
final_clean_flag INT,
address_last_cleaned_timestamp TIMESTAMP,
name_clean_additional_data STRING,
billing_street_clean_additional_data STRING,
shipping_street_clean_additional_data STRING,
review_billing_address_flag BOOLEAN,
review_shipping_address_flag BOOLEAN,
needs_manual_review BOOLEAN)
USING delta      
"""
spark.sql(query)

# COMMAND ----------

# MAGIC %md ###clean.can_states

# COMMAND ----------

query = f"""
CREATE TABLE IF NOT EXISTS {catalog}.clean.can_states (
state	STRING,
state_abbreviation	STRING)
USING delta         
"""
spark.sql(query)

# COMMAND ----------

# MAGIC %md ###clean.aus_states

# COMMAND ----------

query = f"""
CREATE TABLE IF NOT EXISTS {catalog}.clean.aus_states (
state	STRING,
state_abbreviation	STRING)
USING delta         
"""
spark.sql(query)

# COMMAND ----------

# MAGIC %md ###clean.us_states

# COMMAND ----------

query = f"""
CREATE TABLE IF NOT EXISTS {catalog}.clean.us_states (
state	STRING,
state_abbreviation	STRING)
USING delta          
"""
spark.sql(query)

# COMMAND ----------

# MAGIC %md ###clean.melissa_input

# COMMAND ----------

query = f"""
CREATE TABLE IF NOT EXISTS {catalog}.clean.melissa_input (
sid_id	STRING NOT NULL,
name_clean	STRING,
billing_street_clean STRING,
shipping_street_clean STRING,
billing_city_clean STRING,
shipping_city_clean	STRING,
billing_state_clean	STRING,
shipping_state_clean STRING,
billing_country_clean STRING,
shipping_country_clean STRING,
billing_postal_code_clean STRING,
shipping_postal_code_clean STRING,
hash_key STRING,
ingested_timestamp TIMESTAMP,
final_clean_flag INT,
address_match_flag INT)
USING delta          
"""
spark.sql(query)

# COMMAND ----------

# MAGIC %md ###clean.melissa_api_final

# COMMAND ----------

query = f"""
CREATE TABLE IF NOT EXISTS {catalog}.clean.melissa_api_final (
sid_id STRING NOT NULL,
name_clean STRING,
hash_key STRING,
input_billing_street_clean STRING,
validated_billing_street_clean STRING,
input_billing_city_clean STRING,
validated_billing_city_clean STRING,
input_billing_state_clean STRING,
validated_billing_state_clean STRING,
input_billing_postal_code_clean STRING,
validated_billing_postal_code_clean STRING,
input_billing_country_clean STRING,
validated_billing_country_clean STRING,
validated_billing_full_address_clean STRING,
validated_verification_level_billing STRING,
input_shipping_street_clean STRING,
validated_shipping_street_clean STRING,
input_shipping_city_clean STRING,
validated_shipping_city_clean STRING,
input_shipping_state_clean STRING,
validated_shipping_state_clean STRING,
input_shipping_postal_code_clean STRING,
validated_shipping_postal_code_clean STRING,
input_shipping_country_clean STRING,
validated_shipping_country_clean STRING,
validated_shipping_full_address_clean STRING,
validated_verification_level_shipping STRING,
flag_billing_demographic INT,
flag_billing_state INT,
flag_billing_country INT,
flag_billing_null INT,
flag_shipping_demographic INT,
flag_shipping_state INT,
flag_shipping_country INT,
flag_shipping_null INT,
flag_revisit_billing INT,
flag_revisit_shipping INT,
date_of_cleaning DATE)
USING delta          
"""
spark.sql(query)

# COMMAND ----------

# MAGIC %md ###clean.melissa_output_billing_raw

# COMMAND ----------

query = f"""
CREATE TABLE IF NOT EXISTS {catalog}.clean.melissa_output_billing_raw (
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
billing_street_clean STRING,
billing_city_clean STRING,
billing_state_clean STRING,
billing_postal_code_clean STRING,
billing_country_clean STRING,
hash_key STRING)
USING delta           
"""
spark.sql(query)

# COMMAND ----------

# MAGIC %md ###clean.melissa_output_billing_all

# COMMAND ----------

query = f"""
CREATE TABLE IF NOT EXISTS {catalog}.clean.melissa_output_billing_all (
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
billing_street_clean STRING,
billing_city_clean STRING,
billing_state_clean STRING,
billing_postal_code_clean STRING,
billing_country_clean STRING,
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

# MAGIC %md ###clean.melissa_output_billing_final

# COMMAND ----------

query = f"""
CREATE TABLE IF NOT EXISTS {catalog}.clean.melissa_output_billing_final (
sid_id STRING NOT NULL,
name_clean STRING,
input_billing_street_clean STRING,
validated_billing_street_clean STRING,
input_billing_city_clean STRING,
validated_billing_city_clean STRING,
input_billing_state_clean STRING,
validated_billing_state_clean STRING,
input_billing_postal_code_clean STRING,
validated_billing_postal_code_clean STRING,
input_billing_country_clean STRING,
validated_billing_country_clean STRING,
validated_billing_full_address_clean STRING,
validated_verification_level_billing STRING,
hash_key STRING)
USING delta          
"""
spark.sql(query)

# COMMAND ----------

# MAGIC %md ###clean.melissa_output_shipping_raw

# COMMAND ----------

query = f"""
CREATE TABLE IF NOT EXISTS {catalog}.clean.melissa_output_shipping_raw (
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
hash_key STRING)
USING delta           
"""
spark.sql(query)

# COMMAND ----------

# MAGIC %md ###clean.melissa_output_shipping_all

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

# MAGIC %md ###clean.melissa_output_shipping_final

# COMMAND ----------

query = f"""
CREATE TABLE IF NOT EXISTS {catalog}.clean.melissa_output_shipping_final (
sid_id STRING NOT NULL,
name_clean STRING,
input_shipping_street_clean STRING,
validated_shipping_street_clean STRING,
input_shipping_city_clean STRING,
validated_shipping_city_clean STRING,
input_shipping_state_clean STRING,
validated_shipping_state_clean STRING,
input_shipping_postal_code_clean STRING,
validated_shipping_postal_code_clean STRING,
input_shipping_country_clean STRING,
validated_shipping_country_clean STRING,
validated_shipping_full_address_clean STRING,
validated_verification_level_shipping STRING,
hash_key STRING)
USING delta          
"""
spark.sql(query)

# COMMAND ----------

# MAGIC %md
# MAGIC #### clean.valid_postal_code_formats

# COMMAND ----------

query = f"""
CREATE TABLE IF NOT EXISTS {catalog}.clean.valid_postal_code_formats (
country	STRING,
valid_postal_code_format STRING)
USING delta          
"""
spark.sql(query)

# COMMAND ----------

# MAGIC %md ###clean.sf_account_clean_address_reviewed

# COMMAND ----------

query = f"""
CREATE TABLE IF NOT EXISTS {catalog}.clean.sf_account_clean_address_reviewed (
sid_id	STRING,
name	STRING,
name_clean	STRING,
name_clean_acct_flag	BOOLEAN,
name_clean_on_behalf_flag	BOOLEAN,
name_clean_aka_flag	BOOLEAN,
name_clean_asterisks_flag	BOOLEAN,
name_clean_care_of_flag	BOOLEAN,
name_clean_dba_flag	BOOLEAN,
name_clean_donot_flag	BOOLEAN,
name_clean_duplicate_flag	BOOLEAN,
name_clean_fbo_flag	BOOLEAN,
name_clean_length_flag	BOOLEAN,
name_clean_none_na_flag	BOOLEAN,
name_clean_student_flag	BOOLEAN,
name_clean_test_flag	BOOLEAN,
name_clean_trust_flag	BOOLEAN,
name_clean_tbd_flag	BOOLEAN,
review_name_clean_flag	BOOLEAN,
billing_street	STRING,
billing_street_clean	STRING,
billing_street_clean_attn_flag	BOOLEAN,
billing_street_clean_asterisks_flag	BOOLEAN,
billing_street_clean_care_of_flag	BOOLEAN,
billing_street_clean_tbd_flag	BOOLEAN,
billing_street_clean_test_flag	BOOLEAN,
review_billing_street_clean_flag	BOOLEAN,
shipping_street	STRING,
shipping_street_clean	STRING,
shipping_street_clean_attn_flag	BOOLEAN,
shipping_street_clean_asterisks_flag	BOOLEAN,
shipping_street_clean_care_of_flag	BOOLEAN,
shipping_street_clean_tbd_flag	BOOLEAN,
shipping_street_clean_test_flag	BOOLEAN,
review_shipping_street_clean_flag	BOOLEAN,
billing_city	STRING,
billing_city_clean	STRING,
billing_city_clean_null_flag	BOOLEAN,
billing_city_clean_number_flag	BOOLEAN,
shipping_city	STRING,
shipping_city_clean	STRING,
shipping_city_clean_null_flag	BOOLEAN,
shipping_city_clean_number_flag	BOOLEAN,
billing_state	STRING,
billing_state_clean	STRING,
billing_country	STRING,
billing_country_clean	STRING,
billing_postal_code	STRING,
billing_postal_code_clean	STRING,
billing_state_clean_null_flag	BOOLEAN,
billing_state_clean_number_flag	BOOLEAN,
invalid_ca_billing_state_flag	BOOLEAN,
invalid_au_billing_state_flag	BOOLEAN,
invalid_us_billing_state_flag	BOOLEAN,
review_billing_state_clean_flag	BOOLEAN,
billing_country_clean_null_flag	BOOLEAN,
billing_country_clean_number_flag	BOOLEAN,
shipping_state	STRING,
shipping_state_clean	STRING,
shipping_country	STRING,
shipping_country_clean	STRING,
shipping_postal_code	STRING,
shipping_postal_code_clean	STRING,
shipping_state_clean_null_flag	BOOLEAN,
shipping_state_clean_number_flag	BOOLEAN,
invalid_ca_shipping_state_flag	BOOLEAN,
invalid_au_shipping_state_flag	BOOLEAN,
invalid_us_shipping_state_flag	BOOLEAN,
review_shipping_state_clean_flag	BOOLEAN,
shipping_country_clean_null_flag	BOOLEAN,
shipping_country_clean_number_flag	BOOLEAN,
review_invalid_billing_postal_code_clean_flag	BOOLEAN,
review_invalid_shipping_postal_code_clean_flag	BOOLEAN,
final_review_postal_code_flag	BOOLEAN,
final_review_all_address_columns_flag	BOOLEAN,
system_modstamp	TIMESTAMP,
account_id	STRING,
named_account	STRING,
billing_geolocation	STRING,
shipping_geolocation	STRING,
billing_address_complete	STRING,
billing_address_validation_status_msg	STRING,
billing_country_2char_code	STRING,
shipping_address_complete	STRING,
shipping_address_validation_status_msg	STRING,
shipping_country_2char_code	BOOLEAN,
master_record_id	STRING,
type	STRING,
billing_latitude	DOUBLE,
billing_longitude	DOUBLE,
billing_geocode_accuracy	STRING,
shipping_latitude	DOUBLE,
shipping_longitude	DOUBLE,
shipping_geocode_accuracy	STRING,
phone	STRING,
account_number	STRING,
website	STRING,
created_date	TIMESTAMP,
created_by_id	STRING,
last_modified_date	TIMESTAMP,
last_modified_by_id	STRING,
archived	STRING,
billing_county	STRING,
shipping_county	STRING,
billing_address_validation_status	STRING,
hash_key	STRING,
ingested_timestamp	TIMESTAMP,
final_clean_flag	INT,
address_last_cleaned_timestamp	TIMESTAMP,
name_clean_additional_data	STRING,
billing_street_clean_additional_data	STRING,
shipping_street_clean_additional_data	STRING,
review_billing_address_flag	BOOLEAN,
review_shipping_address_flag	BOOLEAN,
needs_manual_review	BOOLEAN,
review_status	STRING,
reviewed_by  STRING)
USING delta 
"""
spark.sql(query)

# COMMAND ----------

# MAGIC %md ###Creating tmp Schema tables

# COMMAND ----------

# MAGIC %md ###tmp.sf_account_clean_address_for_review

# COMMAND ----------

query = f"""
CREATE TABLE IF NOT EXISTS {catalog}.tmp.sf_account_clean_address_for_review (
sid_id STRING NOT NULL,
name STRING,
name_clean STRING,
name_clean_acct_flag BOOLEAN,
name_clean_on_behalf_flag BOOLEAN,
name_clean_aka_flag BOOLEAN,
name_clean_asterisks_flag BOOLEAN,
name_clean_care_of_flag BOOLEAN,
name_clean_dba_flag BOOLEAN,
name_clean_donot_flag BOOLEAN,
name_clean_duplicate_flag BOOLEAN,
name_clean_fbo_flag BOOLEAN,
name_clean_length_flag BOOLEAN,
name_clean_none_na_flag BOOLEAN,
name_clean_student_flag BOOLEAN,
name_clean_test_flag BOOLEAN,
name_clean_trust_flag BOOLEAN,
name_clean_tbd_flag BOOLEAN,
review_name_clean_flag BOOLEAN,
billing_street STRING,
billing_street_clean STRING,
billing_street_clean_attn_flag BOOLEAN,
billing_street_clean_asterisks_flag BOOLEAN,
billing_street_clean_care_of_flag BOOLEAN,
billing_street_clean_tbd_flag BOOLEAN,
billing_street_clean_test_flag BOOLEAN,
review_billing_street_clean_flag BOOLEAN,
shipping_street STRING,
shipping_street_clean STRING,
shipping_street_clean_attn_flag BOOLEAN,
shipping_street_clean_asterisks_flag BOOLEAN,
shipping_street_clean_care_of_flag BOOLEAN,
shipping_street_clean_tbd_flag BOOLEAN,
shipping_street_clean_test_flag BOOLEAN,
review_shipping_street_clean_flag BOOLEAN,
billing_city STRING,
billing_city_clean STRING,
billing_city_clean_null_flag BOOLEAN,
billing_city_clean_number_flag BOOLEAN,
shipping_city STRING,
shipping_city_clean STRING,
shipping_city_clean_null_flag BOOLEAN,
shipping_city_clean_number_flag BOOLEAN,
billing_state STRING,
billing_state_clean STRING,
billing_country STRING,
billing_country_clean STRING,
billing_postal_code STRING,
billing_postal_code_clean STRING,
billing_state_clean_null_flag BOOLEAN,
billing_state_clean_number_flag BOOLEAN,
invalid_ca_billing_state_flag BOOLEAN,
invalid_au_billing_state_flag BOOLEAN,
invalid_us_billing_state_flag BOOLEAN,
review_billing_state_clean_flag BOOLEAN,
billing_country_clean_null_flag BOOLEAN,
billing_country_clean_number_flag BOOLEAN,
shipping_state STRING,
shipping_state_clean STRING,
shipping_country STRING,
shipping_country_clean STRING,
shipping_postal_code STRING,
shipping_postal_code_clean STRING,
shipping_state_clean_null_flag BOOLEAN,
shipping_state_clean_number_flag BOOLEAN,
invalid_ca_shipping_state_flag BOOLEAN,
invalid_au_shipping_state_flag BOOLEAN,
invalid_us_shipping_state_flag BOOLEAN,
review_shipping_state_clean_flag BOOLEAN,
shipping_country_clean_null_flag BOOLEAN,
shipping_country_clean_number_flag BOOLEAN,
review_invalid_billing_postal_code_clean_flag BOOLEAN,
review_invalid_shipping_postal_code_clean_flag BOOLEAN,
final_review_postal_code_flag BOOLEAN,
final_review_all_address_columns_flag BOOLEAN,
system_modstamp TIMESTAMP,
account_id STRING,
named_account STRING,
billing_geolocation STRING,
shipping_geolocation STRING,
billing_address_complete STRING,
billing_address_validation_status_msg STRING,
billing_country_2char_code STRING,
shipping_address_complete STRING,
shipping_address_validation_status_msg STRING,
shipping_country_2char_code BOOLEAN,
master_record_id STRING,
type STRING,
billing_latitude DOUBLE,
billing_longitude DOUBLE,
billing_geocode_accuracy STRING,
shipping_latitude DOUBLE,
shipping_longitude DOUBLE,
shipping_geocode_accuracy STRING,
phone STRING,
account_number STRING,
website STRING,
created_date TIMESTAMP,
created_by_id STRING,
last_modified_date TIMESTAMP,
last_modified_by_id STRING,
archived STRING,
billing_county STRING,
shipping_county STRING,
billing_address_validation_status STRING,
hash_key STRING,
ingested_timestamp TIMESTAMP,
final_clean_flag INT,
address_last_cleaned_timestamp TIMESTAMP,
name_clean_additional_data STRING,
billing_street_clean_additional_data STRING,
shipping_street_clean_additional_data STRING,
review_billing_address_flag BOOLEAN,
review_shipping_address_flag BOOLEAN,
needs_manual_review BOOLEAN)
USING delta          
"""
spark.sql(query)

# COMMAND ----------

# MAGIC %md ###tmp.sf_account_melissa_review

# COMMAND ----------

query = f"""
CREATE TABLE IF NOT EXISTS {catalog}.tmp.sf_account_melissa_review (
sid_id STRING NOT NULL,
name_clean STRING,
hash_key STRING,
input_billing_street_clean STRING,
validated_billing_street_clean STRING,
input_billing_city_clean STRING,
validated_billing_city_clean STRING,
input_billing_state_clean STRING,
validated_billing_state_clean STRING,
input_billing_postal_code_clean STRING,
validated_billing_postal_code_clean STRING,
input_billing_country_clean STRING,
validated_billing_country_clean STRING,
validated_billing_full_address_clean STRING,
validated_verification_level_billing STRING,
input_shipping_street_clean STRING,
validated_shipping_street_clean STRING,
input_shipping_city_clean STRING,
validated_shipping_city_clean STRING,
input_shipping_state_clean STRING,
validated_shipping_state_clean STRING,
input_shipping_postal_code_clean STRING,
validated_shipping_postal_code_clean STRING,
input_shipping_country_clean STRING,
validated_shipping_country_clean STRING,
validated_shipping_full_address_clean STRING,
validated_verification_level_shipping STRING,
flag_billing_demographic INT,
flag_billing_state INT,
flag_billing_country INT,
flag_billing_null INT,
flag_shipping_demographic INT,
flag_shipping_state INT,
flag_shipping_country INT,
flag_shipping_null INT,
flag_revisit_billing INT,
flag_revisit_shipping INT,
date_of_cleaning DATE)
USING delta         
"""
spark.sql(query)

# COMMAND ----------

# MAGIC %md ### Creating enriched Schema tables

# COMMAND ----------

# MAGIC %md ###enriched.sf_account_validated_address

# COMMAND ----------

#%sql
#describe table `mdm-dev`.enriched.sf_account_validated_address

# COMMAND ----------

query = f"""
CREATE TABLE IF NOT EXISTS {catalog}.enriched.sf_account_validated_address (
    sid_id STRING NOT NULL,
    account_id STRING,
    account_plans DOUBLE,
    account_type STRING,
    active_prospecting_by STRING,
    alignment_score DOUBLE,
    annual_revenue DOUBLE,
    archived STRING,
    billing_city STRING,
    billing_country STRING,
    billing_postal_code STRING,
    billing_state STRING,
    billing_street STRING,
    bo_id STRING,
    company_size STRING,
    contact_and_referral_score DOUBLE,
    created_by_id STRING,
    created_date TIMESTAMP,
    currency_iso_code STRING,
    duns_number STRING,
    expires DOUBLE,
    fortune_500 BOOLEAN,
    fortune_500_y_or_n STRING,
    global_2000 BOOLEAN,
    global_2000_y_n STRING,
    ilt STRING,
    knowledge_center_editor STRING,
    knowledge_centers STRING,
    last_activity_date TIMESTAMP,
    last_modified_by_id STRING,
    last_modified_date TIMESTAMP,
    license_management STRING,
    lock_account_type STRING,
    master_record_id STRING,
    name STRING,
    number_of_employees INT,
    of_opportunities INT,
    olsa STRING,
    opportunity_stage STRING,
    original_pilot STRING,
    original_pilot_yes_no STRING,
    other_partner STRING,
    owner_id STRING,
    ownership STRING,
    parent_id STRING,
    phone STRING,
    population_score DOUBLE,
    priority_score DOUBLE,
    project_defined STRING,
    pscrm_company_id STRING,
    record_type_id STRING,
    renewal_evaluation_required2 STRING,
    revenue_gen STRING,
    rrfs STRING,
    rrfs_complete STRING,
    shipping_city STRING,
    shipping_country STRING,
    shipping_postal_code STRING,
    shipping_state STRING,
    shipping_street STRING,
    sic STRING,
    skillport STRING,
    sla STRING,
    status STRING,
    system_modstamp TIMESTAMP,
    ticker_symbol STRING,
    tplms STRING,
    type STRING,
    website STRING,
    ss_nda STRING,
    ss_reference_account STRING,
    top_100 STRING,
    account_industry_account STRING,
    band_calculated STRING,
    geography STRING,
    premier_support_customer STRING,
    type_platform STRING,
    type_toolbook STRING,
    band STRING,
    is_customer_portal BOOLEAN,
    sumt_dba STRING,
    sum_total_account_id STRING,
    sum_total_client STRING,
    is_partner BOOLEAN,
    support_account STRING,
    nbr_of_account_team_members INT,
    ps_spif_ineligible STRING,
    customer_key STRING,
    industry STRING,
    won_opportunities INT,
    no_opportunities_closed INT,
    named_account STRING,
    market_segment_value STRING,
    market_segment STRING,
    discover_org_employees INT,
    db_state_province_code STRING,
    db_revenue_value DOUBLE,
    db_major_industry STRING,
    db_employees_here DOUBLE,
    db_duplicate STRING,
    db_duns STRING,
    db_country_code STRING,
    db_business_name STRING,
    count_of_opportunities_open INT,
    count_of_sum_total_csms INT,
    count_of_sum_total_csds INT,
    conga_output_format STRING,
    dozisf_zoom_info_id STRING,
    current_deployment STRING,
    g250 STRING,
    account_segment STRING,
    customer_persona STRING,
    is_deleted BOOLEAN,
    billing_address_complete STRING,
    billing_address_validation_status STRING,
    billing_address_validation_status_msg STRING,
    billing_country_2char_code STRING,
    billing_county STRING,
    billing_address STRING,
    billing_geocode_accuracy STRING,
    billing_geolocation STRING,
    billing_latitude DOUBLE,
    billing_longitude DOUBLE,
    fax STRING,
    shipping_address_complete STRING,
    gk_account STRING,
    gk_account_classification STRING,
    gk_salesforce_id STRING,
    gkna_created_by_user STRING,
    gkna_created_by_user_id STRING,
    shipping_address_validation_status_msg STRING,
    shipping_geolocation STRING,
    shipping_country_2char_code BOOLEAN,
    shipping_latitude DOUBLE,
    shipping_longitude DOUBLE,
    shipping_geocode_accuracy STRING,
    account_number STRING,
    shipping_county STRING,
    ingested_timestamp TIMESTAMP,
    hash_key STRING,
    record_expiry_period STRING,
    record_effective_date TIMESTAMP,
    record_expiry_date TIMESTAMP,
    record_current_flag INT,
    validated_billing_street_clean STRING,
    validated_billing_city_clean STRING,
    validated_billing_state_clean STRING,
    validated_billing_postal_code_clean STRING,
    validated_billing_country_clean STRING,
    validated_shipping_street_clean STRING,
    validated_shipping_city_clean STRING,
    validated_shipping_state_clean STRING,
    validated_shipping_postal_code_clean STRING,
    validated_shipping_country_clean STRING
)
USING delta         
"""
spark.sql(query)

# COMMAND ----------

# MAGIC %md 
# MAGIC ####inserting data into master tables(clean.can_states,clean.aus_states,clean.us_states,clean.valid_postal_code_formats)

# COMMAND ----------

truncate_query = f"TRUNCATE TABLE {catalog}.clean.can_states;"

insert_query = f"""
INSERT INTO {catalog}.clean.can_states (state, state_abbreviation)
VALUES  ('ALBERTA', 'AB'),
        ('BRITISH COLUMBIA', 'BC'),
        ('MANITOBA', 'MB'),
        ('NEW BRUNSWICK', 'NB'),
        ('NEWFOUNDLAND AND LABRADOR', 'NL'),
        ('NORTHWEST TERRITORIES', 'NT'),
        ('NOVA SCOTIA', 'NS'),
        ('NUNAVUT', 'NU'),
        ('ONTARIO', 'ON'),
        ('PRINCE EDWARD ISLAND', 'PE'),
        ('QUEBEC', 'QC'),
        ('SASKATCHEWAN', 'SK'),
        ('YUKON', 'YT');        
"""

spark.sql(truncate_query)
spark.sql(insert_query)

# COMMAND ----------

truncate_query = f"TRUNCATE TABLE {catalog}.clean.aus_states;"

insert_query = f"""
INSERT INTO {catalog}.clean.aus_states (state, state_abbreviation)
VALUES  ('VICTORIA', 'VI'),
        ('TASMANIA', 'TS'),
        ('QUEENSLAND', 'QL'),
        ('NEW SOUTH WALES', 'NS'),
        ('SOUTH AUSTRALIA', 'SA'),
        ('WESTERN AUSTRALIA', 'WA'),
        ('AUSTRALIAN CAPITAL TERRITORY', 'ACT'),
        ('NORTHERN TERRITORY', 'NT'),
        ('JERVIS BAY TERRITORY', 'JBT');        
"""

spark.sql(truncate_query)
spark.sql(insert_query)

# COMMAND ----------

truncate_query = f"TRUNCATE TABLE {catalog}.clean.us_states;"

insert_query = f"""
INSERT INTO {catalog}.clean.us_states (state, state_abbreviation)
VALUES  ('ALABAMA', 'AL'),
        ('ALASKA', 'AK'),
        ('ARIZONA', 'AZ'),
        ('ARKANSAS', 'AR'),
        ('CALIFORNIA', 'CA'),
        ('COLORADO', 'CO'),
        ('CONNECTICUT', 'CT'),
        ('DELAWARE', 'DE'),
        ('DISTRICT OF COLUMBIA', 'DC'),
        ('FLORIDA', 'FL'),
        ('GEORGIA', 'GA'),
        ('GUAM', 'GU'),
        ('HAWAII', 'HI'),
        ('IDAHO', 'ID'),
        ('ILLINOIS', 'IL'),
        ('INDIANA', 'IN'),
        ('IOWA', 'IA'),
        ('KANSAS', 'KS'),
        ('KENTUCKY', 'KY'),
        ('LOUISIANA', 'LA'),
        ('MAINE', 'ME'),
        ('MARYLAND', 'MD'),
        ('MASSACHUSETTS', 'MA'),
        ('MICHIGAN', 'MI'),
        ('MINNESOTA', 'MN'),
        ('MISSISSIPPI', 'MS'),
        ('MISSOURI', 'MO'),
        ('MONTANA', 'MT'),
        ('NEBRASKA', 'NE'),
        ('NEVADA', 'NV'),
        ('NEW HAMPSHIRE', 'NH'),
        ('NEW JERSEY', 'NJ'),
        ('NEW MEXICO', 'NM'),
        ('NEW YORK', 'NY'),
        ('NORTH CAROLINA', 'NC'),
        ('NORTH DAKOTA', 'ND'),
        ('NORTHERN MARIANA ISLANDS', 'MP'),
        ('OHIO', 'OH'),
        ('OKLAHOMA', 'OK'),
        ('OREGON', 'OR'),
        ('PENNSYLVANIA', 'PA'),
        ('RHODE ISLAND', 'RI'),
        ('SOUTH CAROLINA', 'SC'),
        ('SOUTH DAKOTA', 'SD'),
        ('TENNESSEE', 'TN'),
        ('TEXAS', 'TX'),
        ('TRUST TERRITORIES', 'TT'),
        ('UTAH', 'UT'),
        ('VERMONT', 'VT'),
        ('VIRGINIA', 'VA'),
        ('WASHINGTON', 'WA'),
        ('WEST VIRGINIA', 'WV'),
        ('WISCONSIN', 'WI'),
        ('WYOMING', 'WY'),
        ('U.S. VIRGIN ISLANDS', 'VI'),
        ('AMERICAN SAMOA', 'AS'),
        ('PUERTO RICO', 'PR');        
"""

spark.sql(truncate_query)
spark.sql(insert_query)

# COMMAND ----------

truncate_query = f"TRUNCATE TABLE {catalog}.clean.valid_postal_code_formats;"

insert_query = f"""
INSERT INTO {catalog}.clean.valid_postal_code_formats (country, valid_postal_code_format)
VALUES  ('US/USA/UNITED STATES/AMERICA', 'NNNNN/NNNNN-NNNN'),
        ('CA/CAN/CANADA', 'ANA NAN/ANA NA'),
        ('AU/AUS/AUSTRALIA', 'NNNN'),
        ('GB/UK/GBR/BRITAIN', 'AN/AAN/ANA/ANN/AANA/AANN/AN NAA/AAN NAA/ANA NAA/ANN NAA/AANA NAA/AANN NAA'),
        ('IN/IND/INDIA', 'NNNNNN/NNN NNN'),
        ('DE/DEU/GERMANY', 'NNNNN'),
        ('FR/FRA/FRANCE', 'NNNNN'),
        ('CH/CHE/SWITZERLAND', 'NNNNNN'),
        ('NL/NLD/NETHERLANDS', 'NNNN AA/NNNN'),
        ('JP/JPN/JAPAN', 'NNN-NNNN');        
"""

spark.sql(truncate_query)
spark.sql(insert_query)
