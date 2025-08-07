# Databricks notebook source
# MAGIC %md ##Creating widgets for input parameters

# COMMAND ----------

dbutils.widgets.text("catalog", "dev", "Catalog")
dbutils.widgets.text("storage_account", "sadwhdev02", "storage account")

# COMMAND ----------

# MAGIC %md ##Read the input parameters

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
storage_account = dbutils.widgets.get("storage_account")

# COMMAND ----------

# MAGIC %md ##Creating Salesforce Bronze Tables

# COMMAND ----------

# MAGIC %md ###cm_attribution

# COMMAND ----------

query = f"""
CREATE TABLE IF NOT EXISTS {catalog}.bronze.cm_attribution (
  model_name STRING NOT NULL,
  description STRING,
  lookback INT,
  primary_campaign_id STRING,
  opp_id STRING NOT NULL,
  parent_id STRING,
  company_id STRING,
  company_domain STRING,
  person_id STRING,
  person_domain STRING,
  member_id STRING,
  opp_name STRING,
  opp_create TIMESTAMP,
  opp_close DATE,
  customer_date DATE,
  is_closed BOOLEAN,
  opp_order INT,
  cm_is_won BOOLEAN,
  total_value DOUBLE,
  weighted_value INT,
  campaign_id STRING,
  campaign_name STRING,
  campaign_type STRING,
  event_detail STRING,
  order INT,
  touch_value DOUBLE,
  days_to_value INT,
  response_datetime TIMESTAMP,
  attrib_date DATE,
  influence INT,
  pre_opp BOOLEAN,
  event_system STRING,
  source STRING,
  model_type STRING,
  opp_type STRING,
  ad_source STRING,
  est_revenue DOUBLE,
  default_model BOOLEAN,
  attribution_eligible BOOLEAN,
  campaign_company_person STRING,
  opp_close_date_num INT,
  ingested_file_name STRING,
  ingested_period STRING,
  ingested_date DATE,
  ingested_timestamp TIMESTAMP,
  hash_key STRING)
USING delta
"""

spark.sql(query)

# COMMAND ----------

# MAGIC %md ###cm_person_company

# COMMAND ----------

query = f"""
CREATE TABLE IF NOT EXISTS {catalog}.bronze.cm_person_company (
  id STRING NOT NULL,
  company_id STRING,
  ingested_file_name STRING,
  ingested_period STRING,
  ingested_date DATE,
  ingested_timestamp TIMESTAMP,
  hash_key STRING)
USING delta
PARTITIONED BY (ingested_period)
"""

spark.sql(query)



# COMMAND ----------

# MAGIC %md ##Calibermind Tables

# COMMAND ----------

# MAGIC %md ###cm_attribution

# COMMAND ----------

query = f"""
CREATE TABLE IF NOT EXISTS {catalog}.silver.cm_attribution (
  model_name STRING NOT NULL,
  description STRING,
  lookback INT,
  primary_campaign_id STRING,
  opp_id STRING NOT NULL,
  parent_id STRING,
  company_id STRING,
  company_domain STRING,
  person_id STRING,
  person_domain STRING,
  member_id STRING,
  opp_name STRING,
  opp_create DATE,
  opp_close DATE,
  customer_date DATE,
  is_closed BOOLEAN,
  opp_order INT,
  cm_is_won BOOLEAN,
  total_value DOUBLE,
  weighted_value INT,
  campaign_id STRING,
  campaign_name STRING,
  campaign_type STRING,
  event_detail STRING,
  order INT,
  touch_value DOUBLE,
  days_to_value INT,
  response_datetime TIMESTAMP,
  attrib_date DATE,
  influence INT,
  pre_opp BOOLEAN,
  event_system STRING,
  source STRING,
  model_type STRING,
  opp_type STRING,
  ad_source STRING,
  est_revenue DOUBLE,
  default_model BOOLEAN,
  attribution_eligible BOOLEAN,
  campaign_company_person STRING,
  opp_close_date_num INT,
  ingested_file_name STRING,
  ingested_timestamp TIMESTAMP,
  hash_key STRING,
  record_expiry_period STRING,
  record_effective_date TIMESTAMP,
  record_expiry_date TIMESTAMP,
  record_current_flag BOOLEAN)
USING delta
PARTITIONED BY (record_current_flag, record_expiry_period)
LOCATION 'abfss://onelake@{storage_account}.dfs.core.windows.net/silver/calibermind/cm_attribution'             
"""

spark.sql(query)

# COMMAND ----------

# MAGIC %md ###cm_person_company

# COMMAND ----------

query = f"""
CREATE TABLE IF NOT EXISTS {catalog}.silver.cm_person_company (
  id STRING NOT NULL,
  company_id STRING,
  ingested_file_name STRING,
  ingested_timestamp TIMESTAMP,
  hash_key STRING,
  record_expiry_period STRING,
  record_effective_date TIMESTAMP,
  record_expiry_date TIMESTAMP,
  record_current_flag BOOLEAN)
USING delta
PARTITIONED BY (record_current_flag, record_expiry_period)
LOCATION 'abfss://onelake@{storage_account}.dfs.core.windows.net/silver/calibermind/cm_person_company'           
"""
spark.sql(query)



# COMMAND ----------

# MAGIC %md ##Peoplesoft Tables

# COMMAND ----------

# MAGIC %md ###ps_rt_rate_tbl

# COMMAND ----------

query = f"""
CREATE TABLE IF NOT EXISTS {catalog}.silver.ps_rt_rate_tbl (
  sid_rt_rate_index VARCHAR(10),
  sid_term INT,
  sid_from_cur VARCHAR(3),
  sid_to_cur VARCHAR(3),
  sid_rt_type VARCHAR(5),
  sid_effdt TIMESTAMP,
  rate_mult DECIMAL(15,8),
  rate_div DECIMAL(15,8),
  lastupddttm TIMESTAMP,
  syncid INT,
  ingested_file_name STRING,
  ingested_timestamp TIMESTAMP,
  hash_key STRING,
  record_expiry_period STRING,
  record_effective_date TIMESTAMP,
  record_expiry_date STRING,
  record_current_flag INT)
USING delta
PARTITIONED BY (record_current_flag, record_expiry_period)
LOCATION 'abfss://onelake@{storage_account}.dfs.core.windows.net/silver/peoplesoft/ps_rt_rate_tbl'         
"""
spark.sql(query)


# COMMAND ----------

# MAGIC %md ##Reference Tables

# COMMAND ----------

# MAGIC %md ###ref_account_industry_segment

# COMMAND ----------

query = f"""
CREATE TABLE IF NOT EXISTS {catalog}.silver.ref_account_industry_segment (
  account_industry_account STRING NOT NULL,
  account_industry_segment STRING NOT NULL,
  CONSTRAINT pk_ref_account_industry_segment PRIMARY KEY(account_industry_account)
)
USING delta
LOCATION 'abfss://onelake@{storage_account}.dfs.core.windows.net/silver/reference/ref_account_industry_segment/'          
"""

spark.sql(query)

# COMMAND ----------

truncate_query = f"TRUNCATE TABLE {catalog}.silver.ref_account_industry_segment;"

insert_query = f"""
INSERT INTO {catalog}.silver.ref_account_industry_segment (account_industry_account, account_industry_segment)
VALUES ('Automotive', 'Manufacturing'),
       ('Automotive Manufacturing', 'Manufacturing'),
       ('Food and Beverage', 'Manufacturing'),
       ('Food, Beverage, Tobacco Manufacturing', 'Manufacturing'),
       ('Manufacturing', 'Manufacturing'),
       ('Textiles, Leather, Apparel Manufacturing', 'Manufacturing'),
       ('Business Services', 'B2B Services'),
       ('Computers/IT/Technology Services', 'B2B Services'),
       ('Computer Services', 'B2B Services'),
       ('Consulting', 'B2B Services'),
       ('Marketing/Sales/Advertising/PR', 'B2B Services'),
       ('Energy and Utilities', 'Energy and Utilities'),
       ('Finance/Banking/Insurance', 'Financial Services'),
       ('Healthcare/Medical/Pharma/Bio-Tech', 'Healthcare'),
       ('Membership Organizations', 'Membership Organizations'),
       ('Retail/Wholesale', 'Retail'),
       ('Transportation/Shipping/Logistics', 'Transportation and Logistics'),
       ('Transportation/Shipping/Logistic', 'Transportation and Logistics'),
       ('', 'Z- No Industry');        
"""

spark.sql(truncate_query)
spark.sql(insert_query)

# COMMAND ----------

# MAGIC %md ###ref_gtm_account_country_code_geo

# COMMAND ----------

query = f"""
CREATE TABLE IF NOT EXISTS {catalog}.silver.ref_gtm_account_country_code_geo (
  account_country STRING NOT NULL,
  geo_old STRING NOT NULL,
  geo STRING NOT NULL,
  CONSTRAINT pk_ref_gtm_account_country_code_geo PRIMARY KEY(account_country)
)
USING delta
LOCATION 'abfss://onelake@{storage_account}.dfs.core.windows.net/silver/reference/ref_gtm_account_country_code_geo/'               
"""
spark.sql(query)

# COMMAND ----------

truncate_query = f"TRUNCATE TABLE {catalog}.silver.ref_gtm_account_country_code_geo;"

insert_query = f"""
INSERT INTO {catalog}.silver.ref_gtm_account_country_code_geo (account_country, geo_old, geo)
VALUES
    ('CN','APAC','APJ'),
    ('NP','APAC','APJ'),
    ('JP','APAC','APJ'),
    ('AU','APAC','APJ'),
    ('AUS','APAC','APJ'),
    ('SG','APAC','APJ'),
    ('PH','APAC','APJ'),
    ('FJ','APAC','APJ'),
    ('ID','APAC','APJ'),
    ('TH','APAC','APJ'),
    ('MY','APAC','APJ'),
    ('VN','APAC','APJ'),
    ('MU','APAC','APJ'),
    ('BGD','APAC','APJ'),
    ('Australia','APAC','APJ'),
    ('China','APAC','APJ'),
    ('TW','APAC','APJ'),
    ('KR','APAC','APJ'),
    ('PHL','APAC','APJ'),
    ('Vietnam','APAC','APJ'),
    ('HK','APAC','APJ'),
    ('SGP','APAC','APJ'),
    ('Thailand','APAC','APJ'),
    ('KOR','APAC','APJ'),
    ('MM','APAC','APJ'),
    ('HKG','APAC','APJ'),
    ('Brunei','APAC','APJ'),
    ('IDN','APAC','APJ'),
    ('CHN','APAC','APJ'),
    ('PG','APAC','APJ'),
    ('Taiwan','APAC','APJ'),
    ('JPN','APAC','APJ'),
    ('SINGAPORE','APAC','APJ'),
    ('NZ','APAC','APJ'),
    ('KOREA REP OF','APAC','APJ'),
    ('Maldives Islands','APAC','APJ'),
    ('MAURITIUS','APAC','APJ'),
    ('MALAYSIA','APAC','APJ'),
    ('HONG KONG','APAC','APJ'),
    ('THA','APAC','APJ'),
    ('PHILIPPINES','APAC','APJ'),
    ('INDONESIA','APAC','APJ'),
    ('VNM','APAC','APJ'),
    ('MYS','APAC','APJ'),
    ('Papua New Guinea','APAC','APJ'),
    ('Myanmar','APAC','APJ'),
    ('PR of China','APAC','APJ'),
    ('Repulic of Korea','APAC','APJ'),
    ('NPL','APAC','APJ'),
    ('TWN','APAC','APJ'),
    ('PNG','APAC','APJ'),
    ('TON','APAC','APJ'),
    ('Japan','APAC','APJ'),
    ('New Zealalnd','APAC','APJ'),
    ('Nepal','APAC','APJ'),
    ('SLB','APAC','APJ'),
    ('Laos','APAC','APJ'),
    ('SB','APAC','APJ'),
    ('TL','APAC','APJ'),
    ('TLS','APAC','APJ'),
    ('VUT','APAC','APJ'),
    ('Philippines -- PH','APAC','APJ'),
    ('KP','APAC','APJ'),
    ('KH','APAC','APJ'),
    ('MV','APAC','APJ'),
    ('FIJI','APAC','APJ'),
    ('JAP','APAC','APJ'),
    ('Chinese Taipei','APAC','APJ'),
    ('BD','APAC','APJ'),
    ('Korea','APAC','APJ'),
    ('FJI','APAC','APJ'),
    ('P.R.China','APAC','APJ'),
    ("People\'s Republic of China",'APAC','APJ'),
    ('PRK','APAC','APJ'),
    ('Germany','APAC','APJ'),
    ('SOLOMON ISLANDS','APAC','APJ'),
    ('Vietnam -- VN','APAC','APJ'),
    ('BRN','APAC','APJ'),
    ('MMR','APAC','APJ'),
    ('Taiwan ROC','APAC','APJ'),
    ('SOM','APAC','APJ'),
    ('MO','APAC','APJ'),
    ('BN','APAC','APJ'),
    ('Fiji Islands','APAC','APJ'),
    ('NC','APAC','APJ'),
    ('PW','APAC','APJ'),
    ('TJK','APAC','APJ'),
    ('CXR','APAC','APJ'),
    ('VU','APAC','APJ'),
    ('BANGLADESH','APAC','APJ'),
    ('ANGUILLA','APAC','APJ'),
    ('BT','APAC','APJ'),
    ('BTN','APAC','APJ'),
    ('KI','APAC','APJ'),
    ('NCL','APAC','APJ'),
    ('Peoples Republic of China','APAC','APJ'),
    ('Republic of Korea','APAC','APJ'),
    ('NZL','APAC','APJ'),
    ('New Zealand','APAC','APJ'),
    ('New Zeland','APAC','APJ'),
    ('Micronesia','APAC','APJ'),
    ('South Korea','APAC','APJ'),
    ('MAC','APAC','APJ'),
    ('MUS','APAC','APJ'),
    ('DEU','EMEA','EMEA'),
    ('GB','EMEA','EMEA'),
    ('ENGLAND','EMEA','EMEA'),
    ('GBR','EMEA','EMEA'),
    ('WALES','EMEA','EMEA'),
    ('PL','EMEA','EMEA'),
    ('IE','EMEA','EMEA'),
    ('SCOTLAND','EMEA','EMEA'),
    ('NL','EMEA','EMEA'),
    ('BE','EMEA','EMEA'),
    ('NO','EMEA','EMEA'),
    ('BEL','EMEA','EMEA'),
    ('DK','EMEA','EMEA'),
    ('LU','EMEA','EMEA'),
    ('DNK','EMEA','EMEA'),
    ('SE','EMEA','EMEA'),
    ('LUX','EMEA','EMEA'),
    ('FI','EMEA','EMEA'),
    ('NLD','EMEA','EMEA'),
    ('IS','EMEA','EMEA'),
    ('LT','EMEA','EMEA'),
    ('Belgique','EMEA','EMEA'),
    ('Luxembourg','EMEA','EMEA'),
    ('NORTHERN IRELAND','EMEA','EMEA'),
    ('DENMARK','EMEA','EMEA'),
    ('PK','EMEA','EMEA'),
    ('Pakistan','EMEA','EMEA'),
    ('FO','EMEA','EMEA'),
    ('Belgium','EMEA','EMEA'),
    ('PAK','EMEA','EMEA'),
    ('TR','EMEA','EMEA'),
    ('FIN','EMEA','EMEA'),
    ('JE','EMEA','EMEA'),
    ('NOR','EMEA','EMEA'),
    ('LV','EMEA','EMEA'),
    ('LTU','EMEA','EMEA'),
    ('SWE','EMEA','EMEA'),
    ('LK','EMEA','EMEA'),
    ('Grand Duchy of Luxembourg','EMEA','EMEA'),
    ('Netherlands','EMEA','EMEA'),
    ('The Netherlands','EMEA','EMEA'),
    ('Sweden','EMEA','EMEA'),
    ('NORWAY','EMEA','EMEA'),
    ('ICELAND','EMEA','EMEA'),
    ('LITHUANIA','EMEA','EMEA'),
    ('FINLAND','EMEA','EMEA'),
    ('IRELAND','EMEA','EMEA'),
    ('GG','EMEA','EMEA'),
    ('CHE','EMEA','EMEA'),
    ('United Kingdom','EMEA','EMEA'),
    ('AF','EMEA','EMEA'),
    ('LATVIA','EMEA','EMEA'),
    ('DZ','EMEA','EMEA'),
    ('UK','EMEA','EMEA'),
    ('United Kingdom -- GB','EMEA','EMEA'),
    ('ZA','EMEA','EMEA'),
    ('Sweden -- SE','EMEA','EMEA'),
    ('Belgium -- BE','EMEA','EMEA'),
    ('Netherlands -- NL','EMEA','EMEA'),
    ('IRL','EMEA','EMEA'),
    ('ZAF','EMEA','EMEA'),
    ('FRANCE','EMEA','EMEA'),
    ('ES','EMEA','EMEA'),
    ('MT','EMEA','EMEA'),
    ('Cameroon','EMEA','EMEA'),
    ('PE','EMEA','EMEA'),
    ('British Virgin Islands','EMEA','EMEA'),
    ('PT','EMEA','EMEA'),
    ('SWITZERLAND','EMEA','EMEA'),
    ('IRE','EMEA','EMEA'),
    ('Netherland','EMEA','EMEA'),
    ('UA','EMEA','EMEA'),
    ('GH','EMEA','EMEA'),
    ('SWD','EMEA','EMEA'),
    ('AT','EMEA','EMEA'),
    ('HU','EMEA','EMEA'),
    ('Ireland -- IE','EMEA','EMEA'),
    ('FRA','EMEA','EMEA'),
    ('RU','EMEA','EMEA'),
    ('AUSTRIA','EMEA','EMEA'),
    ('AUT','EMEA','EMEA'),
    ('Gemany','EMEA','EMEA'),
    ('IL','EMEA','EMEA'),
    ('SA','EMEA','EMEA'),
    ('Saudi Arabia','EMEA','EMEA'),
    ('KWT','EMEA','EMEA'),
    ('QA','EMEA','EMEA'),
    ('KW','EMEA','EMEA'),
    ('AE','EMEA','EMEA'),
    ('KSA','EMEA','EMEA'),
    ('EG','EMEA','EMEA'),
    ('LB','EMEA','EMEA'),
    ('OM','EMEA','EMEA'),
    ('QAT','EMEA','EMEA'),
    ('AM','EMEA','EMEA'),
    ('RS','EMEA','EMEA'),
    ('BG','EMEA','EMEA'),
    ('United Kingdon -- GB','EMEA','EMEA'),
    ('Switzerland -- CH','EMEA','EMEA'),
    ('CY','EMEA','EMEA'),
    ('HR','EMEA','EMEA'),
    ('GR','EMEA','EMEA'),
    ('ROU','EMEA','EMEA'),
    ('Angola','EMEA','EMEA'),
    ('TN','EMEA','EMEA'),
    ('KE','EMEA','EMEA'),
    ('MA','EMEA','EMEA'),
    ('SN','EMEA','EMEA'),
    ('LY','EMEA','EMEA'),
    ('Belguim','EMEA','EMEA'),
    ('LI','EMEA','EMEA'),
    ('ENG','EMEA','EMEA'),
    ('Suisse','EMEA','EMEA'),
    ('PF','EMEA','EMEA'),
    ('SUI','EMEA','EMEA'),
    ('Schweiz','EMEA','EMEA'),
    ('Deutschland','EMEA','EMEA'),
    ('Spain','EMEA','EMEA'),
    ('D','EMEA','EMEA'),
    ('Portugal','EMEA','EMEA'),
    ('IM','EMEA','EMEA'),
    ('Italy','EMEA','EMEA'),
    ('LIE','EMEA','EMEA'),
    ('ESP','EMEA','EMEA'),
    ('G','EMEA','EMEA'),
    ('Boulogne','EMEA','EMEA'),
    ('South Yorkshire','EMEA','EMEA'),
    ('MONACO','EMEA','EMEA'),
    ('GIBRALTAR','EMEA','EMEA'),
    ('MC','EMEA','EMEA'),
    ('France -- FR','EMEA','EMEA'),
    ('Spain -- ES','EMEA','EMEA'),
    ('LIECHTENSTEIN','EMEA','EMEA'),
    ('FORT DE FRANCE','EMEA','EMEA'),
    ('PRT','EMEA','EMEA'),
    ('GI','EMEA','EMEA'),
    ('ARE','EMEA','EMEA'),
    ('BLZ','EMEA','EMEA'),
    ('ALB','EMEA','EMEA'),
    ('MDA','EMEA','EMEA'),
    ('UY','EMEA','EMEA'),
    ('ITL','EMEA','EMEA'),
    ('MCO','EMEA','EMEA'),
    ('G BR','EMEA','EMEA'),
    ('Nantes Cedex','EMEA','EMEA'),
    ('GBP','EMEA','EMEA'),
    ('REUNION','EMEA','EMEA'),
    ('Aland Islands','EMEA','EMEA'),
    ('SRI LANKA','EMEA','EMEA'),
    ('PARIS','EMEA','EMEA'),
    ('SL','EMEA','EMEA'),
    ('NE','EMEA','EMEA'),
    ('LR','EMEA','EMEA'),
    ('SYR','EMEA','EMEA'),
    ('MN','EMEA','EMEA'),
    ('LKA','EMEA','EMEA'),
    ('KHM','EMEA','EMEA'),
    ('BIH','EMEA','EMEA'),
    ('SHN','EMEA','EMEA'),
    ('AGO','EMEA','EMEA'),
    ('AFG','EMEA','EMEA'),
    ('BI','EMEA','EMEA'),
    ('MQ','EMEA','EMEA'),
    ('ISR','EMEA','EMEA'),
    ('SYC','EMEA','EMEA'),
    ('BJ','EMEA','EMEA'),
    ('Republic of Ireland','EMEA','EMEA'),
    ('REU','EMEA','EMEA'),
    ('MDG','EMEA','EMEA'),
    ('Allemagne','EMEA','EMEA'),
    ('POL','EMEA','EMEA'),
    ('CZ','EMEA','EMEA'),
    ('RO','EMEA','EMEA'),
    ('NG','EMEA','EMEA'),
    ('Slovakia','EMEA','EMEA'),
    ('Senegal','EMEA','EMEA'),
    ('EGY','EMEA','EMEA'),
    ('ET','EMEA','EMEA'),
    ('YE','EMEA','EMEA'),
    ('Serbia','EMEA','EMEA'),
    ('UG','EMEA','EMEA'),
    ('EE','EMEA','EMEA'),
    ('GE','EMEA','EMEA'),
    ('South Africa','EMEA','EMEA'),
    ('ZM','EMEA','EMEA'),
    ('TZ','EMEA','EMEA'),
    ('MK','EMEA','EMEA'),
    ('JO','EMEA','EMEA'),
    ('IQ','EMEA','EMEA'),
    ('Egypt','EMEA','EMEA'),
    ('CZE','EMEA','EMEA'),
    ('BH','EMEA','EMEA'),
    ('Benin','EMEA','EMEA'),
    ('NGI','EMEA','EMEA'),
    ('Mozambique','EMEA','EMEA'),
    ('SK','EMEA','EMEA'),
    ('SI','EMEA','EMEA'),
    ('United Arab Emirates','EMEA','EMEA'),
    ('MZ','EMEA','EMEA'),
    ('MLT','EMEA','EMEA'),
    ('AZ','EMEA','EMEA'),
    ('LBN','EMEA','EMEA'),
    ('NA','EMEA','EMEA'),
    ('NER','EMEA','EMEA'),
    ('ROM','EMEA','EMEA'),
    ('SAU','EMEA','EMEA'),
    ('CYP','EMEA','EMEA'),
    ('TUR','EMEA','EMEA'),
    ('EST','EMEA','EMEA'),
    ('Nigeria','EMEA','EMEA'),
    ('AZE','EMEA','EMEA'),
    ('TZA','EMEA','EMEA'),
    ('ZWE','EMEA','EMEA'),
    ('BGR','EMEA','EMEA'),
    ('ZMB','EMEA','EMEA'),
    ('KAZ','EMEA','EMEA'),
    ('MD','EMEA','EMEA'),
    ('CAF','EMEA','EMEA'),
    ('NGA','EMEA','EMEA'),
    ('Kingdom of Saudi Arabia','EMEA','EMEA'),
    ('Libyen','EMEA','EMEA'),
    ('Africa','EMEA','EMEA'),
    ('SVK','EMEA','EMEA'),
    ('UZ','EMEA','EMEA'),
    ('Russland','EMEA','EMEA'),
    ('Russia','EMEA','EMEA'),
    ('Bosnia and Herzegovina','EMEA','EMEA'),
    ('Jordan','EMEA','EMEA'),
    ('Kazakhstan','EMEA','EMEA'),
    ('Morocco','EMEA','EMEA'),
    ('ZW','EMEA','EMEA'),
    ('POLAND','EMEA','EMEA'),
    ('RUSSIAN FEDERATION','EMEA','EMEA'),
    ('CYPRUS','EMEA','EMEA'),
    ('ROMANIA','EMEA','EMEA'),
    ('ESTONIA','EMEA','EMEA'),
    ('Turkey','EMEA','EMEA'),
    ('Ivory Coast','EMEA','EMEA'),
    ('HUNGARY','EMEA','EMEA'),
    ('CZECH REPUBLIC','EMEA','EMEA'),
    ('BAHRAIN','EMEA','EMEA'),
    ('CROATIA','EMEA','EMEA'),
    ('MALTA','EMEA','EMEA'),
    ('KUWAIT','EMEA','EMEA'),
    ('OMAN','EMEA','EMEA'),
    ('QATAR','EMEA','EMEA'),
    ('BELARUS','EMEA','EMEA'),
    ('BULGARIA','EMEA','EMEA'),
    ('ETHIOPIA','EMEA','EMEA'),
    ('UKRAINE','EMEA','EMEA'),
    ('LEBANON','EMEA','EMEA'),
    ('ARMENIA','EMEA','EMEA'),
    ('GREECE','EMEA','EMEA'),
    ('UAE','EMEA','EMEA'),
    ('ML','EMEA','EMEA'),
    ("Cote d\'Ivoire",'EMEA','EMEA'),
    ('Ghana','EMEA','EMEA'),
    ('Dubai','EMEA','EMEA'),
    ('Iraq','EMEA','EMEA'),
    ('BY','EMEA','EMEA'),
    ('SS','EMEA','EMEA'),
    ('RW','EMEA','EMEA'),
    ('GM','EMEA','EMEA'),
    ('IR','EMEA','EMEA'),
    ('South Sudan','EMEA','EMEA'),
    ('Nigeria -- NG','EMEA','EMEA'),
    ('MW','EMEA','EMEA'),
    ('BW','EMEA','EMEA'),
    ('Macedonia','EMEA','EMEA'),
    ('CM','EMEA','EMEA'),
    ('South Africa -- ZA','EMEA','EMEA'),
    ('GRC','EMEA','EMEA'),
    ('IRN','EMEA','EMEA'),
    ('SSD','EMEA','EMEA'),
    ('RSA','EMEA','EMEA'),
    ('LBY','EMEA','EMEA'),
    ('SDN','EMEA','EMEA'),
    ('CMR','EMEA','EMEA'),
    ('KEN','EMEA','EMEA'),
    ('BHR','EMEA','EMEA'),
    ('Czech Repulic','EMEA','EMEA'),
    ('HRV','EMEA','EMEA'),
    ('Kosove','EMEA','EMEA'),
    ('ETH','EMEA','EMEA'),
    ('OMN','EMEA','EMEA'),
    ('IRAN','EMEA','EMEA'),
    ('MWI','EMEA','EMEA'),
    ('Slovak Republic','EMEA','EMEA'),
    ('HUN','EMEA','EMEA'),
    ('Moldova','EMEA','EMEA'),
    ('GHA','EMEA','EMEA'),
    ('UKR','EMEA','EMEA'),
    ('DZA','EMEA','EMEA'),
    ('SRB','EMEA','EMEA'),
    ('ISL','EMEA','EMEA'),
    ('MKD','EMEA','EMEA'),
    ('UGA','EMEA','EMEA'),
    ('SD','EMEA','EMEA'),
    ('Maroc','EMEA','EMEA'),
    ('Tunisie','EMEA','EMEA'),
    ('Cameroun','EMEA','EMEA'),
    ('Algeria','EMEA','EMEA'),
    ('Guinea','EMEA','EMEA'),
    ('AO','EMEA','EMEA'),
    ('GMB','EMEA','EMEA'),
    ('Kosovo','EMEA','EMEA'),
    ('Burkina Faso','EMEA','EMEA'),
    ('VG','EMEA','EMEA'),
    ('SWI','EMEA','EMEA'),
    ('ITA','EMEA','EMEA'),
    ('VIRGIN ISLANDS UK','EMEA','EMEA'),
    ('FR','EMEA','EMEA'),
    ('CH','EMEA','EMEA'),
    ('DE','EMEA','EMEA'),
    ('RUS','EMEA','EMEA'),
    ('KZ','EMEA','EMEA'),
    ('IT','EMEA','EMEA'),
    ('NI','EMEA','EMEA'),
    ('Kenya','EMEA','EMEA'),
    ('Israel','EMEA','EMEA'),
    ('IND','India','APJ'),
    ('IN','India','APJ'),
    ('Indian','India','APJ'),
    ('MDV','India','APJ'),
    ('India','India','APJ'),
    ('USA','North America','North America'),
    ('US','North America','North America'),
    ('BM','North America','North America'),
    ('CAN','North America','North America'),
    ('United States','North America','North America'),
    ('United States --US','North America','North America'),
    ('CHL','North America','North America'),
    ('GT','North America','North America'),
    ('MX','North America','North America'),
    ('CO','North America','North America'),
    ('BRA','North America','North America'),
    ('Mexico','North America','North America'),
    ('Canada','North America','North America'),
    ('BR','North America','North America'),
    ('DO','North America','North America'),
    ('CA','North America','North America'),
    ('BRZ','North America','North America'),
    ('VGB','North America','North America'),
    ('PRI','North America','North America'),
    ('AR','North America','North America'),
    ('GU','North America','North America'),
    ('MEX','North America','North America'),
    ('TT','North America','North America'),
    ('BMU','North America','North America'),
    ('Mexico -- MX','North America','North America'),
    ('SC','North America','North America'),
    ('Bermuda','North America','North America'),
    ('Untied States','North America','North America'),
    ('U.S. Virgin Islands','North America','North America'),
    ('CRI','North America','North America'),
    ('BB','North America','North America'),
    ('BZ','North America','North America'),
    ('ASM','North America','North America'),
    ('BRB','North America','North America'),
    ('HND','North America','North America'),
    ('ANT','North America','North America'),
    ('Bolivia','North America','North America'),
    ('BHS','North America','North America'),
    ('U','North America','North America'),
    ('BO','North America','North America'),
    ('TTO','North America','North America'),
    ('JM','North America','North America'),
    ('SV','North America','North America'),
    ('SR','North America','North America'),
    ('GTM','North America','North America'),
    ('CR','North America','North America'),
    ('UMI','North America','North America'),
    ('ABW','North America','North America'),
    ('MP','North America','North America'),
    ('PY','North America','North America'),
    ('AG','North America','North America'),
    ('BS','North America','North America'),
    ('HT','North America','North America'),
    ('ST LUCIA','North America','North America'),
    ('URUGUAY','North America','North America'),
    ('GY','North America','North America'),
    ('PR','North America','North America'),
    ('GD','North America','North America'),
    ('TG','North America','North America'),
    ('PA','North America','North America'),
    ('LC','North America','North America'),
    ('UM','North America','North America'),
    ('HN','North America','North America'),
    ('GP','North America','North America'),
    ('COL','North America','North America'),
    ('GUM','North America','North America'),
    ('EC','North America','North America'),
    ('Canadad','North America','North America'),
    ('VE','North America','North America'),
    ('BRAZIL','North America','North America'),
    ('CYM','North America','North America'),
    ('LCA','North America','North America'),
    ('JAM','North America','North America'),
    ('ARG','North America','North America'),
    ('CAYMAN ISLANDS','North America','North America'),
    ('PANAMA','North America','North America'),
    ('CURACAO','North America','North America'),
    ('Colombia','North America','North America'),
    ('JAMAICA','North America','North America'),
    ('CHILE','North America','North America'),
    ('Argentina','North America','North America'),
    ('EL SALVADOR','North America','North America'),
    ('COSTA RICA','North America','North America'),
    ('TRINIDAD & TOBAGO','North America','North America'),
    ('BARBADOS','North America','North America'),
    ('BAHAMAS','North America','North America'),
    ('VENEZUELA','North America','North America'),
    ('PERU','North America','North America'),
    ('ECU','North America','North America'),
    ('Ecuador','North America','North America'),
    ('SLV','North America','North America'),
    ('PER','North America','North America'),
    ('MNP','North America','North America'),
    ('PAN','North America','North America'),
    ('DOM','North America','North America'),
    ('Mexico D.F.','North America','North America'),
    ('VEN','North America','North America'),
    ('Brasil','North America','North America'),
    ('Jamaica W.','North America','North America'),
    ('United States - US','North America','North America'),
    ('Dominican Republic','North America','North America'),
    ('AL','EMEA','EMEA'),
    ('AS','APAC','APJ'),
    ('AD','EMEA','EMEA'),
    ('AI','North America','North America'),
    ('AQ','APAC','APJ'),
    ('BA','EMEA','EMEA'),
    ('BV','North America','North America'),
    ('IO','APAC','APJ'),
    ('BF','EMEA','EMEA'),
    ('CV','EMEA','EMEA'),
    ('CF','EMEA','EMEA'),
    ('TD','EMEA','EMEA'),
    ('CX','APAC','APJ'),
    ('CC','APAC','APJ'),
    ('KM','EMEA','EMEA'),
    ('CG','EMEA','EMEA'),
    ('CD','EMEA','EMEA'),
    ('CK','APAC','APJ'),
    ('CI','EMEA','EMEA'),
    ('DJ','EMEA','EMEA'),
    ('TP','EMEA','EMEA'),
    ('GQ','EMEA','EMEA'),
    ('ER','EMEA','EMEA'),
    ('FK','North America','North America'),
    ('TF','APAC','APJ'),
    ('GA','EMEA','EMEA'),
    ('GL','EMEA','EMEA'),
    ('GN','EMEA','EMEA'),
    ('GW','EMEA','EMEA'),
    ('HM','APAC','APJ'),
    ('VA','EMEA','EMEA'),
    ('KG','APAC','APJ'),
    ('LA','APAC','APJ'),
    ('LS','EMEA','EMEA'),
    ('MG','EMEA','EMEA'),
    ('MH','APAC','APJ'),
    ('MR','EMEA','EMEA'),
    ('YT','EMEA','EMEA'),
    ('FM','APAC','APJ'),
    ('ME','EMEA','EMEA'),
    ('MS','North America','North America'),
    ('NR','APAC','APJ'),
    ('AN','North America','North America'),
    ('NU','APAC','APJ'),
    ('NF','APAC','APJ'),
    ('PN','APAC','APJ'),
    ('RE','APAC','APJ'),
    ('WS','APAC','APJ'),
    ('SM','EMEA','EMEA'),
    ('ST','EMEA','EMEA'),
    ('SO','EMEA','EMEA'),
    ('GS','EMEA','EMEA'),
    ('SH','EMEA','EMEA'),
    ('PM','EMEA','EMEA'),
    ('SJ','EMEA','EMEA'),
    ('SZ','EMEA','EMEA'),
    ('SY','APAC','APJ'),
    ('TJ','APAC','APJ'),
    ('TK','APAC','APJ'),
    ('TO','APAC','APJ'),
    ('TM','APAC','APJ'),
    ('TV','APAC','APJ'),
    ('WF','APAC','APJ'),
    ('EH','EMEA','EMEA'),
    ('AND','EMEA','EMEA'),
    ('AIA','North America','North America'),
    ('ATA','APAC','APJ'),
    ('ARM','EMEA','EMEA'),
    ('BLR','EMEA','EMEA'),
    ('BEN','EMEA','EMEA'),
    ('BWA','EMEA','EMEA'),
    ('BVT','North America','North America'),
    ('IOT','APAC','APJ'),
    ('BFA','EMEA','EMEA'),
    ('BDI','EMEA','EMEA'),
    ('CPV','EMEA','EMEA'),
    ('TCD','EMEA','EMEA'),
    ('CCK','APAC','APJ'),
    ('COM','EMEA','EMEA'),
    ('COG','EMEA','EMEA'),
    ('COD','EMEA','EMEA'),
    ('COK','APAC','APJ'),
    ('CIV','EMEA','EMEA'),
    ('DJI','EMEA','EMEA'),
    ('TMP','EMEA','EMEA'),
    ('GNQ','EMEA','EMEA'),
    ('ERI','EMEA','EMEA'),
    ('FLK','North America','North America'),
    ('FRO','EMEA','EMEA'),
    ('PYF','APAC','APJ'),
    ('ATF','APAC','APJ'),
    ('GAB','EMEA','EMEA'),
    ('GEO','EMEA','EMEA'),
    ('GIB','EMEA','EMEA'),
    ('GRL','EMEA','EMEA'),
    ('GIN','EMEA','EMEA'),
    ('GNB','EMEA','EMEA'),
    ('HMD','APAC','APJ'),
    ('VAT','EMEA','EMEA'),
    ('IRQ','EMEA','EMEA'),
    ('JOR','EMEA','EMEA'),
    ('KIR','APAC','APJ'),
    ('KGZ','APAC','APJ'),
    ('LAO','APAC','APJ'),
    ('LVA','EMEA','EMEA'),
    ('LSO','EMEA','EMEA'),
    ('LBR','EMEA','EMEA'),
    ('MLI','EMEA','EMEA'),
    ('MHL','APAC','APJ'),
    ('MRT','EMEA','EMEA'),
    ('MYT','EMEA','EMEA'),
    ('FSM','APAC','APJ'),
    ('MNG','APAC','APJ'),
    ('MNE','EMEA','EMEA'),
    ('MSR','North America','North America'),
    ('MAR','EMEA','EMEA'),
    ('MOZ','EMEA','EMEA'),
    ('NAM','EMEA','EMEA'),
    ('NRU','APAC','APJ'),
    ('NIU','APAC','APJ'),
    ('NFK','APAC','APJ'),
    ('PLW','APAC','APJ'),
    ('PCN','APAC','APJ'),
    ('RWA','EMEA','EMEA'),
    ('WSM','APAC','APJ'),
    ('SMR','EMEA','EMEA'),
    ('STP','EMEA','EMEA'),
    ('SEN','EMEA','EMEA'),
    ('SLE','EMEA','EMEA'),
    ('SVN','EMEA','EMEA'),
    ('SGS','EMEA','EMEA'),
    ('SPM','EMEA','EMEA'),
    ('SJM','EMEA','EMEA'),
    ('SWZ','EMEA','EMEA'),
    ('TGO','EMEA','EMEA'),
    ('TKL','APAC','APJ'),
    ('TUN','EMEA','EMEA'),
    ('TKM','APAC','APJ'),
    ('TUV','APAC','APJ'),
    ('UZB','APAC','APJ'),
    ('WLF','APAC','APJ'),
    ('ESH','EMEA','EMEA'),
    ('YEM','EMEA','EMEA');         
"""

spark.sql(truncate_query)
spark.sql(insert_query)

# COMMAND ----------

# MAGIC %md ###ref_region_from_name

# COMMAND ----------

query = f"""
CREATE TABLE IF NOT EXISTS {catalog}.silver.ref_region_from_name (
  name STRING NOT NULL,
  level STRING NOT NULL,
  region STRING NOT NULL,
  CONSTRAINT pk_ref_region_from_name PRIMARY KEY(name)
)
USING delta
LOCATION 'abfss://onelake@{storage_account}.dfs.core.windows.net/silver/reference/ref_region_from_name/'               
"""

spark.sql(query)

# COMMAND ----------

truncate_query = f"TRUNCATE TABLE {catalog}.silver.ref_region_from_name;"

insert_query = f"""
INSERT INTO {catalog}.silver.ref_region_from_name (name, level, region)
VALUES ('Rajiv Kumar', 'RVP', 'India'),
       ('Divyesh Sindhwaad', 'AVP', 'India'),
       ('Kamal Dutta', 'AVP', 'India'),
       ('K Dutta', 'AVP', 'India'),
       ('Agata Nowakowska', 'AVP', 'EMEA'),
       ('Other', 'Other', 'North America'),
       ('Kath Greenhough', 'AVP', 'APAC'),
       ('Gerard Marlow', 'AVP', 'EMEA'),
       ('Rosie Cairnes', 'AVP', 'APAC'),
       ('Ian Rawlings', 'RVP', 'EMEA'),
       ('Glyn Roberts', 'AVP', 'EMEA'),
       ('Eddie Brock', 'AVP', 'EMEA'),
       ('Rhys Hughes', 'RVP', 'APAC'),
       ('R Cairnes', 'AVP', 'APAC');         
"""

spark.sql(truncate_query)
spark.sql(insert_query)

# COMMAND ----------

# MAGIC %md ###ref_forecast_product_family_from_sbu

# COMMAND ----------

query = f"""
CREATE TABLE IF NOT EXISTS {catalog}.silver.ref_forecast_product_family_from_sbu (
  sbu STRING NOT NULL,
  product_family STRING NOT NULL,
  CONSTRAINT pk_ref_forecast_product_family_from_sbu PRIMARY KEY(sbu)
)
USING delta
LOCATION 'abfss://onelake@{storage_account}.dfs.core.windows.net/silver/reference/ref_forecast_product_family_from_sbu/'               
"""

spark.sql(query)

# COMMAND ----------

truncate_query = f"TRUNCATE TABLE {catalog}.silver.ref_forecast_product_family_from_sbu;"

insert_query = f"""
INSERT INTO {catalog}.silver.ref_forecast_product_family_from_sbu (sbu, product_family)
VALUES ('Compliance', 'Skillsoft'),
       ('Skillsoft', 'Skillsoft'),
       ('Vodeclic', 'Skillsoft'),
       ('SumTotal', 'SumTotal'),
       ('Others', 'Others');         
"""

spark.sql(truncate_query)
spark.sql(insert_query)

# COMMAND ----------

# MAGIC %md ###ref_svp_product_family_from_name

# COMMAND ----------

query = f"""
CREATE TABLE IF NOT EXISTS {catalog}.silver.ref_svp_product_family_from_name (
  evp_name STRING NOT NULL,
  svp_based_product STRING NOT NULL,
  CONSTRAINT pk_ref_svp_product_family_from_name PRIMARY KEY(evp_name)
)
USING delta
LOCATION 'abfss://onelake@{storage_account}.dfs.core.windows.net/silver/reference/ref_svp_product_family_from_name/'               
"""
spark.sql(query)

# COMMAND ----------

truncate_query = f"TRUNCATE TABLE {catalog}.silver.ref_svp_product_family_from_name;"

insert_query = f"""
INSERT INTO {catalog}.silver.ref_svp_product_family_from_name (evp_name, svp_based_product)
VALUES ('Michelle Boockoff-Bajdek', 'Skillsoft'),
       ('Robert Hartsough', 'Skillsoft'),
       ('Michelle Bajdek', 'Skillsoft'),
       ('Matthew Glitzer', 'Skillsoft'),
       ('Patrick Manzo', 'Skillsoft'),
       ('Tony Barbone', 'SumTotal'),
       ('Ted Winslow', 'SumTotal'),
       ('Rich Walker', 'SumTotal'),
       ('Eric Stine', 'Skillsoft'),
       ('Liam Butler', 'SumTotal'),
       ('Jeff Tarr', 'Skillsoft'),
       ('Other', 'Others/PS');   
"""

spark.sql(truncate_query)
spark.sql(insert_query)


# COMMAND ----------

# MAGIC %md ##Creating Salesforce Bronze Tables

# COMMAND ----------

# MAGIC %md ###sf_account

# COMMAND ----------

query = f"""
CREATE TABLE IF NOT EXISTS {catalog}.bronze.sf_account (
  sid_id STRING NOT NULL,
  account_id STRING,
  account_plans DOUBLE,
  account_type STRING,
  active_prospecting_by STRING,
  alignment_score DOUBLE,
  annual_revenue BIGINT,
  billing_city STRING,
  billing_country STRING,
  billing_postal_code STRING,
  billing_state STRING,
  billing_street STRING,
  bo_id STRING,
  company_size STRING,
  contact_and_referral_score DOUBLE,
  contract_end_date STRING,
  contract_start_date STRING,
  created_by_id STRING,
  created_date TIMESTAMP,
  currency_iso_code STRING,
  duns_number INT,
  expires DOUBLE,
  fax STRING,
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
  of_opportunities DOUBLE,
  olsa STRING,
  opportunity_stage STRING,
  original_pilot BOOLEAN,
  original_pilot_yes_no STRING,
  other_partner STRING,
  owner_id STRING,
  ownership STRING,
  parent_id STRING,
  phone STRING,
  population_score DOUBLE,
  priority_score DOUBLE,
  project_defined BOOLEAN,
  pscrm_company_id STRING,
  record_type_id STRING,
  renewal_evaluation_required2 STRING,
  revenue_gen STRING,
  rrfs DOUBLE,
  rrfs_complete DOUBLE,
  shipping_city STRING,
  shipping_country STRING,
  shipping_postal_code STRING,
  shipping_state STRING,
  shipping_street STRING,
  sic INT,
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
  premier_support_customer BOOLEAN,
  type_platform STRING,
  type_toolbook STRING,
  band STRING,
  is_customer_portal BOOLEAN,
  sumt_dba STRING,
  sum_total_account_id STRING,
  sum_total_client STRING,
  is_partner BOOLEAN,
  support_account BOOLEAN,
  nbr_of_account_team_members DOUBLE,
  ps_spif_ineligible BOOLEAN,
  customer_key INT,
  industry STRING,
  won_opportunities DOUBLE,
  no_opportunities_closed DOUBLE,
  named_account STRING,
  market_segment_value DOUBLE,
  market_segment STRING,
  discover_org_employees STRING,
  db_state_province_code STRING,
  db_revenue_value STRING,
  db_major_industry STRING,
  db_employees_here STRING,
  db_duplicate STRING,
  db_duns STRING,
  db_country_code STRING,
  db_business_name STRING,
  count_of_opportunities_open DOUBLE,
  count_of_sum_total_csms STRING,
  count_of_sum_total_csds STRING,
  conga_output_format INT,
  dozisf__zoom_info_id BIGINT,
  current_deployment STRING,
  g250 BOOLEAN,
  account_segment STRING,
  customer_persona STRING,
  business_division STRING,
  is_deleted BOOLEAN,
  days_since_last_activity DOUBLE,
  ingested_file_name STRING,
  ingested_period STRING,
  ingested_date DATE,
  ingested_timestamp TIMESTAMP,
  hash_key STRING)
USING delta
PARTITIONED BY (ingested_period)
"""

spark.sql(query)

# COMMAND ----------

# MAGIC %md ###sf_campaign

# COMMAND ----------

query = f"""
CREATE TABLE IF NOT EXISTS {catalog}.bronze.sf_campaign (
  sid_id STRING NOT NULL,
  actual_cost STRING,
  amount_all_opportunities DOUBLE,
  amount_won_opportunities DOUBLE,
  budgeted_cost STRING,
  buyer_type STRING,
  campaign_member_record_type_id STRING,
  converted_leads DOUBLE,
  created_by_id STRING,
  created_date TIMESTAMP,
  currency_iso_code STRING,
  end_date TIMESTAMP,
  expected_of_leads DOUBLE,
  expected_response STRING,
  expected_revenue STRING,
  goals STRING,
  hierarchy_actual_cost DOUBLE,
  hierarchy_amount_all_opportunities DOUBLE,
  hierarchy_amount_won_opportunities DOUBLE,
  hierarchy_budgeted_cost DOUBLE,
  hierarchy_expected_revenue DOUBLE,
  hierarchy_number_of_contacts INT,
  hierarchy_number_of_converted_leads INT,
  hierarchy_number_of_leads INT,
  hierarchy_number_of_opportunities INT,
  hierarchy_number_of_responses INT,
  hierarchy_number_of_won_opportunities INT,
  hierarchy_number_sent DOUBLE,
  is_active BOOLEAN,
  last_activity_date TIMESTAMP,
  last_modified_by_id STRING,
  last_modified_date TIMESTAMP,
  name STRING,
  number_of_contacts INT,
  number_of_converted_leads INT,
  number_of_leads INT,
  number_of_opportunities INT,
  number_of_responses INT,
  number_of_won_opportunities INT,
  number_sent DOUBLE,
  owner_id STRING,
  parent_id STRING,
  record_type_id STRING,
  start_date TIMESTAMP,
  status STRING,
  system_modstamp TIMESTAMP,
  target STRING,
  targeted_audience STRING,
  type STRING,
  campaign_type STRING,
  region STRING,
  fiscal_year STRING,
  segment STRING,
  vendor STRING,
  interest_topic STRING,
  analyst STRING,
  lead_source STRING,
  is_deleted BOOLEAN,
  ingested_file_name STRING,
  ingested_period STRING,
  ingested_date DATE,
  ingested_timestamp TIMESTAMP,
  hash_key STRING)
USING delta
PARTITIONED BY (ingested_period)
"""

spark.sql(query)

# COMMAND ----------

# MAGIC %md ###sf_campaign_member

# COMMAND ----------

query = f"""
CREATE TABLE IF NOT EXISTS {catalog}.bronze.sf_campaign_member (
  sid_id STRING NOT NULL,
  campaign_id STRING,
  contact_id STRING,
  created_by_id STRING,
  created_date TIMESTAMP,
  currency_iso_code STRING,
  first_responded_date TIMESTAMP,
  has_responded BOOLEAN,
  is_converted DOUBLE,
  last_modified_by_id STRING,
  last_modified_date TIMESTAMP,
  lead_id STRING,
  status STRING,
  system_modstamp TIMESTAMP,
  is_deleted BOOLEAN,
  ingested_file_name STRING,
  ingested_period STRING,
  ingested_date DATE,
  ingested_timestamp TIMESTAMP,
  hash_key STRING)
USING delta
PARTITIONED BY (ingested_period)
"""

spark.sql(query)

# COMMAND ----------

# MAGIC %md ###sf_contact

# COMMAND ----------

query = f"""
CREATE TABLE IF NOT EXISTS {catalog}.bronze.sf_contact (
  sid_id STRING NOT NULL,
  account_id STRING,
  asset_downloaded STRING,
  asst_email STRING,
  business_issues_described STRING,
  created_by_id STRING,
  created_date TIMESTAMP,
  currency_iso_code STRING,
  decision_maker BOOLEAN,
  department STRING,
  do_not_call BOOLEAN,
  email_domain STRING,
  email_opt_out BOOLEAN,
  email_bounced_date TIMESTAMP,
  email_bounced_reason STRING,
  has_opted_out_of_email BOOLEAN,
  job_level STRING,
  job_role STRING,
  last_activity_date TIMESTAMP,
  last_curequest_date STRING,
  last_cuupdate_date STRING,
  last_modified_by_id STRING,
  last_modified_date TIMESTAMP,
  lead_source_most_recent STRING,
  lead_source STRING,
  master_record_id STRING,
  owner_id STRING,
  peoplesoft_contact_id STRING,
  primary_contact BOOLEAN,
  program_type STRING,
  program_type_most_recent STRING,
  reports_to_id STRING,
  status STRING,
  system_modstamp TIMESTAMP,
  title STRING,
  sync_contact_with_marketo_c BOOLEAN,
  aql_date TIMESTAMP,
  contact_region STRING,
  behavior_score DOUBLE,
  demographic_score DOUBLE,
  do_not_call__c BOOLEAN,
  primary_business_division STRING,
  department__c STRING,
  is_deleted BOOLEAN,
  ingested_file_name STRING,
  ingested_period STRING,
  ingested_date DATE,
  ingested_timestamp TIMESTAMP,
  hash_key STRING
  )
USING delta
PARTITIONED BY (ingested_period)
"""

spark.sql(query)

# COMMAND ----------

# MAGIC %md ###sf_lead

# COMMAND ----------

query = f"""
CREATE TABLE IF NOT EXISTS {catalog}.bronze.sf_lead (
  sid_id STRING NOT NULL,
  alignment_of_business_fit STRING,
  alignment_score DOUBLE,
  annual_revenue BIGINT,
  asset_downloaded STRING,
  budget STRING,
  business_segment STRING,
  company STRING,
  company_size STRING,
  contact_and_referral_network STRING,
  contact_and_referral_score DOUBLE,
  converted_account_id STRING,
  converted_contact_id STRING,
  converted_date TIMESTAMP,
  converted_opportunity_id STRING,
  created_by_id STRING,
  created_date TIMESTAMP,
  currency_iso_code STRING,
  decision_maker BOOLEAN,
  decision_maker_score DOUBLE,
  decision_timeframe_score DOUBLE,
  department STRING,
  disqualified_notes STRING,
  do_not_use_named_accounts BOOLEAN,
  do_not_call BOOLEAN,
  email_domain STRING,
  email_bounced_date TIMESTAMP,
  email_bounced_reason STRING,
  has_opted_out_of_email BOOLEAN,
  industry STRING,
  is_converted BOOLEAN,
  is_unread_by_owner BOOLEAN,
  job_level STRING,
  job_role STRING,
  last_activity_date TIMESTAMP,
  last_modified_by_id STRING,
  last_modified_date TIMESTAMP,
  lead_owner_id STRING,
  lead_source_most_recent STRING,
  lead_source STRING,
  master_record_id STRING,
  number_of_employees INT,
  of_potential_license_users STRING,
  other_business_issue_described STRING,
  other_business_segment STRING,
  other_industry STRING,
  other_reason_disqualified STRING,
  owner_id STRING,
  program_type_most_recent STRING,
  project_defined BOOLEAN,
  rating STRING,
  reason_disqualified STRING,
  record_type_id STRING,
  source_id STRING,
  status STRING,
  system_modstamp TIMESTAMP,
  territory_id STRING,
  timeframe STRING,
  title STRING,
  tree_name STRING,
  type STRING,
  website STRING,
  account_industry_lead STRING,
  do_not_sync_lead_with_marketo BOOLEAN,
  lead_status_change_date TIMESTAMP,
  sum_total_lead_create_date STRING,
  sum_total_lead_id STRING,
  description STRING,
  business_issues_described STRING,
  matched_account_siccode INT,
  matched_account_industry STRING,
  matched_account_revenue DOUBLE,
  matched_account_size STRING,
  matched_market_segment STRING,
  matched_account_id STRING,
  lean_data__matched_buyer_persona STRING,
  lean_data__reporting_matched_account STRING,
  meeting_appointment_date_time TIMESTAMP,
  who_is_the_main_competitor STRING,
  demographic_score DOUBLE,
  behavior_score DOUBLE,
  aql_date TIMESTAMP,
  contact_region STRING,
  aql_primary_solution_of_interest STRING,
  ek_source_id STRING,
  external_referral STRING,
  internal_referral STRING,
  other_lead_source STRING,
  skype STRING,
  lead_source__c STRING,
  is_deleted BOOLEAN,
  ingested_file_name STRING,
  ingested_period STRING,
  ingested_date DATE,
  ingested_timestamp TIMESTAMP,
  hash_key STRING)
USING delta
PARTITIONED BY (ingested_period)
"""

spark.sql(query)

# COMMAND ----------

# MAGIC %md ###sf_object_territory_association

# COMMAND ----------

query = f"""
CREATE TABLE IF NOT EXISTS {catalog}.bronze.sf_object_territory_association (
  sid_id STRING NOT NULL,
  association_cause STRING,
  last_modified_by_id STRING,
  last_modified_date TIMESTAMP,
  object_id STRING,
  sobject_type STRING,
  system_modstamp TIMESTAMP,
  territory2id STRING,
  is_deleted BOOLEAN,
  ingested_file_name STRING,
  ingested_period STRING,
  ingested_date DATE,
  ingested_timestamp TIMESTAMP,
  hash_key STRING)
USING delta
PARTITIONED BY (ingested_period)
"""

spark.sql(query)

# COMMAND ----------

# MAGIC %md ###sf_opportunity

# COMMAND ----------

query = f"""
CREATE TABLE IF NOT EXISTS {catalog}.bronze.sf_opportunity (
  sid_id STRING NOT NULL,
  account_id STRING,
  agreement_type STRING,
  amount DOUBLE,
  at_risk BOOLEAN,
  at_risk_yes_no STRING,
  billing_contact STRING,
  books_site_type STRING,
  budget STRING,
  campaign_id STRING,
  close_date TIMESTAMP,
  created_outclause_opp BOOLEAN,
  created_by_id STRING,
  created_date TIMESTAMP,
  currency_iso_code STRING,
  dialogue STRING,
  end_date STRING,
  expire_date TIMESTAMP,
  forecast_category_read_only STRING,
  forecast_date TIMESTAMP,
  forecast_month STRING,
  forecast_quarter STRING,
  forecast_category STRING,
  forecast_category_name STRING,
  fortune_500_yes_no STRING,
  global_2000_yes_no STRING,
  has_opportunity_line_item BOOLEAN,
  ilt STRING,
  industry STRING,
  is_closed BOOLEAN,
  is_won BOOLEAN,
  knowledge_center_editor STRING,
  knowledge_centers STRING,
  last_activity_date TIMESTAMP,
  last_modified_by_id STRING,
  last_modified_date TIMESTAMP,
  lead_source_most_recent STRING,
  lead_source STRING,
  live_learning STRING,
  manager_comments STRING,
  mobile_links STRING,
  name STRING,
  next_step STRING,
  opportunity_number STRING,
  other_business_issue_described STRING,
  parent_opportunity STRING,
  pricebook2id STRING,
  primary_campaign_source_lead_source STRING,
  primary_campaign_source_program_type STRING,
  probability DOUBLE,
  program_type_most_recent STRING,
  pull_forward BOOLEAN,
  pull_forward_yes_no STRING,
  record_type_id STRING,
  segment STRING,
  sla STRING,
  stage_name STRING,
  start_date TIMESTAMP,
  system_modstamp TIMESTAMP,
  termlengthnumber DOUBLE,
  timeframe STRING,
  type STRING,
  account_type1 STRING,
  compliance_annual_amount STRING,
  compliance_stub_amount STRING,
  custom_annual_amount STRING,
  custom_net_amount STRING,
  service_annual_amount STRING,
  service_net_amount STRING,
  service_net_stub_amount STRING,
  service_stub_amount STRING,
  regular_annual_amount STRING,
  regular_stub_amount STRING,
  stub_year STRING,
  ss_ml STRING,
  bdc_meeting_tracker_name STRING,
  reasons_for_opportunity_win STRING,
  length_of_sales_cycle_win STRING,
  did_you_utilize_the_rfp_team_win STRING,
  result_of_opportunity_win STRING,
  loyalty_survey_or_interview_win STRING,
  additional_comments_win STRING,
  not_a_loss STRING,
  reason_for_this_opportunity_loss STRING,
  length_of_sales_cycle_loss STRING,
  did_you_utilize_the_rfp_team_loss STRING,
  result_of_opportunity_loss STRING,
  loyalty_survey_or_interview_loss STRING,
  additional_review_loss STRING,
  leadership_annual_amount STRING,
  leadership_stub_amount STRING,
  leadership_upgrade_amount STRING,
  welch_way_annual_amount STRING,
  welch_way_stub_amount STRING,
  welch_way_upgrade_amount STRING,
  compliance_upgrade_amount STRING,
  custom_net_upgrade_amount STRING,
  service_net_upgrade_amount STRING,
  regular_upgrade_amount STRING,
  solutions_defined STRING,
  pricing_agreed STRING,
  contract_delivered STRING,
  contract_redlines_complete STRING,
  mobilizer_identified_and_engaged STRING,
  evaluation_pilot_complete STRING,
  process_and_timelines_known STRING,
  government_contract_vehicle STRING,
  contacts_in_buying_team_5_4 STRING,
  next_step_comments STRING,
  original_parent_opp_type STRING,
  original_customer_type STRING,
  original_customer_status STRING,
  competitive_activity STRING,
  competitive_activity_other STRING,
  referring_sales_rep STRING,
  referral_type STRING,
  sum_total_opportunity_number STRING,
  opportunity_score DOUBLE,
  at_risk_green_upside_conversion STRING,
  quote_id STRING,
  agreement_start_date TIMESTAMP,
  quote_option_name STRING,
  closed_by STRING,
  what_is_the_compelling_event STRING,
  agreement_end_date TIMESTAMP,
  why_skillsoft_sum_total STRING,
  what_is_the_competitive_landscape STRING,
  what_is_our_executive_strategy STRING,
  what_are_the_barriers_or_risks STRING,
  next_step_date TIMESTAMP,
  year_one_oi DOUBLE,
  total_contract_amount DOUBLE,
  project_closure_date STRING,
  stub_term_days STRING,
  cdm_last_updated TIMESTAMP,
  last_date_w_rvp_mgr STRING,
  top_deal STRING,
  top_deal_bu STRING,
  client_executive_sponsor STRING,
  total_risk_grade DOUBLE,
  account_plan_completed STRING,
  risk_solution INT,
  risk_authority INT,
  budget_risk INT,
  risk_strategic INT,
  risk_timing INT,
  contract_year STRING,
  master_opp_number STRING,
  master_opportunity STRING,
  parent_opp_number STRING,
  management_adjustment STRING,
  oppty_source STRING,
  accepted_date TIMESTAMP,
  rejected_date TIMESTAMP,
  job_role STRING,
  meeting_appointment_date_time TIMESTAMP,
  lead_qualification_date TIMESTAMP,
  marketing_opportunity_age DOUBLE,
  business_issues_described STRING,
  contact_id STRING,
  reason_for_oppty_win_loss STRING,
  detail_of_oppty_win_loss STRING,
  account_id_18_char STRING,
  at_risk_green_upside_convtd_amt DOUBLE,
  auto_renewal_language BOOLEAN,
  books24x7_sub_id STRING,
  compliance_content_needed_in_lms BOOLEAN,
  contract_end_date TIMESTAMP,
  count_of_service_products DOUBLE,
  created_by_role STRING,
  created_expire_opp BOOLEAN,
  custom_distribution_amount DOUBLE,
  custom_redistribution_percent DOUBLE,
  custom_reps DOUBLE,
  customer_key INT,
  eligible_for_q3_q4_fy20_promotion BOOLEAN,
  expected_revenue DOUBLE,
  forecast_year INT,
  forecast_year_fy STRING,
  has_default_line_items DOUBLE,
  has_open_activity BOOLEAN,
  has_overdue_task BOOLEAN,
  is_excluded_from_territory2filter BOOLEAN,
  lid__is_influenced BOOLEAN,
  mind_leaders BOOLEAN,
  mkto_si__marketo_analyzer STRING,
  mkto_si__sales_insight STRING,
  next_fiscal_year_start_date TIMESTAMP,
  no_of_forecast_lines DOUBLE,
  not_modified BOOLEAN,
  o_push_counter STRING,
  of_active_splits DOUBLE,
  of_compliance_rep DOUBLE,
  of_gbdes DOUBLE,
  of_lgs DOUBLE,
  of_product_splits DOUBLE,
  of_products DOUBLE,
  of_products_per_term DOUBLE,
  of_standard_custom_users DOUBLE,
  opp_max_end_date TIMESTAMP,
  opp_min_forecast_date TIMESTAMP,
  opp_owner_division STRING,
  opportunit_owner1 STRING,
  opportunity_account_name STRING,
  outclause BOOLEAN,
  owner_id__c STRING,
  professional_services_recipient STRING,
  ps_spif_ineligible BOOLEAN,
  salesforce_id STRING,
  send_ps_survey BOOLEAN,
  skillport_to_sum_total_learn_migration BOOLEAN,
  splits DOUBLE,
  standard_custom_redistribution_percent DOUBLE,
  sum_of_custom_percent DOUBLE,
  sum_of_percent DOUBLE,
  sum_of_standard_custom_percent DOUBLE,
  term_length INT,
  total_compliance_amount DOUBLE,
  total_contract_value_year_1 DOUBLE,
  total_core_amount DOUBLE,
  total_service_gross_amount DOUBLE,
  total_shadow_amount DOUBLE,
  total_standard_amount DOUBLE,
  unique_opportunity_count DOUBLE,
  use_yearly_allocation BOOLEAN,
  year_1_nb_upgrade_oi DOUBLE,
  year_1_standard_product_splits DOUBLE,
  sales_motion STRING,
  out_clause STRING,
  owner_id STRING,
  lead_source__c STRING,
  id STRING,
  is_deleted BOOLEAN,
  record_type_name STRING,
  type_gk STRING,
  opportunity_close_plan STRING,
  fiscal STRING,
  fiscal_quarter INT,
  fiscal_year INT,
  ingested_file_name STRING,
  ingested_period STRING,
  ingested_date DATE,
  ingested_timestamp TIMESTAMP,
  hash_key STRING)
USING delta
PARTITIONED BY (ingested_period)
"""

spark.sql(query)

# COMMAND ----------

# MAGIC %md ###sf_opportunity_contact_role

# COMMAND ----------

query = f"""
CREATE TABLE IF NOT EXISTS {catalog}.bronze.sf_opportunity_contact_role (
  sid_id STRING NOT NULL,
  contact_id STRING,
  created_by_id STRING,
  created_date TIMESTAMP,
  is_primary BOOLEAN,
  last_modified_by_id STRING,
  last_modified_date TIMESTAMP,
  opportunity_id STRING,
  role STRING,
  is_deleted BOOLEAN,
  system_modstamp TIMESTAMP,
  ingested_file_name STRING,
  ingested_period STRING,
  ingested_date DATE,
  ingested_timestamp TIMESTAMP,
  hash_key STRING)
USING delta
PARTITIONED BY (ingested_period)
"""

spark.sql(query)

# COMMAND ----------

# MAGIC %md ###sf_opportunity_forecast

# COMMAND ----------

query = f"""
CREATE TABLE IF NOT EXISTS {catalog}.bronze.sf_opportunity_forecast (
  sid_id STRING NOT NULL,
  closed_amount DOUBLE,
  commission_type STRING,
  core_amount DOUBLE,
  core_split_percent DOUBLE,
  created_by_id STRING,
  created_date TIMESTAMP,
  credit_type STRING,
  currency_iso_code STRING,
  end_date TIMESTAMP,
  forecast_amount DOUBLE,
  forecast_category STRING,
  forecast_compliance_amount DOUBLE,
  forecast_custom_amount DOUBLE,
  forecast_custom_net_amount DOUBLE,
  forecast_date TIMESTAMP,
  forecast_fyqtr STRING,
  forecast_month STRING,
  forecast_qtrfy STRING,
  forecast_quarter STRING,
  forecast_regular_amount DOUBLE,
  forecast_service_amount DOUBLE,
  forecast_service_net_amount DOUBLE,
  forecast_year STRING,
  last_activity_date STRING,
  last_modified_by_id STRING,
  last_modified_date TIMESTAMP,
  name STRING,
  opportunity STRING,
  quarter_date TIMESTAMP,
  quota_lookup STRING,
  revenue_type STRING,
  sales_user STRING,
  shadow_amount DOUBLE,
  shadow_split_percent DOUBLE,
  start_date TIMESTAMP,
  system_modstamp TIMESTAMP,
  team_member_role STRING,
  forecast_leadership_amount DOUBLE,
  forecast_welch_way_amount DOUBLE,
  product STRING,
  product_amount DOUBLE,
  stub BOOLEAN,
  default BOOLEAN,
  commission_role STRING,
  term INT,
  expire_core_offset DOUBLE,
  expire_shadow_offset DOUBLE,
  total_core_value DOUBLE,
  total_shadow_value DOUBLE,
  closed_amount_shadow DOUBLE,
  combined_amount DOUBLE,
  comm_rule_number STRING,
  connection_received_id STRING,
  connection_sent_id STRING,
  days_closed DOUBLE,
  end_date_month STRING,
  end_date_quarter STRING,
  end_date_year STRING,
  forecast_category_for_db STRING,
  forecast_amount_shadow DOUBLE,
  forecast_month_sort STRING,
  product_18_id STRING,
  record_type_id STRING,
  sales_motion STRING,
  sales_user_avp STRING,
  sales_user_cvp STRING,
  sales_user_role STRING,
  sales_user_rvp STRING,
  total_core_and_shadow_value DOUBLE,
  is_deleted BOOLEAN,
  ingested_file_name STRING,
  ingested_period STRING,
  ingested_date DATE,
  ingested_timestamp TIMESTAMP,
  hash_key STRING)
USING delta
PARTITIONED BY (ingested_period)
"""

spark.sql(query)

# COMMAND ----------

# MAGIC %md ###sf_opportunity_history

# COMMAND ----------

query = f"""
CREATE TABLE IF NOT EXISTS {catalog}.bronze.sf_opportunity_history (
  sid_id STRING NOT NULL,
  amount DOUBLE,
  close_date TIMESTAMP,
  created_by_id STRING,
  created_date TIMESTAMP,
  currency_iso_code STRING,
  expected_revenue DOUBLE,
  forecast_category STRING,
  opportunity_id STRING,
  probability DOUBLE,
  stage_name STRING,
  system_modstamp TIMESTAMP,
  is_deleted BOOLEAN,
  ingested_file_name STRING,
  ingested_period STRING,
  ingested_date DATE,
  ingested_timestamp TIMESTAMP,
  hash_key STRING)
USING delta
PARTITIONED BY (ingested_period)
"""

spark.sql(query)

# COMMAND ----------

# MAGIC %md ###sf_product

# COMMAND ----------

query = f"""
CREATE TABLE IF NOT EXISTS {catalog}.bronze.sf_product (
  sid_id STRING NOT NULL,
  all_clauses STRING,
  amnesty_authorized_audience_required STRING,
  asset_id STRING,
  available_for_e_agreements STRING,
  chapters_to_go_available STRING,
  clause_header STRING,
  comm_type STRING,
  contract_product_family STRING,
  contract_product_name STRING,
  course_objects_available STRING,
  created_by_id STRING,
  created_date TIMESTAMP,
  credit_type STRING,
  currency_iso_code STRING,
  deployment_method_required STRING,
  family STRING,
  finance_prod_id STRING,
  finance_product_description STRING,
  is_active BOOLEAN,
  kit BOOLEAN,
  last_modified_by_id STRING,
  last_modified_date TIMESTAMP,
  localizations_available STRING,
  name STRING,
  number_of_revenue_installments STRING,
  product_group STRING,
  product_line STRING,
  product_pillar STRING,
  product_type STRING,
  product_code STRING,
  reseller_bundle_product STRING,
  reseller_skillsoft_product STRING,
  revenue_installment_period STRING,
  revenue_schedule_type STRING,
  salesforce_id STRING,
  sb_id STRING,
  source_id STRING,
  st_cs_reporting_product STRING,
  system_modstamp TIMESTAMP,
  titles_required BOOLEAN,
  type STRING,
  usage STRING,
  year_1_product BOOLEAN,
  sbu STRING,
  product_classifications STRING,
  is_prof_service BOOLEAN,
  is_deleted BOOLEAN,
  ingested_file_name STRING,
  ingested_period STRING,
  ingested_date DATE,
  ingested_timestamp TIMESTAMP,
  hash_key STRING)
USING delta
PARTITIONED BY (ingested_period)
"""

spark.sql(query)

# COMMAND ----------

# MAGIC %md ###sf_territory

# COMMAND ----------

query = f"""
CREATE TABLE IF NOT EXISTS {catalog}.bronze.sf_territory (
  sid_id STRING NOT NULL,
  name STRING,
  territory2model_id STRING,
  territory2type_id STRING,
  parent_territory2id STRING,
  account_access_level STRING,
  case_access_level STRING,
  contact_access_level STRING,
  currency_iso_code STRING,
  description STRING,
  developer_name STRING,
  forecast_user_id STRING,
  last_modified_by_id STRING,
  last_modified_date TIMESTAMP,
  opportunity_access_level STRING,
  system_modstamp TIMESTAMP,
  ingested_file_name STRING,
  ingested_period STRING,
  ingested_date DATE,
  ingested_timestamp TIMESTAMP,
  hash_key STRING)
USING delta
PARTITIONED BY (ingested_period)
TBLPROPERTIES (
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2')
"""

spark.sql(query)

# COMMAND ----------

# MAGIC %md ###sf_user

# COMMAND ----------

query = f"""
CREATE TABLE IF NOT EXISTS {catalog}.bronze.sf_user (
  sid_id STRING NOT NULL,
  about_me STRING,
  account_id STRING,
  alias STRING,
  area STRING,
  avp STRING,
  call_center_id STRING,
  city STRING,
  commission_role STRING,
  community_nickname STRING,
  company_name STRING,
  contact_id STRING,
  country STRING,
  created_by_id STRING,
  created_date TIMESTAMP,
  currency_iso_code STRING,
  cvp STRING,
  default_currency_iso_code STRING,
  delegated_approver_id STRING,
  department STRING,
  division STRING,
  effective_date TIMESTAMP,
  email STRING,
  email_encoding_key STRING,
  empl_id STRING,
  employee_number STRING,
  expiration_date TIMESTAMP,
  extension STRING,
  fax STRING,
  federation_identifier STRING,
  first_name STRING,
  forecast_enabled BOOLEAN,
  is_active BOOLEAN,
  language_locale_key STRING,
  last_login_date TIMESTAMP,
  last_modified_by_id STRING,
  last_modified_date TIMESTAMP,
  last_name STRING,
  last_password_change_date TIMESTAMP,
  lead_generator_id STRING,
  locale_sid_key STRING,
  manager_email STRING,
  manager_formula STRING,
  manager_id STRING,
  mobile_phone STRING,
  name STRING,
  offline_pda_trial_expiration_date STRING,
  offline_trial_expiration_date STRING,
  phone STRING,
  postal_code STRING,
  profile_id STRING,
  receives_admin_info_emails BOOLEAN,
  receives_info_emails BOOLEAN,
  rvp STRING,
  state STRING,
  street STRING,
  system_modstamp TIMESTAMP,
  time_zone_sid_key STRING,
  title STRING,
  username STRING,
  user_permissions_avantgo_user BOOLEAN,
  user_permissions_call_center_auto_login BOOLEAN,
  user_permissions_marketing_user BOOLEAN,
  user_permissions_offline_user BOOLEAN,
  user_permissions_sfcontent_user BOOLEAN,
  user_preferences_activity_reminders_popup BOOLEAN,
  user_preferences_apex_pages_developer_mode BOOLEAN,
  user_preferences_event_reminders_checkbox_default BOOLEAN,
  user_preferences_reminder_sound_off BOOLEAN,
  user_preferences_task_reminders_checkbox_default BOOLEAN,
  user_role_id STRING,
  user_type STRING,
  manager_level STRING,
  exclude_from_dw_reporting BOOLEAN,
  marketing_region STRING,
  dir STRING,
  evp STRING,
  badge_text STRING,
  digest_frequency STRING,
  sum_total_user_id STRING,
  user_18_id STRING,
  users_name_escaped STRING,
  ingested_file_name STRING,
  ingested_period STRING,
  ingested_date DATE,
  ingested_timestamp TIMESTAMP,
  hash_key STRING)
USING delta
PARTITIONED BY (ingested_period)
"""

spark.sql(query)

# COMMAND ----------

# MAGIC %md ###sf_user_role

# COMMAND ----------

query = f"""
CREATE TABLE IF NOT EXISTS {catalog}.bronze.sf_user_role (
  sid_id STRING NOT NULL,
  case_access_for_account_owner STRING,
  contact_access_for_account_owner STRING,
  forecast_user_id STRING,
  last_modified_by_id STRING,
  last_modified_date TIMESTAMP,
  may_forecast_manager_share BOOLEAN,
  name STRING,
  opportunity_access_for_account_owner STRING,
  parent_role_id STRING,
  portal_account_id STRING,
  portal_account_owner_id STRING,
  portal_type STRING,
  rollup_description STRING,
  system_modstamp TIMESTAMP,
  ingested_file_name STRING,
  ingested_period STRING,
  ingested_date DATE,
  ingested_timestamp TIMESTAMP,
  hash_key STRING)
USING delta
PARTITIONED BY (ingested_period)
"""

spark.sql(query)



# COMMAND ----------

# MAGIC %md ##Salesforce Tables

# COMMAND ----------

# MAGIC %md ###sf_account

# COMMAND ----------

query = f"""
CREATE TABLE IF NOT EXISTS {catalog}.silver.sf_account (
  sid_id STRING NOT NULL,
  account_id STRING,
  account_plans DECIMAL(18,0),
  account_type STRING,
  active_prospecting_by STRING,
  alignment_score DECIMAL(18,0),
  annual_revenue DECIMAL(18,0) DEFAULT 0,
  billing_city STRING,
  billing_country STRING,
  billing_postal_code STRING,
  billing_state STRING,
  billing_street STRING,
  bo_id STRING,
  company_size STRING,
  contact_and_referral_score DECIMAL(18,0),
  contract_end_date TIMESTAMP,
  contract_start_date TIMESTAMP,
  created_by_id STRING,
  created_date TIMESTAMP,
  currency_iso_code STRING,
  duns_number STRING,
  expires DECIMAL(18,0),
  fax STRING,
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
  of_opportunities DECIMAL(18,0),
  olsa STRING,
  opportunity_stage STRING,
  original_pilot BOOLEAN,
  original_pilot_yes_no STRING,
  other_partner STRING,
  owner_id STRING,
  ownership STRING,
  parent_id STRING,
  phone STRING,
  population_score DECIMAL(18,0),
  priority_score DECIMAL(18,0),
  project_defined BOOLEAN,
  pscrm_company_id STRING,
  record_type_id STRING,
  renewal_evaluation_required2 STRING,
  revenue_gen DECIMAL(18,2) DEFAULT 0,
  rrfs DECIMAL(18,0),
  rrfs_complete DECIMAL(18,0),
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
  premier_support_customer BOOLEAN,
  type_platform STRING,
  type_toolbook STRING,
  band STRING,
  is_customer_portal BOOLEAN,
  sumt_dba STRING,
  sum_total_account_id STRING,
  sum_total_client STRING,
  is_partner BOOLEAN,
  support_account BOOLEAN,
  nbr_of_account_team_members DECIMAL(18,0),
  ps_spif_ineligible BOOLEAN,
  customer_key STRING,
  industry STRING,
  won_opportunities DECIMAL(18,0),
  no_opportunities_closed DECIMAL(18,0),
  named_account STRING,
  market_segment_value DECIMAL(18,0),
  market_segment STRING,
  discover_org_employees DECIMAL(8,0),
  db_state_province_code STRING,
  db_revenue_value DECIMAL(18,2),
  db_major_industry STRING,
  db_employees_here DECIMAL(18,0),
  db_duplicate STRING,
  db_duns STRING,
  db_country_code STRING,
  db_business_name STRING,
  days_since_last_activity DECIMAL(18,0),
  count_of_opportunities_open DECIMAL(18,0),
  count_of_sum_total_csms DECIMAL(2,0),
  count_of_sum_total_csds DECIMAL(2,0),
  conga_output_format STRING,
  dozisf__zoom_info_id STRING,
  current_deployment STRING,
  g250 STRING,
  account_segment STRING,
  customer_persona STRING,
  business_division STRING,
  is_deleted BOOLEAN,
  ingested_file_name STRING,
  ingested_timestamp TIMESTAMP,
  hash_key STRING,
  record_expiry_period STRING,
  record_effective_date TIMESTAMP,
  record_expiry_date TIMESTAMP,
  record_current_flag INT)
USING delta
PARTITIONED BY (record_current_flag, record_expiry_period)
LOCATION 'abfss://onelake@{storage_account}.dfs.core.windows.net/silver/salesforce/sf_account'
TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')
"""
spark.sql(query)

# COMMAND ----------

# MAGIC %md ###sf_campaign

# COMMAND ----------

query = f"""
CREATE TABLE IF NOT EXISTS {catalog}.silver.sf_campaign (
  sid_id STRING NOT NULL,
  actual_cost DECIMAL(18,0) DEFAULT 0,
  amount_all_opportunities DECIMAL(18,0) DEFAULT 0,
  amount_won_opportunities DECIMAL(18,0) DEFAULT 0,
  budgeted_cost DECIMAL(18,0) DEFAULT 0,
  buyer_type STRING,
  campaign_member_record_type_id STRING,
  converted_leads DECIMAL(18,0),
  created_by_id STRING,
  created_date TIMESTAMP,
  currency_iso_code STRING,
  end_date TIMESTAMP,
  expected_of_leads DECIMAL(18,0),
  expected_response DECIMAL(10,2),
  expected_revenue DECIMAL(18,0) DEFAULT 0,
  goals STRING,
  hierarchy_actual_cost DECIMAL(18,0) DEFAULT 0,
  hierarchy_amount_all_opportunities DECIMAL(18,0) DEFAULT 0,
  hierarchy_amount_won_opportunities DECIMAL(18,0) DEFAULT 0,
  hierarchy_budgeted_cost DECIMAL(18,0) DEFAULT 0,
  hierarchy_expected_revenue DECIMAL(18,0) DEFAULT 0,
  hierarchy_number_of_contacts INT,
  hierarchy_number_of_converted_leads INT,
  hierarchy_number_of_leads INT,
  hierarchy_number_of_opportunities INT,
  hierarchy_number_of_responses INT,
  hierarchy_number_of_won_opportunities INT,
  hierarchy_number_sent DECIMAL(18,0),
  is_active BOOLEAN,
  last_activity_date TIMESTAMP,
  last_modified_by_id STRING,
  last_modified_date TIMESTAMP,
  name STRING,
  number_of_contacts INT,
  number_of_converted_leads INT,
  number_of_leads INT,
  number_of_opportunities INT,
  number_of_responses INT,
  number_of_won_opportunities INT,
  number_sent DECIMAL(18,0),
  owner_id STRING,
  parent_id STRING,
  record_type_id STRING,
  start_date TIMESTAMP,
  status STRING,
  system_modstamp TIMESTAMP,
  target STRING,
  targeted_audience STRING,
  type STRING,
  campaign_type STRING,
  region STRING,
  fiscal_year STRING,
  segment STRING,
  vendor STRING,
  interest_topic STRING,
  analyst STRING,
  lead_source STRING,
  is_deleted BOOLEAN,
  ingested_file_name STRING,
  ingested_timestamp TIMESTAMP,
  hash_key STRING,
  record_expiry_period STRING,
  record_effective_date TIMESTAMP,
  record_expiry_date TIMESTAMP,
  record_current_flag INT)
USING delta
PARTITIONED BY (record_current_flag, record_expiry_period)
LOCATION 'abfss://onelake@{storage_account}.dfs.core.windows.net/silver/salesforce/sf_campaign'   
TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')        
"""
spark.sql(query)

# COMMAND ----------

# MAGIC %md ###sf_campaign_member

# COMMAND ----------

query = f"""
CREATE TABLE IF NOT EXISTS {catalog}.silver.sf_campaign_member (
  sid_id STRING NOT NULL,
  campaign_id STRING,
  contact_id STRING,
  created_by_id STRING,
  created_date TIMESTAMP,
  currency_iso_code STRING,
  first_responded_date TIMESTAMP,
  has_responded BOOLEAN,
  is_converted DECIMAL(9,0),
  last_modified_by_id STRING,
  last_modified_date TIMESTAMP,
  lead_id STRING,
  status STRING,
  system_modstamp TIMESTAMP,
  is_deleted BOOLEAN,
  ingested_file_name STRING,
  ingested_timestamp TIMESTAMP,
  hash_key STRING,
  record_expiry_period STRING,
  record_effective_date TIMESTAMP,
  record_expiry_date TIMESTAMP,
  record_current_flag INT)
USING delta
PARTITIONED BY (record_current_flag, record_expiry_period)
LOCATION 'abfss://onelake@{storage_account}.dfs.core.windows.net/silver/salesforce/sf_campaign_member'
"""
spark.sql(query)

# COMMAND ----------

# MAGIC %md ###sf_contact

# COMMAND ----------

query = f"""
CREATE TABLE IF NOT EXISTS {catalog}.silver.sf_contact (
  sid_id STRING NOT NULL,
  account_id STRING,
  asset_downloaded STRING,
  asst_email STRING,
  business_issues_described STRING,
  created_by_id STRING,
  created_date TIMESTAMP,
  currency_iso_code STRING,
  decision_maker BOOLEAN,
  department STRING,
  do_not_call BOOLEAN,
  email_domain STRING,
  email_opt_out BOOLEAN,
  email_bounced_date TIMESTAMP,
  email_bounced_reason STRING,
  has_opted_out_of_email BOOLEAN,
  job_level STRING,
  job_role STRING,
  last_activity_date TIMESTAMP,
  last_curequest_date TIMESTAMP,
  last_cuupdate_date TIMESTAMP,
  last_modified_by_id STRING,
  last_modified_date TIMESTAMP,
  lead_source_most_recent STRING,
  lead_source STRING,
  master_record_id STRING,
  owner_id STRING,
  peoplesoft_contact_id STRING,
  primary_contact BOOLEAN,
  program_type STRING,
  program_type_most_recent STRING,
  reports_to_id STRING,
  status STRING,
  system_modstamp TIMESTAMP,
  title STRING,
  sync_contact_with_marketo_c BOOLEAN,
  aql_date DATE,
  contact_region STRING,
  behavior_score DECIMAL(5,2),
  demographic_score DECIMAL(5,2),
  do_not_call__c BOOLEAN,
  primary_business_division STRING,
  department__c STRING,
  is_deleted BOOLEAN,
  ingested_file_name STRING,
  ingested_timestamp TIMESTAMP,
  hash_key STRING,
  record_expiry_period STRING,
  record_effective_date TIMESTAMP,
  record_expiry_date TIMESTAMP,
  record_current_flag INT)
USING delta
PARTITIONED BY (record_current_flag, record_expiry_period)
LOCATION 'abfss://onelake@{storage_account}.dfs.core.windows.net/silver/salesforce/sf_contact'
"""
spark.sql(query)

# COMMAND ----------

# MAGIC %md ###sf_lead

# COMMAND ----------

query = f"""
CREATE TABLE IF NOT EXISTS {catalog}.silver.sf_lead (
  sid_id STRING NOT NULL,
  alignment_of_business_fit STRING,
  alignment_score DECIMAL(18,0),
  annual_revenue DECIMAL(18,0) DEFAULT 0,
  asset_downloaded STRING,
  budget STRING,
  business_segment STRING,
  company STRING,
  company_size STRING,
  contact_and_referral_network STRING,
  contact_and_referral_score DECIMAL(18,0),
  converted_account_id STRING,
  converted_contact_id STRING,
  converted_date TIMESTAMP,
  converted_opportunity_id STRING,
  created_by_id STRING,
  created_date TIMESTAMP,
  currency_iso_code STRING,
  decision_maker BOOLEAN,
  decision_maker_score DECIMAL(18,0),
  decision_timeframe_score DECIMAL(18,0),
  department STRING,
  disqualified_notes STRING,
  do_not_use_named_accounts BOOLEAN,
  do_not_call BOOLEAN,
  email_domain STRING,
  email_bounced_date TIMESTAMP,
  email_bounced_reason STRING,
  has_opted_out_of_email BOOLEAN,
  industry STRING,
  is_converted BOOLEAN,
  is_unread_by_owner BOOLEAN,
  job_level STRING,
  job_role STRING,
  last_activity_date TIMESTAMP,
  last_modified_by_id STRING,
  last_modified_date TIMESTAMP,
  lead_owner_id STRING,
  lead_source_most_recent STRING,
  lead_source STRING,
  master_record_id STRING,
  number_of_employees INT,
  of_potential_license_users STRING,
  other_business_issue_described STRING,
  other_business_segment STRING,
  other_industry STRING,
  other_reason_disqualified STRING,
  owner_id STRING,
  program_type_most_recent STRING,
  project_defined BOOLEAN,
  rating STRING,
  reason_disqualified STRING,
  record_type_id STRING,
  source_id STRING,
  status STRING,
  system_modstamp TIMESTAMP,
  territory_id STRING,
  timeframe STRING,
  title STRING,
  tree_name STRING,
  type STRING,
  website STRING,
  account_industry_lead STRING,
  do_not_sync_lead_with_marketo BOOLEAN,
  lead_status_change_date TIMESTAMP,
  sum_total_lead_create_date TIMESTAMP,
  sum_total_lead_id STRING,
  description STRING,
  business_issues_described STRING,
  matched_account_siccode STRING,
  matched_account_industry STRING,
  matched_account_revenue DECIMAL(18,2)  DEFAULT 0,
  matched_account_size STRING,
  matched_market_segment STRING,
  matched_account_id STRING,
  lean_data__matched_buyer_persona STRING,
  lean_data__reporting_matched_account STRING,
  meeting_appointment_date_time TIMESTAMP,
  who_is_the_main_competitor STRING,
  demographic_score DECIMAL(5,2),
  behavior_score DECIMAL(5,2),
  aql_date DATE,
  contact_region STRING,
  aql_primary_solution_of_interest STRING,
  ek_source_id STRING,
  external_referral STRING,
  internal_referral STRING,
  other_lead_source STRING,
  skype STRING,
  lead_source__c STRING,
  is_deleted BOOLEAN,
  ingested_file_name STRING,
  ingested_timestamp TIMESTAMP,
  hash_key STRING,
  record_expiry_period STRING,
  record_effective_date TIMESTAMP,
  record_expiry_date TIMESTAMP,
  record_current_flag INT)
USING delta
PARTITIONED BY (record_current_flag, record_expiry_period)
LOCATION 'abfss://onelake@{storage_account}.dfs.core.windows.net/silver/salesforce/sf_lead'
TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')         
"""
spark.sql(query)

# COMMAND ----------

# MAGIC %md ###sf_object_territory_association

# COMMAND ----------

query = f"""
CREATE TABLE IF NOT EXISTS {catalog}.silver.sf_object_territory_association (
  sid_id STRING NOT NULL,
  association_cause STRING,
  last_modified_by_id STRING,
  last_modified_date TIMESTAMP,
  object_id STRING,
  sobject_type STRING,
  system_modstamp TIMESTAMP,
  territory2id STRING,
  is_deleted BOOLEAN,
  _rescued_data STRING,
  ingested_file_name STRING,
  ingested_timestamp TIMESTAMP,
  hash_key STRING,
  record_expiry_period STRING,
  record_effective_date TIMESTAMP,
  record_expiry_date TIMESTAMP,
  record_current_flag INT)
USING delta
PARTITIONED BY (record_current_flag, record_expiry_period)
LOCATION 'abfss://onelake@{storage_account}.dfs.core.windows.net/silver/salesforce/sf_object_territory_association'     
"""
spark.sql(query)

# COMMAND ----------

# MAGIC %md ###sf_opportunity

# COMMAND ----------

query = f"""
CREATE TABLE IF NOT EXISTS {catalog}.silver.sf_opportunity (
  sid_id STRING NOT NULL,
  account_id STRING,
  agreement_type STRING,
  amount DECIMAL(18,2) DEFAULT 0,
  at_risk BOOLEAN,
  at_risk_yes_no STRING,
  billing_contact STRING,
  books_site_type STRING,
  budget STRING,
  campaign_id STRING,
  close_date TIMESTAMP,
  created_outclause_opp BOOLEAN,
  created_by_id STRING,
  created_date TIMESTAMP,
  currency_iso_code STRING,
  dialogue STRING,
  end_date TIMESTAMP,
  expire_date TIMESTAMP,
  forecast_category_read_only STRING,
  forecast_date TIMESTAMP,
  forecast_month STRING,
  forecast_quarter STRING,
  forecast_category STRING,
  forecast_category_name STRING,
  fortune_500_yes_no STRING,
  global_2000_yes_no STRING,
  has_opportunity_line_item BOOLEAN,
  ilt STRING,
  industry STRING,
  is_closed BOOLEAN,
  is_won BOOLEAN,
  knowledge_center_editor STRING,
  knowledge_centers STRING,
  last_activity_date TIMESTAMP,
  last_modified_by_id STRING,
  last_modified_date TIMESTAMP,
  lead_source_most_recent STRING,
  lead_source STRING,
  live_learning STRING,
  manager_comments STRING,
  mobile_links STRING,
  name STRING,
  next_step STRING,
  opportunity_number STRING,
  other_business_issue_described STRING,
  parent_opportunity STRING,
  pricebook2id STRING,
  primary_campaign_source_lead_source STRING,
  primary_campaign_source_program_type STRING,
  probability DECIMAL(3,0),
  program_type_most_recent STRING,
  pull_forward BOOLEAN,
  pull_forward_yes_no STRING,
  record_type_id STRING,
  segment STRING,
  sla STRING,
  stage_name STRING,
  start_date TIMESTAMP,
  system_modstamp TIMESTAMP,
  termlengthnumber DECIMAL(18,0),
  timeframe STRING,
  type STRING,
  account_type1 STRING,
  compliance_annual_amount DECIMAL(12,2) DEFAULT 0,
  compliance_stub_amount DECIMAL(12,2) DEFAULT 0,
  custom_annual_amount DECIMAL(12,2) DEFAULT 0,
  custom_net_amount DECIMAL(12,2) DEFAULT 0,
  service_annual_amount DECIMAL(12,2) DEFAULT 0,
  service_net_amount DECIMAL(12,2) DEFAULT 0,
  service_net_stub_amount DECIMAL(12,2) DEFAULT 0,
  service_stub_amount DECIMAL(12,2) DEFAULT 0,
  regular_annual_amount DECIMAL(12,2) DEFAULT 0,
  regular_stub_amount DECIMAL(12,2) DEFAULT 0,
  stub_year STRING,
  ss_ml STRING,
  bdc_meeting_tracker_name STRING,
  reasons_for_opportunity_win STRING,
  length_of_sales_cycle_win STRING,
  did_you_utilize_the_rfp_team_win STRING,
  result_of_opportunity_win STRING,
  loyalty_survey_or_interview_win STRING,
  additional_comments_win STRING,
  not_a_loss STRING,
  reason_for_this_opportunity_loss STRING,
  length_of_sales_cycle_loss STRING,
  did_you_utilize_the_rfp_team_loss STRING,
  result_of_opportunity_loss STRING,
  loyalty_survey_or_interview_loss STRING,
  additional_review_loss STRING,
  leadership_annual_amount DECIMAL(12,2) DEFAULT 0,
  leadership_stub_amount DECIMAL(12,2) DEFAULT 0,
  leadership_upgrade_amount DECIMAL(12,2) DEFAULT 0,
  welch_way_annual_amount DECIMAL(12,2) DEFAULT 0,
  welch_way_stub_amount DECIMAL(12,2) DEFAULT 0,
  welch_way_upgrade_amount DECIMAL(12,2) DEFAULT 0,
  compliance_upgrade_amount DECIMAL(12,2) DEFAULT 0,
  custom_net_upgrade_amount DECIMAL(12,2) DEFAULT 0,
  service_net_upgrade_amount DECIMAL(12,2) DEFAULT 0,
  regular_upgrade_amount DECIMAL(12,2) DEFAULT 0,
  solutions_defined STRING,
  pricing_agreed STRING,
  contract_delivered STRING,
  contract_redlines_complete STRING,
  mobilizer_identified_and_engaged STRING,
  evaluation_pilot_complete STRING,
  process_and_timelines_known STRING,
  government_contract_vehicle STRING,
  contacts_in_buying_team_5_4 DECIMAL(3,0),
  next_step_comments STRING,
  original_parent_opp_type STRING,
  original_customer_type STRING,
  original_customer_status STRING,
  competitive_activity STRING,
  competitive_activity_other STRING,
  referring_sales_rep STRING,
  referral_type STRING,
  sum_total_opportunity_number STRING,
  opportunity_score DECIMAL(18,2),
  at_risk_green_upside_conversion DECIMAL(18,2),
  quote_id STRING,
  agreement_start_date TIMESTAMP,
  quote_option_name STRING,
  closed_by STRING,
  what_is_the_compelling_event STRING,
  agreement_end_date TIMESTAMP,
  why_skillsoft_sum_total STRING,
  what_is_the_competitive_landscape STRING,
  what_is_our_executive_strategy STRING,
  what_are_the_barriers_or_risks STRING,
  next_step_date TIMESTAMP,
  year_one_oi DECIMAL(18,2),
  total_contract_amount DECIMAL(18,2) DEFAULT 0,
  project_closure_date STRING,
  stub_term_days DECIMAL(18,0),
  cdm_last_updated TIMESTAMP,
  last_date_w_rvp_mgr TIMESTAMP,
  top_deal STRING,
  top_deal_bu STRING,
  client_executive_sponsor STRING,
  total_risk_grade DECIMAL(18,0),
  account_plan_completed TIMESTAMP,
  risk_solution STRING,
  risk_authority STRING,
  budget_risk STRING,
  risk_strategic STRING,
  risk_timing STRING,
  contract_year INT,
  master_opp_number STRING,
  master_opportunity STRING,
  parent_opp_number STRING,
  management_adjustment STRING,
  oppty_source STRING,
  accepted_date TIMESTAMP,
  rejected_date TIMESTAMP,
  job_role STRING,
  meeting_appointment_date_time TIMESTAMP,
  lead_qualification_date TIMESTAMP,
  marketing_opportunity_age DECIMAL(18,0),
  business_issues_described STRING,
  contact_id STRING,
  reason_for_oppty_win_loss STRING,
  detail_of_oppty_win_loss STRING,
  account_id_18_char STRING,
  at_risk_green_upside_convtd_amt DECIMAL(18,2),
  auto_renewal_language BOOLEAN,
  books24x7_sub_id STRING,
  compliance_content_needed_in_lms BOOLEAN,
  contract_end_date DATE,
  count_of_service_products DECIMAL(18,0),
  created_by_role STRING,
  created_expire_opp BOOLEAN,
  custom_distribution_amount DECIMAL(18,2) DEFAULT 0,
  custom_redistribution_percent DECIMAL(18,4) DEFAULT 0,
  custom_reps DECIMAL(18,0),
  customer_key STRING,
  eligible_for_q3_q4_fy20_promotion BOOLEAN,
  expected_revenue DECIMAL(18,2),
  forecast_year STRING,
  forecast_year_fy STRING,
  has_default_line_items DECIMAL(18,0),
  has_open_activity BOOLEAN,
  has_overdue_task BOOLEAN,
  is_excluded_from_territory2filter BOOLEAN,
  lid__is_influenced BOOLEAN,
  mind_leaders BOOLEAN,
  mkto_si__marketo_analyzer STRING,
  mkto_si__sales_insight STRING,
  fiscal STRING,
  fiscal_quarter INT,
  fiscal_year INT,
  next_fiscal_year_start_date TIMESTAMP,
  no_of_forecast_lines DECIMAL(18,0),
  not_modified STRING,
  o_push_counter STRING,
  of_active_splits DECIMAL(18,0),
  of_compliance_rep DECIMAL(18,0),
  of_gbdes DECIMAL(18,0),
  of_lgs DECIMAL(18,0),
  of_product_splits DECIMAL(18,0),
  of_products DECIMAL(18,0),
  of_products_per_term DECIMAL(18,0),
  of_standard_custom_users DECIMAL(18,0),
  opp_max_end_date TIMESTAMP,
  opp_min_forecast_date DATE,
  opp_owner_division STRING,
  opportunit_owner1 STRING,
  opportunity_account_name STRING,
  outclause BOOLEAN,
  owner_id__c STRING,
  professional_services_recipient STRING,
  ps_spif_ineligible BOOLEAN,
  salesforce_id STRING,
  send_ps_survey BOOLEAN,
  skillport_to_sum_total_learn_migration BOOLEAN,
  splits DECIMAL(18,0),
  standard_custom_redistribution_percent DECIMAL(18,4),
  sum_of_custom_percent DECIMAL(18,4),
  sum_of_percent DECIMAL(18,4),
  sum_of_standard_custom_percent DECIMAL(18,4),
  term_length STRING,
  total_compliance_amount DECIMAL(18,2),
  total_contract_value_year_1 DECIMAL(18,2),
  total_core_amount DECIMAL(18,2),
  total_service_gross_amount DECIMAL(18,2),
  total_shadow_amount DECIMAL(18,2),
  total_standard_amount DECIMAL(18,2),
  unique_opportunity_count DECIMAL(18,0),
  use_yearly_allocation BOOLEAN,
  year_1_nb_upgrade_oi DECIMAL(18,2),
  year_1_standard_product_splits DECIMAL(18,0),
  sales_motion STRING,
  out_clause STRING,
  owner_id STRING,
  lead_source__c STRING,
  id STRING,
  is_deleted BOOLEAN,
  record_type_name STRING,
  type_gk STRING,
  opportunity_close_plan STRING,
  _rescued_data STRING,
  ingested_file_name STRING,
  ingested_timestamp TIMESTAMP,
  hash_key STRING,
  record_expiry_period STRING,
  record_effective_date TIMESTAMP,
  record_expiry_date TIMESTAMP,
  record_current_flag INT)
USING delta
PARTITIONED BY (record_current_flag, record_expiry_period)
LOCATION 'abfss://onelake@{storage_account}.dfs.core.windows.net/silver/salesforce/sf_opportunity'
TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')
"""
spark.sql(query)

# COMMAND ----------

# MAGIC %md ###sf_opportunity_contact_role

# COMMAND ----------

query = f"""
CREATE TABLE IF NOT EXISTS {catalog}.silver.sf_opportunity_contact_role (
  sid_id STRING NOT NULL,
  contact_id STRING,
  created_by_id STRING,
  created_date TIMESTAMP,
  is_primary BOOLEAN,
  last_modified_by_id STRING,
  last_modified_date TIMESTAMP,
  opportunity_id STRING,
  role STRING,
  system_modstamp TIMESTAMP,
  is_deleted BOOLEAN,
  ingested_file_name STRING,
  ingested_timestamp TIMESTAMP,
  hash_key STRING,
  record_expiry_period STRING,
  record_effective_date TIMESTAMP,
  record_expiry_date TIMESTAMP,
  record_current_flag INT)
USING delta
PARTITIONED BY (record_current_flag, record_expiry_period)
LOCATION 'abfss://onelake@{storage_account}.dfs.core.windows.net/silver/salesforce/sf_opportunity_contact_role'
"""
spark.sql(query)

# COMMAND ----------

# MAGIC %md ###sf_opportunity_forecast

# COMMAND ----------

query = f"""
CREATE TABLE IF NOT EXISTS {catalog}.silver.sf_opportunity_forecast (
  sid_id STRING NOT NULL,
  closed_amount DECIMAL(18,2) DEFAULT 0,
  commission_type STRING,
  core_amount DECIMAL(18,2) DEFAULT 0,
  core_split_percent DECIMAL(5,2),
  created_by_id STRING,
  created_date TIMESTAMP,
  credit_type STRING,
  currency_iso_code STRING,
  end_date TIMESTAMP,
  forecast_amount DECIMAL(18,2) DEFAULT 0,
  forecast_category STRING,
  forecast_compliance_amount DECIMAL(18,2) DEFAULT 0,
  forecast_custom_amount DECIMAL(18,2) DEFAULT 0,
  forecast_custom_net_amount DECIMAL(18,2),
  forecast_date TIMESTAMP,
  forecast_fyqtr STRING,
  forecast_month STRING,
  forecast_qtrfy STRING,
  forecast_quarter STRING,
  forecast_regular_amount DECIMAL(18,2) DEFAULT 0,
  forecast_service_amount DECIMAL(18,2) DEFAULT 0,
  forecast_service_net_amount DECIMAL(18,2) DEFAULT 0,
  forecast_year STRING,
  last_activity_date TIMESTAMP,
  last_modified_by_id STRING,
  last_modified_date TIMESTAMP,
  name STRING,
  opportunity STRING,
  quarter_date TIMESTAMP,
  quota_lookup STRING,
  revenue_type STRING,
  sales_user STRING,
  shadow_amount DECIMAL(18,2) DEFAULT 0,
  shadow_split_percent DECIMAL(5,2),
  start_date TIMESTAMP,
  system_modstamp TIMESTAMP,
  team_member_role STRING,
  forecast_leadership_amount DECIMAL(18,2) DEFAULT 0,
  forecast_welch_way_amount DECIMAL(12,2) DEFAULT 0,
  product STRING,
  product_amount DECIMAL(18,2),
  stub BOOLEAN,
  default BOOLEAN,
  commission_role STRING,
  term STRING,
  expire_core_offset DECIMAL(18,2) DEFAULT 0,
  expire_shadow_offset DECIMAL(18,2) DEFAULT 0,
  total_core_value DECIMAL(18,2) DEFAULT 0,
  total_shadow_value DECIMAL(18,2) DEFAULT 0,
  closed_amount_shadow DECIMAL(18,2) DEFAULT 0,
  combined_amount DECIMAL(18,2) DEFAULT 0,
  comm_rule_number STRING,
  connection_received_id STRING,
  connection_sent_id STRING,
  days_closed DECIMAL(5,0),
  end_date_month STRING,
  end_date_quarter STRING,
  end_date_year STRING,
  forecast_category_for_db STRING,
  forecast_amount_shadow DECIMAL(18,2) DEFAULT 0,
  forecast_month_sort STRING,
  product_18_id STRING,
  record_type_id STRING,
  sales_motion STRING,
  sales_user_avp STRING,
  sales_user_cvp STRING,
  sales_user_role STRING,
  sales_user_rvp STRING,
  total_core_and_shadow_value DECIMAL(18,2) DEFAULT 0,
  is_deleted BOOLEAN,
  ingested_file_name STRING,
  ingested_timestamp TIMESTAMP,
  hash_key STRING,
  record_expiry_period STRING,
  record_effective_date TIMESTAMP,
  record_expiry_date TIMESTAMP,
  record_current_flag INT)
USING delta
PARTITIONED BY (record_current_flag, record_expiry_period)
LOCATION 'abfss://onelake@{storage_account}.dfs.core.windows.net/silver/salesforce/sf_opportunity_forecast'     
TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')    
"""
spark.sql(query)

# COMMAND ----------

# MAGIC %md ###sf_opportunity_history

# COMMAND ----------

query = f"""
CREATE TABLE IF NOT EXISTS {catalog}.silver.sf_opportunity_history (
  sid_id STRING NOT NULL,
  amount DECIMAL(18,2),
  close_date TIMESTAMP,
  created_by_id STRING,
  created_date TIMESTAMP,
  currency_iso_code STRING,
  expected_revenue DECIMAL(18,2),
  forecast_category STRING,
  opportunity_id STRING,
  probability DECIMAL(3,0),
  stage_name STRING,
  system_modstamp TIMESTAMP,
  is_deleted BOOLEAN,
  ingested_file_name STRING,
  ingested_timestamp TIMESTAMP,
  hash_key STRING,
  record_expiry_period STRING,
  record_effective_date TIMESTAMP,
  record_expiry_date TIMESTAMP,
  record_current_flag INT)
USING delta
PARTITIONED BY (record_current_flag, record_expiry_period)
LOCATION 'abfss://onelake@{storage_account}.dfs.core.windows.net/silver/salesforce/sf_opportunity_history'           
"""
spark.sql(query)

# COMMAND ----------

# MAGIC %md ###sf_product 

# COMMAND ----------

query = f"""
CREATE TABLE IF NOT EXISTS {catalog}.silver.sf_product (
  sid_id STRING NOT NULL,
  all_clauses STRING,
  amnesty_authorized_audience_required STRING,
  asset_id STRING,
  available_for_e_agreements STRING,
  chapters_to_go_available STRING,
  clause_header STRING,
  comm_type STRING,
  contract_product_family STRING,
  contract_product_name STRING,
  course_objects_available STRING,
  created_by_id STRING,
  created_date TIMESTAMP,
  credit_type STRING,
  currency_iso_code STRING,
  deployment_method_required STRING,
  family STRING,
  finance_prod_id STRING,
  finance_product_description STRING,
  is_active BOOLEAN,
  kit BOOLEAN,
  last_modified_by_id STRING,
  last_modified_date TIMESTAMP,
  localizations_available STRING,
  name STRING,
  number_of_revenue_installments INT,
  product_group STRING,
  product_line STRING,
  product_pillar STRING,
  product_type STRING,
  product_code STRING,
  reseller_bundle_product STRING,
  reseller_skillsoft_product STRING,
  revenue_installment_period STRING,
  revenue_schedule_type STRING,
  salesforce_id STRING,
  sb_id STRING,
  source_id STRING,
  st_cs_reporting_product STRING,
  system_modstamp TIMESTAMP,
  titles_required BOOLEAN,
  type STRING,
  usage STRING,
  year_1_product BOOLEAN,
  sbu STRING,
  product_classifications STRING,
  is_prof_service BOOLEAN,
  is_deleted BOOLEAN,
  ingested_file_name STRING,
  ingested_timestamp TIMESTAMP,
  hash_key STRING,
  record_expiry_period STRING,
  record_effective_date TIMESTAMP,
  record_expiry_date TIMESTAMP,
  record_current_flag INT)
USING delta
PARTITIONED BY (record_current_flag, record_expiry_period)
LOCATION 'abfss://onelake@{storage_account}.dfs.core.windows.net/silver/salesforce/sf_product'      
"""
spark.sql(query)

# COMMAND ----------

# MAGIC %md ###sf_territory

# COMMAND ----------

query = f"""
CREATE TABLE IF NOT EXISTS {catalog}.silver.sf_territory (
  sid_id STRING NOT NULL,
  account_access_level STRING,
  case_access_level STRING,
  contact_access_level STRING,
  currency_iso_code STRING,
  description STRING,
  developer_name STRING,
  forecast_user_id STRING,
  last_modified_by_id STRING,
  last_modified_date TIMESTAMP,
  name STRING,
  opportunity_access_level STRING,
  parent_territory2id STRING,
  system_modstamp TIMESTAMP,
  territory2model_id STRING,
  territory2type_id STRING,
  ingested_file_name STRING,
  ingested_timestamp TIMESTAMP,
  hash_key STRING,
  record_expiry_period STRING,
  record_effective_date TIMESTAMP,
  record_expiry_date TIMESTAMP,
  record_current_flag INT)
USING delta
PARTITIONED BY (record_current_flag, record_expiry_period)
LOCATION 'abfss://onelake@{storage_account}.dfs.core.windows.net/silver/salesforce/sf_territory'           
"""
spark.sql(query)

# COMMAND ----------

# MAGIC %md ###sf_user

# COMMAND ----------

query = f"""
CREATE TABLE IF NOT EXISTS {catalog}.silver.sf_user (
  sid_id STRING NOT NULL,
  about_me STRING,
  account_id STRING,
  alias STRING,
  area STRING,
  avp STRING,
  call_center_id STRING,
  city STRING,
  commission_role STRING,
  community_nickname STRING,
  company_name STRING,
  contact_id STRING,
  country STRING,
  created_by_id STRING,
  created_date TIMESTAMP,
  currency_iso_code STRING,
  cvp STRING,
  default_currency_iso_code STRING,
  delegated_approver_id STRING,
  department STRING,
  division STRING,
  effective_date TIMESTAMP,
  email STRING,
  email_encoding_key STRING,
  empl_id STRING,
  employee_number STRING,
  expiration_date TIMESTAMP,
  extension STRING,
  fax STRING,
  federation_identifier STRING,
  first_name STRING,
  forecast_enabled BOOLEAN,
  is_active BOOLEAN,
  language_locale_key STRING,
  last_login_date TIMESTAMP,
  last_modified_by_id STRING,
  last_modified_date TIMESTAMP,
  last_name STRING,
  last_password_change_date TIMESTAMP,
  lead_generator_id STRING,
  locale_sid_key STRING,
  manager_email STRING,
  manager_formula STRING,
  manager_id STRING,
  mobile_phone STRING,
  name STRING,
  offline_pda_trial_expiration_date TIMESTAMP,
  offline_trial_expiration_date TIMESTAMP,
  phone STRING,
  postal_code STRING,
  profile_id STRING,
  receives_admin_info_emails BOOLEAN,
  receives_info_emails BOOLEAN,
  rvp STRING,
  state STRING,
  street STRING,
  system_modstamp TIMESTAMP,
  time_zone_sid_key STRING,
  title STRING,
  username STRING,
  user_permissions_avantgo_user BOOLEAN,
  user_permissions_call_center_auto_login BOOLEAN,
  user_permissions_marketing_user BOOLEAN,
  user_permissions_offline_user BOOLEAN,
  user_permissions_sfcontent_user BOOLEAN,
  user_preferences_activity_reminders_popup BOOLEAN,
  user_preferences_apex_pages_developer_mode BOOLEAN,
  user_preferences_event_reminders_checkbox_default BOOLEAN,
  user_preferences_reminder_sound_off BOOLEAN,
  user_preferences_task_reminders_checkbox_default BOOLEAN,
  user_role_id STRING,
  user_type STRING,
  manager_level STRING,
  exclude_from_dw_reporting BOOLEAN,
  marketing_region STRING,
  dir STRING,
  evp STRING,
  badge_text STRING,
  digest_frequency STRING,
  sum_total_user_id STRING,
  user_18_id STRING,
  users_name_escaped STRING,
  ingested_file_name STRING,
  ingested_timestamp TIMESTAMP,
  hash_key STRING,
  record_expiry_period STRING,
  record_effective_date TIMESTAMP,
  record_expiry_date TIMESTAMP,
  record_current_flag INT)
USING delta
PARTITIONED BY (record_current_flag, record_expiry_period)
LOCATION 'abfss://onelake@{storage_account}.dfs.core.windows.net/silver/salesforce/sf_user'
"""
spark.sql(query)

# COMMAND ----------

# MAGIC %md ###sf_user_role

# COMMAND ----------

query = f"""
CREATE TABLE IF NOT EXISTS {catalog}.silver.sf_user_role (
  sid_id STRING NOT NULL,
  case_access_for_account_owner STRING,
  contact_access_for_account_owner STRING,
  forecast_user_id STRING,
  last_modified_by_id STRING,
  last_modified_date TIMESTAMP,
  may_forecast_manager_share BOOLEAN,
  name STRING,
  opportunity_access_for_account_owner STRING,
  parent_role_id STRING,
  portal_account_id STRING,
  portal_account_owner_id STRING,
  portal_type STRING,
  rollup_description STRING,
  system_modstamp TIMESTAMP,
  ingested_file_name STRING,
  ingested_timestamp TIMESTAMP,
  hash_key STRING,
  record_expiry_period STRING,
  record_effective_date TIMESTAMP,
  record_expiry_date TIMESTAMP,
  record_current_flag INT)
USING delta
PARTITIONED BY (record_current_flag, record_expiry_period)
LOCATION 'abfss://onelake@{storage_account}.dfs.core.windows.net/silver/salesforce/sf_user_role'          
"""
spark.sql(query)


# COMMAND ----------

# MAGIC %md ##Creating Gold Base Tables

# COMMAND ----------

# MAGIC %md ###sf_base_gtm_account

# COMMAND ----------

query = f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.sf_base_gtm_account (
  account_id STRING NOT NULL,
  account_customer_key STRING,
  account_duns_number STRING,
  account_name STRING,
  account_business_division STRING,
  account_territory_business_unit STRING,
  account_territory_level_1 STRING,
  account_territory_level_2 STRING,
  account_territory_level_3 STRING,
  account_territory_level_4 STRING,
  account_market_segment STRING,
  account_customer_persona STRING,
  account_customer_success_segment STRING,
  account_company_size STRING,
  account_number_of_employees INT,
  account_annual_revenue DECIMAL(18,0),
  account_industry STRING,
  account_industry_segment STRING,
  account_billing_country STRING,
  account_ownership STRING,
  account_created_date TIMESTAMP,
  account_shipping_country STRING,
  account_shipping_country_geo STRING,
  run_date DATE)
USING delta
LOCATION 'abfss://onelake@{storage_account}.dfs.core.windows.net/gold/base/sf_base_gtm_account/'
TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')
"""

spark.sql(query)

# COMMAND ----------

# MAGIC %md ###sf_base_gtm_product

# COMMAND ----------

query = f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.sf_base_gtm_product (
  forecast_product_id STRING NOT NULL,
  forecast_product_name STRING,
  forecast_gtm_category STRING,
  forecast_product_sbu STRING,
  forecast_product_pillar STRING,
  forecast_product_group STRING,
  forecast_product_classifications STRING,
  forecast_product_family STRING,
  run_date DATE)
USING delta
LOCATION 'abfss://onelake@{storage_account}.dfs.core.windows.net/gold/base/sf_base_gtm_product/'
TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')
"""

spark.sql(query)

# COMMAND ----------

# MAGIC %md ###sf_base_gtm_contact

# COMMAND ----------

query = f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.sf_base_gtm_contact (
  contact_id STRING NOT NULL,
  contact_primary_business_division STRING,
  contact_job_level STRING,
  contact_job_role STRING,
  contact_title STRING,
  contact_email_domain STRING,
  run_date DATE)
USING delta
LOCATION 'abfss://onelake@{storage_account}.dfs.core.windows.net/gold/base/sf_base_gtm_contact/'
TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')
"""
spark.sql(query)

# COMMAND ----------

# MAGIC %md ###sf_base_gtm_user

# COMMAND ----------

query = f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.sf_base_gtm_user (
  user_id STRING,
  user_svp STRING,
  user_first_name STRING,
  user_last_name STRING,
  user_full_name STRING,
  user_cvp STRING,
  user_avp STRING,
  user_rvp STRING,
  user_dir STRING,
  user_div STRING,
  svp_based_product_family STRING,
  avp_based_region STRING,
  run_date DATE)
USING delta
LOCATION 'abfss://onelake@{storage_account}.dfs.core.windows.net/gold/base/sf_base_gtm_user/'
TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')
"""

spark.sql(query)

# COMMAND ----------

# MAGIC %md
# MAGIC ### sf_base_gtm_opportunity

# COMMAND ----------

query = f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.sf_base_gtm_opportunity (
  opportunity_id STRING,
  opp_number STRING,
  opp_stage STRING,
  opp_name STRING,
  opp_record_type STRING,
  opp_sf_based_source STRING,
  opp_creation_date DATE,
  opp_account_id STRING,
  opp_owner_id STRING,
  opp_revenue_type STRING,
  opp_ilt_offering_type STRING,
  opp_primary_contact_id STRING,
  opp_accepted_date TIMESTAMP,
  opp_rejected_date TIMESTAMP,
  opp_appointment_date TIMESTAMP,
  opp_projected_close_date TIMESTAMP,
  opp_bant_job_role STRING,
  opp_bant_timeframe STRING,
  opp_bant_budget STRING,
  opp_bant_primary_solution_of_interest STRING,
  opp_management_adjustment STRING,
  opp_green_upside_ind BOOLEAN,
  opp_at_risk_ind BOOLEAN,
  opp_forecast_category_name STRING,
  opp_forecast_category STRING,
  opp_created_by_id STRING,
  opp_creator_role STRING,
  opp_last_activity_date TIMESTAMP,
  opp_win_loss_reason STRING,
  opp_close_plan STRING,
  run_date DATE)
USING delta
LOCATION 'abfss://onelake@{storage_account}.dfs.core.windows.net/gold/base/sf_base_gtm_opportunity/'
TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')
"""

spark.sql(query)

# COMMAND ----------

# MAGIC %md
# MAGIC ### sf_base_gtm_forecast

# COMMAND ----------

query = f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.sf_base_gtm_forecast (
  forecast_id STRING,
  term STRING,
  opportunity STRING,
  forecast_commission_type STRING,
  sales_user STRING,
  team_member_role STRING,
  product STRING,
  forecast_category_old STRING,
  forecast_category STRING,
  forecast_date TIMESTAMP,
  forecast_currency_code STRING,
  core_amount DECIMAL(19,2),
  core_amount_converted DECIMAL(27,2),
  shadow_amount DECIMAL(19,2),
  shadow_amount_converted DECIMAL(27,2),
  forecast_revenue_type STRING,
  run_date DATE)
USING delta
LOCATION 'abfss://onelake@{storage_account}.dfs.core.windows.net/gold/base/sf_base_gtm_forecast/'
TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')
"""

spark.sql(query)

# COMMAND ----------

# MAGIC %md ##Creating Gold Aggregate Tables

# COMMAND ----------

# MAGIC %md ###aggr_gtm_opportunity_update_sf

# COMMAND ----------

query = f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.aggr_gtm_opportunity_update_sf (
  opportunity_id STRING NOT NULL,
  ft_campaign_id STRING,
  ft_campaign_name STRING,
  ft_channel STRING,
  mt_campaign_id STRING,
  mt_campaign_name STRING,
  mt_channel STRING,
  lt_campaign_id STRING,
  lt_campaign_name STRING,
  lt_channel STRING)
USING delta
LOCATION 'abfss://onelake@{storage_account}.dfs.core.windows.net/gold/aggregate/aggr_gtm_opportunity_update_sf/'
TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')
"""

spark.sql(query)

# COMMAND ----------

# MAGIC %md ###aggr_gtm_opportunity_attribution_current

# COMMAND ----------

query = f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.aggr_gtm_opportunity_attribution_current (
  model_name STRING NOT NULL,
  model_type STRING,
  opportunity_id STRING,
  opp_name STRING,
  opp_record_type_name STRING,
  account_id STRING,
  person_id STRING,
  contact_id STRING,
  lead_id STRING,
  member_id STRING,
  opp_creation_date DATE,
  inquiry_status STRING,
  inquiry_date DATE,
  campaign_id STRING,
  campaign_name STRING,
  campaign_program_type STRING,
  campaign_lead_source STRING,
  opp_channel STRING,
  campaign_region STRING,
  campaign_segment STRING,
  campaign_vendor STRING,
  campaign_touch_type STRING,
  attribution_date DATE,
  event_system STRING,
  opp_revenue_type STRING,
  ad_source STRING,
  pre_opp_creation_ind BOOLEAN,
  attribution_eligible_ind BOOLEAN,
  run_date DATE)
USING delta
LOCATION 'abfss://onelake@{storage_account}.dfs.core.windows.net/gold/aggregate/aggr_gtm_opportunity_attribution_current/'
TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')
"""

spark.sql(query)

# COMMAND ----------

# MAGIC %md ###aggr_gtm_opportunity_forecast_current

# COMMAND ----------

query = f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.aggr_gtm_opportunity_forecast_current (
  opportunity_id STRING NOT NULL,
  opp_number STRING,
  opp_name STRING,
  opp_record_type STRING,
  opp_stage STRING,
  opp_avp_based_region STRING,
  opp_sf_based_source STRING,
  opp_svp_based_product_family STRING,
  opp_revenue_type STRING,
  opp_ilt_offering_type STRING,
  opp_creation_date DATE,
  opp_accepted_date TIMESTAMP,
  opp_rejected_date TIMESTAMP,
  opp_appointment_date TIMESTAMP,
  opp_projected_close_date TIMESTAMP,
  opp_last_activity_date TIMESTAMP,
  opp_bant_job_role STRING,
  opp_bant_timeframe STRING,
  opp_bant_budget STRING,
  opp_bant_primary_solution_of_interest STRING,
  opp_win_loss_reason STRING,
  opp_close_plan STRING,
  opp_owner_id STRING,
  opp_owner_name STRING,
  opp_owner_svp STRING,
  opp_owner_cvp STRING,
  opp_owner_avp STRING,
  opp_owner_rvp STRING,
  opp_owner_dir STRING,
  opp_owner_div STRING,
  opp_creator_id STRING,
  opp_creator_name STRING,
  opp_creator_svp STRING,
  opp_creator_cvp STRING,
  opp_creator_avp STRING,
  opp_creator_rvp STRING,
  opp_creator_dir STRING,
  opp_creator_div STRING,
  opp_creator_role STRING,
  opp_creator_avp_based_region STRING,
  opp_account_id STRING,
  opp_account_name STRING,
  opp_account_business_division STRING,
  opp_account_customer_persona STRING,
  opp_account_territory_business_unit STRING,
  opp_account_territory_level_1 STRING,
  opp_account_territory_level_2 STRING,
  opp_account_territory_level_3 STRING,
  opp_account_territory_level_4 STRING,
  opp_account_market_segment STRING,
  opp_account_number_of_employees INT,
  opp_account_annual_revenue DECIMAL(18,0),
  opp_account_industry STRING,
  opp_account_industry_segment STRING,
  opp_account_billing_country STRING,
  opp_account_customer_key STRING,
  opp_primary_contact_id STRING,
  opp_primary_contact_job_level STRING,
  opp_primary_contact_job_role STRING,
  opp_primary_contact_title STRING,
  opp_primary_contact_email_domain STRING,
  opp_forecast_category STRING,
  opp_management_adjustment STRING,
  forecast_id STRING,
  forecast_term STRING,
  forecast_avp_based_region STRING,
  forecast_revenue_type STRING,
  forecast_product_family STRING,
  forecast_date TIMESTAMP,
  forecast_commission_type STRING,
  forecast_category STRING,
  forecast_gtm_category STRING,
  forecast_product_sbu STRING,
  forecast_product_pillar STRING,
  forecast_product_group STRING,
  forecast_product_classifications STRING,
  forecast_product_id STRING,
  forecast_product_name STRING,
  forecast_service_type STRING,
  forecast_owner_id STRING,
  forecast_owner_name STRING,
  forecast_owner_svp STRING,
  forecast_owner_cvp STRING,
  forecast_owner_avp STRING,
  forecast_owner_rvp STRING,
  forecast_owner_dir STRING,
  forecast_owner_div STRING,
  forecast_currency_code STRING,
  core_amount DECIMAL(29,2),
  core_amount_converted DECIMAL(37,2),
  shadow_amount DECIMAL(29,2),
  shadow_amount_converted DECIMAL(37,2),
  forecast_landing_date DATE,
  forecast_landing_qtr_start_date DATE,
  opp_mql_ind INT,
  opp_sql_ind INT,
  opp_evaluation_ind INT,
  opp_proposal_ind INT,
  opp_negotiation_ind INT,
  opp_history_sql_date DATE,
  opp_history_date_evaluation_or_higher DATE,
  opp_history_date_proposal_or_higher DATE,
  opp_history_date_negotiation_or_higher DATE,
  opp_history_close_date DATE,
  opp_history_won_date DATE,
  opp_history_lost_date DATE,
  opp_sales_cycle_creation_to_last_close INT,
  opp_sales_cycle_creation_to_first_close INT,
  opp_sales_cycle_sql_to_last_close INT,
  opp_sales_cycle_sql_to_first_close INT,
  opp_sales_cycle_mql_to_last_close INT,
  opp_sales_cycle_mql_to_first_close INT,
  opp_sales_cycle_sql_to_first_win INT,
  opp_sales_cycle_sql_to_last_win INT,
  opp_ft_campaign_id_365_lookback_days STRING,
  opp_ft_campaign_name_365_lookback_days STRING,
  opp_ft_campaign_program_type_365_lookback_days STRING,
  opp_ft_campaign_lead_source_365_lookback_days STRING,
  opp_mt_campaign_id_365_lookback_days STRING,
  opp_mt_campaign_name_365_lookback_days STRING,
  opp_mt_campaign_program_type_365_lookback_days STRING,
  opp_mt_campaign_lead_source_365_lookback_days STRING,
  opp_source_based_on_mt_campaign_365_lookback_days STRING,
  opp_channel_based_on_mt_campaign_365_lookback_days STRING,
  opp_lt_campaign_id_365_lookback_days STRING,
  opp_lt_campaign_name_365_lookback_days STRING,
  opp_lt_campaign_program_type_365_lookback_days STRING,
  opp_lt_campaign_lead_source_365_lookback_days STRING,
  opp_ft_campaign_id_180_lookback_days STRING,
  opp_ft_campaign_name_180_lookback_days STRING,
  opp_ft_campaign_program_type_180_lookback_days STRING,
  opp_ft_campaign_lead_source_180_lookback_days STRING,
  opp_mt_campaign_id_180_lookback_days STRING,
  opp_mt_campaign_name_180_lookback_days STRING,
  opp_mt_campaign_program_type_180_lookback_days STRING,
  opp_mt_campaign_lead_source_180_lookback_days STRING,
  opp_source_based_on_mt_campaign_180_lookback_days STRING,
  opp_channel_based_on_mt_campaign_180_lookback_days STRING,
  opp_lt_campaign_id_180_lookback_days STRING,
  opp_lt_campaign_name_180_lookback_days STRING,
  opp_lt_campaign_program_type_180_lookback_days STRING,
  opp_lt_campaign_lead_source_180_lookback_days STRING,
  opp_ft_campaign_id_90_lookback_days STRING,
  opp_ft_campaign_name_90_lookback_days STRING,
  opp_ft_campaign_program_type_90_lookback_days STRING,
  opp_ft_campaign_lead_source_90_lookback_days STRING,
  opp_mt_campaign_id_90_lookback_days STRING,
  opp_mt_campaign_name_90_lookback_days STRING,
  opp_mt_campaign_program_type_90_lookback_days STRING,
  opp_mt_campaign_lead_source_90_lookback_days STRING,
  opp_source_based_on_mt_campaign_90_lookback_days STRING,
  opp_channel_based_on_mt_campaign_90_lookback_days STRING,
  opp_lt_campaign_id_90_lookback_days STRING,
  opp_lt_campaign_name_90_lookback_days STRING,
  opp_lt_campaign_program_type_90_lookback_days STRING,
  opp_lt_campaign_lead_source_90_lookback_days STRING,
  run_date DATE)
USING delta
LOCATION 'abfss://onelake@{storage_account}.dfs.core.windows.net/gold/aggregate/aggr_gtm_opportunity_forecast_current/'
TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')
"""

spark.sql(query)

# COMMAND ----------

# MAGIC %md ###aggr_gtm_opportunity_current

# COMMAND ----------

query = f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.aggr_gtm_opportunity_current (
  opportunity_id STRING NOT NULL,
  opp_number STRING,
  opp_name STRING,
  opp_stage STRING,
  opp_record_type STRING,
  opp_avp_based_region STRING,
  opp_sf_based_source STRING,
  opp_svp_based_product_family STRING,
  opp_revenue_type STRING,
  opp_ilt_offering_type STRING,
  opp_creation_date DATE,
  opp_accepted_date TIMESTAMP,
  opp_rejected_date TIMESTAMP,
  opp_appointment_date TIMESTAMP,
  opp_projected_close_date TIMESTAMP,
  opp_last_activity_date TIMESTAMP,
  opp_bant_job_role STRING,
  opp_bant_timeframe STRING,
  opp_bant_budget STRING,
  opp_bant_primary_solution_of_interest STRING,
  opp_win_loss_reason STRING,
  opp_close_plan STRING,
  opp_owner_id STRING,
  opp_owner_name STRING,
  opp_owner_svp STRING,
  opp_owner_cvp STRING,
  opp_owner_avp STRING,
  opp_owner_rvp STRING,
  opp_owner_dir STRING,
  opp_owner_div STRING,
  opp_creator_id STRING,
  opp_creator_name STRING,
  opp_creator_svp STRING,
  opp_creator_cvp STRING,
  opp_creator_avp STRING,
  opp_creator_rvp STRING,
  opp_creator_dir STRING,
  opp_creator_div STRING,
  opp_creator_role STRING,
  opp_account_id STRING,
  opp_account_name STRING,
  opp_account_business_division STRING,
  opp_account_customer_persona STRING,
  opp_account_territory_business_unit STRING,
  opp_account_territory_level_1 STRING,
  opp_account_territory_level_2 STRING,
  opp_account_territory_level_3 STRING,
  opp_account_territory_level_4 STRING,
  opp_account_market_segment STRING,
  opp_account_number_of_employees INT,
  opp_account_annual_revenue DECIMAL(18,0),
  opp_account_industry STRING,
  opp_account_industry_segment STRING,
  opp_account_billing_country STRING,
  opp_account_customer_key STRING,
  opp_primary_contact_id STRING,
  opp_primary_contact_job_level STRING,
  opp_primary_contact_job_role STRING,
  opp_primary_contact_title STRING,
  opp_primary_contact_email_domain STRING,
  opp_forecast_category STRING,
  opp_management_adjustment STRING,
  forecast_landing_date DATE,
  forecast_landing_qtr_start_date DATE,
  opp_mql_ind INT,
  opp_sql_ind INT,
  opp_evaluation_ind INT,
  opp_proposal_ind INT,
  opp_negotiation_ind INT,
  opp_history_sql_date DATE,
  opp_history_date_evaluation_or_higher DATE,
  opp_history_date_proposal_or_higher DATE,
  opp_history_date_negotiation_or_higher DATE,
  opp_history_close_date DATE,
  opp_history_won_date DATE,
  opp_history_lost_date DATE,
  opp_sales_cycle_creation_to_last_close INT,
  opp_sales_cycle_creation_to_first_close INT,
  opp_sales_cycle_sql_to_last_close INT,
  opp_sales_cycle_sql_to_first_close INT,
  opp_sales_cycle_mql_to_last_close INT,
  opp_sales_cycle_mql_to_first_close INT,
  opp_sales_cycle_sql_to_first_win INT,
  opp_sales_cycle_sql_to_last_win INT,
  opp_ft_campaign_id_365_lookback_days STRING,
  opp_ft_campaign_name_365_lookback_days STRING,
  opp_ft_campaign_program_type_365_lookback_days STRING,
  opp_ft_campaign_lead_source_365_lookback_days STRING,
  opp_mt_campaign_id_365_lookback_days STRING,
  opp_mt_campaign_name_365_lookback_days STRING,
  opp_mt_campaign_program_type_365_lookback_days STRING,
  opp_mt_campaign_lead_source_365_lookback_days STRING,
  opp_source_based_on_mt_campaign_365_lookback_days STRING,
  opp_channel_based_on_mt_campaign_365_lookback_days STRING,
  opp_lt_campaign_id_365_lookback_days STRING,
  opp_lt_campaign_name_365_lookback_days STRING,
  opp_lt_campaign_program_type_365_lookback_days STRING,
  opp_lt_campaign_lead_source_365_lookback_days STRING,
  opp_ft_campaign_id_180_lookback_days STRING,
  opp_ft_campaign_name_180_lookback_days STRING,
  opp_ft_campaign_program_type_180_lookback_days STRING,
  opp_ft_campaign_lead_source_180_lookback_days STRING,
  opp_mt_campaign_id_180_lookback_days STRING,
  opp_mt_campaign_name_180_lookback_days STRING,
  opp_mt_campaign_program_type_180_lookback_days STRING,
  opp_mt_campaign_lead_source_180_lookback_days STRING,
  opp_source_based_on_mt_campaign_180_lookback_days STRING,
  opp_channel_based_on_mt_campaign_180_lookback_days STRING,
  opp_lt_campaign_id_180_lookback_days STRING,
  opp_lt_campaign_name_180_lookback_days STRING,
  opp_lt_campaign_program_type_180_lookback_days STRING,
  opp_lt_campaign_lead_source_180_lookback_days STRING,
  opp_ft_campaign_id_90_lookback_days STRING,
  opp_ft_campaign_name_90_lookback_days STRING,
  opp_ft_campaign_program_type_90_lookback_days STRING,
  opp_ft_campaign_lead_source_90_lookback_days STRING,
  opp_mt_campaign_id_90_lookback_days STRING,
  opp_mt_campaign_name_90_lookback_days STRING,
  opp_mt_campaign_program_type_90_lookback_days STRING,
  opp_mt_campaign_lead_source_90_lookback_days STRING,
  opp_source_based_on_mt_campaign_90_lookback_days STRING,
  opp_channel_based_on_mt_campaign_90_lookback_days STRING,
  opp_lt_campaign_id_90_lookback_days STRING,
  opp_lt_campaign_name_90_lookback_days STRING,
  opp_lt_campaign_program_type_90_lookback_days STRING,
  opp_lt_campaign_lead_source_90_lookback_days STRING,
  opp_nnb_indicator STRING,
  forecast_term STRING,
  forecast_date TIMESTAMP,
  forecast_date_sks_nnb_won TIMESTAMP,
  opp_sks_nnb_dollars DECIMAL(38,2),
  opp_sks_nnb_dollars_won DECIMAL(38,2),
  forecast_win_sks_nnb_ind INT,
  first_pipeline_date DATE,
  first_pipeline_forecast_date DATE,
  opp_stage_first_pipeline_entered STRING,
  first_pipeline_business_lead_core_amount_converted DOUBLE,
  first_pipeline_compliance_core_amount_converted DOUBLE,
  first_pipeline_tech_dev_core_amount_converted DOUBLE,
  first_pipeline_a_la_carte_core_amount_converted DOUBLE,
  first_pipeline_complete_coll_core_amount_converted DOUBLE,
  first_pipeline_prod_collab_core_amount_converted DOUBLE,
  first_pipeline_other_core_amount_converted DOUBLE,
  first_pipeline_coaching_core_amount_converted DOUBLE,
  first_pipeline_ilt_core_amount_converted DOUBLE,
  first_pipeline_code_core_amount_converted DOUBLE,
  first_pipeline_total_core_amount_converted DOUBLE,
  first_pipeline_bus_unit_content_core_amount_converted DOUBLE,
  first_pipeline_bus_unit_coaching_core_amount_converted DOUBLE,
  first_pipeline_bus_unit_ilt_core_amount_converted DOUBLE,
  first_pipeline_bus_unit_code_core_amount_converted DOUBLE,
  first_pipeline_gtm_bl_core_amount_converted DOUBLE,
  first_pipeline_gtm_bl_core_amount_converted_excl_coaching DOUBLE,
  first_pipeline_gtm_td_core_amount_converted DOUBLE,
  first_pipeline_gtm_td_core_amount_converted_excl_ilt DOUBLE,
  first_pipeline_gtm_td_core_amount_converted_excl_code DOUBLE,
  first_pipeline_gtm_td_core_amount_converted_excl_ilt_code DOUBLE,
  first_pipeline_gtm_complaince_core_amount_converted DOUBLE,
  forecast_business_lead_core_amount_converted DECIMAL(38,2),
  forecast_compliance_core_amount_converted DECIMAL(38,2),
  forecast_tech_dev_core_amount_converted DECIMAL(38,2),
  forecast_a_la_carte_core_amount_converted DECIMAL(38,2),
  forecast_complete_coll_core_amount_converted DECIMAL(38,2),
  forecast_prod_collab_core_amount_converted DECIMAL(38,2),
  forecast_other_core_amount_converted DECIMAL(38,2),
  forecast_coaching_core_amount_converted DECIMAL(38,2),
  forecast_ilt_core_amount_converted DECIMAL(38,2),
  forecast_code_core_amount_converted DECIMAL(38,2),
  forecast_total_core_amount_converted DECIMAL(38,2),
  forecast_bus_unit_content_core_amount_converted DECIMAL(38,2),
  forecast_bus_unit_coaching_core_amount_converted DECIMAL(38,2),
  forecast_bus_unit_ilt_core_amount_converted DECIMAL(38,2),
  forecast_bus_unit_code_core_amount_converted DECIMAL(38,2),
  forecast_gtm_bl_core_amount_converted DOUBLE,
  forecast_gtm_bl_core_amount_converted_excl_coaching DOUBLE,
  forecast_gtm_td_core_amount_converted DOUBLE,
  forecast_gtm_td_core_amount_converted_excl_ilt DOUBLE,
  forecast_gtm_td_core_amount_converted_excl_code DOUBLE,
  forecast_gtm_td_core_amount_converted_excl_ilt_code DOUBLE,
  forecast_gtm_complaince_core_amount_converted DECIMAL(38,2),
  ilt_first_shadow_pipeline_date TIMESTAMP,
  ilt_first_shadow_pipeline_forecast_date TIMESTAMP,
  ilt_first_shadow_pipeline_amount_converted DOUBLE,
  ilt_forecast_shadow_amount_converted DECIMAL(38,2),
  run_date DATE)
USING delta
LOCATION 'abfss://onelake@{storage_account}.dfs.core.windows.net/gold/aggregate/aggr_gtm_opportunity_current/'
TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')
"""

spark.sql(query)

# COMMAND ----------

# MAGIC %md ###aggr_gtm_prospect_current

# COMMAND ----------

query = f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.aggr_gtm_prospect_current (
  lead_id STRING NOT NULL,
  company STRING,
  title STRING,
  annual_revenue DECIMAL(18,0),
  company_size STRING,
  department STRING,
  job_level STRING,
  job_role STRING,
  number_of_employees INT,
  website STRING,
  email_domain_populated_ind INT,
  status STRING,
  lead_status_change_date TIMESTAMP,
  last_activity_date TIMESTAMP,
  last_modified_date TIMESTAMP,
  account_industry_lead STRING,
  asset_downloaded STRING,
  lead_source STRING,
  lead_source_most_recent STRING,
  program_type_most_recent STRING,
  source_id STRING,
  business_issues_described STRING,
  do_not_call BOOLEAN,
  has_opted_out_of_email BOOLEAN,
  is_converted BOOLEAN,
  behavior_score DECIMAL(5,2),
  demographic_score DECIMAL(5,2),
  aql_date DATE,
  reason_disqualified STRING,
  other_reason_disqualified STRING,
  matched_account_siccode STRING,
  matched_account_revenue DECIMAL(18,2),
  matched_account_size STRING,
  matched_market_segment STRING,
  created_by_id STRING,
  converted_account_id STRING,
  converted_contact_id STRING,
  converted_date TIMESTAMP,
  lead_owner_id STRING,
  owner_id STRING,
  lean_data__reporting_matched_account STRING,
  cm_matched_account_id STRING,
  min_first_responded_date TIMESTAMP,
  initial_campaign_id STRING,
  lead_created_date TIMESTAMP,
  first_eval_date DATE,
  initial_opportunity_id STRING,
  account_id STRING,
  first_mql_date DATE,
  lead_email_domain STRING,
  lead_email_domain_personal INT,
  lead_email_domain_internal INT,
  ilt_lead INT,
  mdr_sdr_queue INT,
  is_demo_contact_us_lead INT,
  is_ecommerce_paid_lead INT,
  date_begin TIMESTAMP,
  run_date DATE)
USING delta
LOCATION 'abfss://onelake@{storage_account}.dfs.core.windows.net/gold/aggregate/aggr_gtm_prospect_current/'
TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')
"""

spark.sql(query)

# COMMAND ----------

# MAGIC %md ###aggr_gtm_prospect_snap

# COMMAND ----------

query = f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.aggr_gtm_prospect_snap (
  lead_id STRING NOT NULL,
  company STRING,
  title STRING,
  annual_revenue DECIMAL(18,0),
  company_size STRING,
  department STRING,
  job_level STRING,
  job_role STRING,
  number_of_employees INT,
  website STRING,
  email_domain_populated_ind INT,
  status STRING,
  lead_status_change_date TIMESTAMP,
  last_activity_date TIMESTAMP,
  last_modified_date TIMESTAMP,
  account_industry_lead STRING,
  asset_downloaded STRING,
  lead_source STRING,
  lead_source_most_recent STRING,
  program_type_most_recent STRING,
  source_id STRING,
  business_issues_described STRING,
  do_not_call BOOLEAN,
  has_opted_out_of_email BOOLEAN,
  is_converted BOOLEAN,
  behavior_score DECIMAL(5,2),
  demographic_score DECIMAL(5,2),
  aql_date DATE,
  reason_disqualified STRING,
  other_reason_disqualified STRING,
  matched_account_siccode STRING,
  matched_account_revenue DECIMAL(18,2),
  matched_account_size STRING,
  matched_market_segment STRING,
  created_by_id STRING,
  converted_account_id STRING,
  converted_contact_id STRING,
  converted_date TIMESTAMP,
  lead_owner_id STRING,
  owner_id STRING,
  lean_data__reporting_matched_account STRING,
  cm_matched_account_id STRING,
  min_first_responded_date TIMESTAMP,
  initial_campaign_id STRING,
  lead_created_date TIMESTAMP,
  first_eval_date DATE,
  initial_opportunity_id STRING,
  account_id STRING,
  first_mql_date DATE,
  lead_email_domain STRING,
  lead_email_domain_personal INT,
  lead_email_domain_internal INT,
  ilt_lead INT,
  mdr_sdr_queue INT,
  is_demo_contact_us_lead INT,
  is_ecommerce_paid_lead INT,
  date_begin TIMESTAMP,
  lead_age INT,
  days_to_conversion INT,
  converted_within_30_days STRING,
  converted_within_60_days STRING,
  converted_within_90_days STRING,
  converted_within_180_days STRING,
  is_lead_converted_previously INT,
  run_date DATE,
  snapshot_date DATE,
  snapshot_period STRING)
USING delta
PARTITIONED BY (snapshot_period)
LOCATION 'abfss://onelake@{storage_account}.dfs.core.windows.net/gold/aggregate/aggr_gtm_prospect_snap/'
TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')
"""
spark.sql(query)

# COMMAND ----------

# MAGIC %md ###aggr_gtm_lead_behavior_current

# COMMAND ----------

query = f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.aggr_gtm_lead_behavior_current (
  lead_id STRING NOT NULL,
  inbound_campaign_filterer INT,
  recent_campaign_type_webinar_ind INT,
  recent_campaign_type_virtual_event_ind INT,
  recent_campaign_type_website_ind INT,
  recent_campaign_type_chat_ind INT,
  recent_campaign_type_events_ind INT,
  recent_campaign_type_list_load_ind INT,
  recent_campaign_type_paid_media_ind INT,
  recent_campaign_type_marketing_nurture_ind INT,
  recent_campaign_type_sales_ind INT,
  recent_campaign_type_other_ind INT,
  recent_campaign_type_blank_ind INT,
  recent_campaign_segment_business_and_lead_ind INT,
  recent_campaign_segment_compliance_ind INT,
  recent_campaign_segment_tech_and_dev_ind INT,
  recent_campaign_segment_skl_ind INT,
  recent_campaign_segment_gk_ind INT,
  recent_campaign_segment_other_ind INT,
  recent_campaign_segment_blank_ind INT,
  responded_date_begin DATE,
  responded_date_end DATE,
  first_campaign_date DATE,
  first_campaign_type_webinar_ind INT,
  first_campaign_type_virtual_event_ind INT,
  first_campaign_type_website_ind INT,
  first_campaign_type_chat_ind INT,
  first_campaign_type_events_ind INT,
  first_campaign_type_list_load_ind INT,
  first_campaign_type_paid_media_ind INT,
  first_campaign_type_marketing_nurture_ind INT,
  first_campaign_type_sales_ind INT,
  first_campaign_type_other_ind INT,
  first_campaign_type_blank_ind INT,
  first_campaign_segment_business_and_lead_ind INT,
  first_campaign_segment_compliance_ind INT,
  first_campaign_segment_tech_and_dev_ind INT,
  first_campaign_segment_skl_ind INT,
  first_campaign_segment_gk_ind INT,
  first_campaign_segment_other_ind INT,
  first_campaign_segment_blank_ind INT,
  run_date DATE)
USING delta
LOCATION 'abfss://onelake@{storage_account}.dfs.core.windows.net/gold/aggregate/aggr_gtm_lead_behavior_current/'
TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')
"""

spark.sql(query)

# COMMAND ----------

# MAGIC %md ###aggr_gtm_lead_behavior_snap

# COMMAND ----------

query = f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.aggr_gtm_lead_behavior_snap (
  lead_id STRING NOT NULL,
  inbound_campaign_filterer INT,
  recent_campaign_type_webinar_ind INT,
  recent_campaign_type_virtual_event_ind INT,
  recent_campaign_type_website_ind INT,
  recent_campaign_type_chat_ind INT,
  recent_campaign_type_events_ind INT,
  recent_campaign_type_list_load_ind INT,
  recent_campaign_type_paid_media_ind INT,
  recent_campaign_type_marketing_nurture_ind INT,
  recent_campaign_type_sales_ind INT,
  recent_campaign_type_other_ind INT,
  recent_campaign_type_blank_ind INT,
  recent_campaign_segment_business_and_lead_ind INT,
  recent_campaign_segment_compliance_ind INT,
  recent_campaign_segment_tech_and_dev_ind INT,
  recent_campaign_segment_skl_ind INT,
  recent_campaign_segment_gk_ind INT,
  recent_campaign_segment_other_ind INT,
  recent_campaign_segment_blank_ind INT,
  responded_date_begin DATE,
  responded_date_end DATE,
  first_campaign_date DATE,
  first_campaign_type_webinar_ind INT,
  first_campaign_type_virtual_event_ind INT,
  first_campaign_type_website_ind INT,
  first_campaign_type_chat_ind INT,
  first_campaign_type_events_ind INT,
  first_campaign_type_list_load_ind INT,
  first_campaign_type_paid_media_ind INT,
  first_campaign_type_marketing_nurture_ind INT,
  first_campaign_type_sales_ind INT,
  first_campaign_type_other_ind INT,
  first_campaign_type_blank_ind INT,
  first_campaign_segment_business_and_lead_ind INT,
  first_campaign_segment_compliance_ind INT,
  first_campaign_segment_tech_and_dev_ind INT,
  first_campaign_segment_skl_ind INT,
  first_campaign_segment_gk_ind INT,
  first_campaign_segment_other_ind INT,
  first_campaign_segment_blank_ind INT,
  days_since_last_campaign_response INT,
  days_since_first_campaign_response INT,
  run_date DATE,
  snapshot_date DATE,
  snapshot_period STRING)
USING delta
PARTITIONED BY (snapshot_period)
LOCATION 'abfss://onelake@{storage_account}.dfs.core.windows.net/gold/aggregate/aggr_gtm_lead_behavior_snap/'
TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')
"""

spark.sql(query)

# COMMAND ----------

# MAGIC %md ###aggr_gtm_lead_demographic_current

# COMMAND ----------

query = f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.aggr_gtm_lead_demographic_current (
  lead_id STRING NOT NULL,
  lead_status STRING,
  company_size STRING,
  is_website_domain_type_dot_com_ind INT,
  is_website_domain_type_blank_ind INT,
  is_website_domain_type_non_us_ind INT,
  is_website_domain_type_org_ind INT,
  is_website_domain_type_edu_ind INT,
  is_website_domain_type_gov_ind INT,
  is_number_of_employees_populated_ind INT,
  is_asset_downloaded_ind INT,
  is_department_populated_ind INT,
  is_department_hr_ind INT,
  is_department_it_ind INT,
  is_department_sales_marketing_ind INT,
  is_department_legal_compliance_ind INT,
  is_job_level_populated_ind INT,
  is_job_level_c_level_ind INT,
  is_job_level_vp_ind INT,
  is_job_level_director_ind INT,
  is_job_level_manager_ind INT,
  is_job_level_non_manager_ind INT,
  is_job_level_manager_and_above_ind INT,
  is_job_level_director_and_above_ind INT,
  is_job_level_vp_and_above_ind INT,
  is_business_issue_described_ind INT,
  is_business_issue_described_business_and_lead_ind INT,
  is_business_issue_described_compliance_ind INT,
  is_business_issue_described_tech_dev_ind INT,
  is_region_blank_ind INT,
  is_region_na_ind INT,
  is_region_apac_ind INT,
  is_region_emea_ind INT,
  is_region_india_ind INT,
  has_opted_out_of_email INT,
  behavior_score INT,
  demographic_score INT,
  marketing_score INT,
  aql_date DATE,
  is_aql_ind INT,
  lead_last_activity_date DATE,
  is_converted_contact INT,
  date_begin DATE,
  date_end DATE,
  run_date DATE)
USING delta
LOCATION 'abfss://onelake@{storage_account}.dfs.core.windows.net/gold/aggregate/aggr_gtm_lead_demographic_current/'
TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')
"""

spark.sql(query)

# COMMAND ----------

# MAGIC %md ###aggr_gtm_lead_demographic_snap

# COMMAND ----------

query = f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.aggr_gtm_lead_demographic_snap (
  lead_id STRING NOT NULL,
  lead_status STRING,
  company_size STRING,
  is_website_domain_type_dot_com_ind INT,
  is_website_domain_type_blank_ind INT,
  is_website_domain_type_non_us_ind INT,
  is_website_domain_type_org_ind INT,
  is_website_domain_type_edu_ind INT,
  is_website_domain_type_gov_ind INT,
  is_number_of_employees_populated_ind INT,
  is_asset_downloaded_ind INT,
  is_department_populated_ind INT,
  is_department_hr_ind INT,
  is_department_it_ind INT,
  is_department_sales_marketing_ind INT,
  is_department_legal_compliance_ind INT,
  is_job_level_populated_ind INT,
  is_job_level_c_level_ind INT,
  is_job_level_vp_ind INT,
  is_job_level_director_ind INT,
  is_job_level_manager_ind INT,
  is_job_level_non_manager_ind INT,
  is_job_level_manager_and_above_ind INT,
  is_job_level_director_and_above_ind INT,
  is_job_level_vp_and_above_ind INT,
  is_business_issue_described_ind INT,
  is_business_issue_described_business_and_lead_ind INT,
  is_business_issue_described_compliance_ind INT,
  is_business_issue_described_tech_dev_ind INT,
  is_region_blank_ind INT,
  is_region_na_ind INT,
  is_region_apac_ind INT,
  is_region_emea_ind INT,
  is_region_india_ind INT,
  has_opted_out_of_email INT,
  behavior_score INT,
  demographic_score INT,
  marketing_score INT,
  aql_date DATE,
  is_aql_ind INT,
  lead_last_activity_date DATE,
  is_converted_contact INT,
  date_begin DATE,
  date_end DATE,
  days_since_last_activity INT,
  days_left_in_month INT,
  days_left_in_quarter_cy INT,
  days_left_in_year_cy INT,
  days_left_in_quarter_fy INT,
  days_left_in_year_fy INT,
  run_date DATE,
  snapshot_date DATE,
  snapshot_period STRING)
USING delta
PARTITIONED BY (snapshot_period)
LOCATION 'abfss://onelake@{storage_account}.dfs.core.windows.net/gold/aggregate/aggr_gtm_lead_demographic_snap/'
TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')
"""

spark.sql(query)

# COMMAND ----------

# MAGIC %md ###aggr_gtm_lead_snap

# COMMAND ----------

query = f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.aggr_gtm_lead_snap (
  lead_id STRING NOT NULL,
  lead_created_date TIMESTAMP,
  first_mql_date DATE,
  first_eval_date DATE,
  lead_age INT,
  lead_email_domain STRING,
  lead_email_domain_personal INT,
  lead_email_domain_internal INT,
  ILT_lead INT,
  MDR_SDR_queue INT,
  is_demo_contact_us_lead INT,
  is_ecommerce_paid_lead INT,
  is_converted BOOLEAN,
  lead_status STRING,
  company_size STRING,
  is_website_domain_type_dot_com_ind INT,
  is_website_domain_type_blank_ind INT,
  is_website_domain_type_non_us_ind INT,
  is_website_domain_type_org_ind INT,
  is_website_domain_type_edu_ind INT,
  is_website_domain_type_gov_ind INT,
  is_number_of_employees_populated_ind INT,
  is_asset_downloaded_ind INT,
  is_department_populated_ind INT,
  is_department_hr_ind INT,
  is_department_it_ind INT,
  is_department_sales_marketing_ind INT,
  is_department_legal_compliance_ind INT,
  is_job_level_populated_ind INT,
  is_job_level_c_level_ind INT,
  is_job_level_vp_ind INT,
  is_job_level_director_ind INT,
  is_job_level_manager_ind INT,
  is_job_level_non_manager_ind INT,
  is_job_level_manager_and_above_ind INT,
  is_job_level_director_and_above_ind INT,
  is_job_level_vp_and_above_ind INT,
  is_business_issue_described_ind INT,
  is_business_issue_described_business_and_lead_ind INT,
  is_business_issue_described_compliance_ind INT,
  is_business_issue_described_tech_dev_ind INT,
  is_region_blank_ind INT,
  is_region_na_ind INT,
  is_region_apac_ind INT,
  is_region_emea_ind INT,
  is_region_india_ind INT,
  has_opted_out_of_email INT,
  behavior_score INT,
  demographic_score INT,
  marketing_score INT,
  aql_date DATE,
  is_aql_ind INT,
  lead_last_activity_date DATE,
  days_since_last_activity INT,
  days_left_in_month INT,
  days_left_in_quarter_cy INT,
  days_left_in_year_cy INT,
  days_left_in_quarter_fy INT,
  days_left_in_year_fy INT,
  is_converted_contact INT,
  date_begin DATE,
  date_end DATE,
  inbound_campaign_filterer INT,
  recent_campaign_type_webinar_ind INT,
  recent_campaign_type_virtual_event_ind INT,
  recent_campaign_type_website_ind INT,
  recent_campaign_type_chat_ind INT,
  recent_campaign_type_events_ind INT,
  recent_campaign_type_list_load_ind INT,
  recent_campaign_type_paid_media_ind INT,
  recent_campaign_type_marketing_nurture_ind INT,
  recent_campaign_type_sales_ind INT,
  recent_campaign_type_other_ind INT,
  recent_campaign_type_blank_ind INT,
  recent_campaign_segment_business_and_lead_ind INT,
  recent_campaign_segment_compliance_ind INT,
  recent_campaign_segment_tech_and_dev_ind INT,
  recent_campaign_segment_skl_ind INT,
  recent_campaign_segment_gk_ind INT,
  recent_campaign_segment_other_ind INT,
  recent_campaign_segment_blank_ind INT,
  responded_date_begin DATE,
  responded_date_end DATE,
  first_campaign_date DATE,
  first_campaign_type_webinar_ind INT,
  first_campaign_type_virtual_event_ind INT,
  first_campaign_type_website_ind INT,
  first_campaign_type_chat_ind INT,
  first_campaign_type_events_ind INT,
  first_campaign_type_list_load_ind INT,
  first_campaign_type_paid_media_ind INT,
  first_campaign_type_marketing_nurture_ind INT,
  first_campaign_type_sales_ind INT,
  first_campaign_type_other_ind INT,
  first_campaign_type_blank_ind INT,
  first_campaign_segment_business_and_lead_ind INT,
  first_campaign_segment_compliance_ind INT,
  first_campaign_segment_tech_and_dev_ind INT,
  first_campaign_segment_skl_ind INT,
  first_campaign_segment_gk_ind INT,
  first_campaign_segment_other_ind INT,
  first_campaign_segment_blank_ind INT,
  days_since_first_campaign_response INT,
  days_since_last_campaign_response INT,
  days_to_conversion INT,
  converted_within_30_days STRING,
  converted_within_60_days STRING,
  converted_within_90_days STRING,
  converted_within_180_days STRING,
  is_lead_converted_previously INT,
  snapshot_date DATE,
  snapshot_period STRING)
USING delta
PARTITIONED BY (snapshot_period)
LOCATION 'abfss://onelake@{storage_account}.dfs.core.windows.net/gold/aggregate/aggr_gtm_lead_snap/'
TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')
"""

spark.sql(query)

# COMMAND ----------

# MAGIC %md ###aggr_gtm_lead_score_daily_snap

# COMMAND ----------

query = f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.aggr_gtm_lead_score_daily_snap(
  lead_id STRING NOT NULL,
  predict_date DATE,
  model_predicted_score STRING,
  behavior_model_conversion__c STRING,
  load_date DATE
)
USING delta
LOCATION 'abfss://onelake@{storage_account}.dfs.core.windows.net/gold/aggregate/aggr_gtm_lead_score_daily_snap/'
TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')
"""

spark.sql(query)
