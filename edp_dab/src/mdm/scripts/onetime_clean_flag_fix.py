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

# MAGIC %sql
# MAGIC UPDATE ${catalog}.clean.melissa_input
# MAGIC SET final_clean_flag = 1
# MAGIC WHERE sid_id IN (SELECT sid_id FROM ${catalog}.clean.sf_account_clean_address WHERE final_clean_flag = 1);
