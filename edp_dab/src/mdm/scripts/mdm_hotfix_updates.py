# Databricks notebook source
# MAGIC %sql
# MAGIC MERGE INTO mdm_prod.clean.melissa_input AS b
# MAGIC USING mdm_prod.clean.sf_account_clean_address AS a
# MAGIC   ON b.sid_id = a.sid_id
# MAGIC WHEN MATCHED AND b.hash_key != a.hash_key THEN
# MAGIC   UPDATE SET b.hash_key = a.hash_key

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO mdm_prod.clean.melissa_output_billing_invalid_results AS b
# MAGIC USING mdm_prod.clean.melissa_input AS a
# MAGIC   ON b.sid_id = a.sid_id
# MAGIC WHEN MATCHED AND b.hash_key != a.hash_key THEN
# MAGIC   UPDATE SET b.hash_key = a.hash_key

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO mdm_prod.clean.melissa_output_billing_raw AS b
# MAGIC USING mdm_prod.clean.melissa_input AS a
# MAGIC   ON b.sid_id = a.sid_id
# MAGIC WHEN MATCHED AND b.hash_key != a.hash_key THEN
# MAGIC   UPDATE SET b.hash_key = a.hash_key

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO mdm_prod.clean.melissa_output_billing_all AS b
# MAGIC USING mdm_prod.clean.melissa_input AS a
# MAGIC   ON b.sid_id = a.sid_id
# MAGIC WHEN MATCHED AND b.hash_key != a.hash_key THEN
# MAGIC   UPDATE SET b.hash_key = a.hash_key

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO mdm_prod.clean.melissa_output_billing_final AS b
# MAGIC USING mdm_prod.clean.melissa_input AS a
# MAGIC   ON b.sid_id = a.sid_id
# MAGIC WHEN MATCHED AND b.hash_key != a.hash_key THEN
# MAGIC   UPDATE SET b.hash_key = a.hash_key

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO mdm_prod.clean.melissa_output_shipping_raw AS b
# MAGIC USING mdm_prod.clean.melissa_input AS a
# MAGIC   ON b.sid_id = a.sid_id
# MAGIC WHEN MATCHED AND b.hash_key != a.hash_key THEN
# MAGIC   UPDATE SET b.hash_key = a.hash_key

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO mdm_prod.clean.melissa_output_shipping_all AS b
# MAGIC USING mdm_prod.clean.melissa_input AS a
# MAGIC   ON b.sid_id = a.sid_id
# MAGIC WHEN MATCHED AND b.hash_key != a.hash_key THEN
# MAGIC   UPDATE SET b.hash_key = a.hash_key

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO mdm_prod.clean.melissa_output_shipping_final AS b
# MAGIC USING mdm_prod.clean.melissa_input AS a
# MAGIC   ON b.sid_id = a.sid_id
# MAGIC WHEN MATCHED AND b.hash_key != a.hash_key THEN
# MAGIC   UPDATE SET b.hash_key = a.hash_key

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO mdm_prod.clean.melissa_api_final AS b
# MAGIC USING mdm_prod.clean.melissa_input AS a
# MAGIC   ON b.sid_id = a.sid_id
# MAGIC WHEN MATCHED AND b.hash_key != a.hash_key THEN
# MAGIC   UPDATE SET b.hash_key = a.hash_key

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO mdm_prod.tmp.sf_account_melissa_review AS b
# MAGIC USING mdm_prod.clean.melissa_input AS a
# MAGIC   ON b.sid_id = a.sid_id
# MAGIC WHEN MATCHED AND b.hash_key != a.hash_key THEN
# MAGIC   UPDATE SET b.hash_key = a.hash_key
