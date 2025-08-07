# Databricks notebook source
# MAGIC %md
# MAGIC ## Set up widgets

# COMMAND ----------

dbutils.widgets.text("source_catalog", "", "Catalog")
source_catalog = dbutils.widgets.get("source_catalog")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import Libraries

# COMMAND ----------

import pandas as pd
import numpy as np
 
import time
import uuid
 
from ipywidgets import widgets, interact
 
import pyspark.sql.functions as fn

# COMMAND ----------

# MAGIC %md
# MAGIC ## Shipping Review Function

# COMMAND ----------

def pull_shipping_records_for_review():
    query = f"""select sid_id, shipping_street, shipping_street_clean, review_shipping_street_clean_flag,
    case when shipping_street IS NOT NULL AND shipping_street_clean is null then 'Shipping Street Null'
         when shipping_street_clean IS NOT NULL AND shipping_street_clean NOT LIKE '%0%'
                                               AND shipping_street_clean NOT LIKE '%1%'
                                               AND shipping_street_clean NOT LIKE '%2%'
                                               AND shipping_street_clean NOT LIKE '%3%'
                                               AND shipping_street_clean NOT LIKE '%4%'
                                               AND shipping_street_clean NOT LIKE '%5%'
                                               AND shipping_street_clean NOT LIKE '%6%'
                                               AND shipping_street_clean NOT LIKE '%7%'
                                               AND shipping_street_clean NOT LIKE '%8%'
                                               AND shipping_street_clean NOT LIKE '%9%' then 'does not Contains Number'
     end as shipping_street_rule_broken,
    
    shipping_city, shipping_city_clean, 
    case when shipping_city IS NOT NULL AND shipping_city_clean is null then 'Shipping City Null'
         when shipping_city_clean_number_flag = true then 'Contains Number'
    end as shipping_city_rule_broken,
    
    shipping_state, shipping_state_clean, review_shipping_state_clean_flag,
    case when shipping_state IS NOT NULL AND shipping_state_clean is null then 'Shipping State Null'
        when shipping_state_clean_number_flag = true then 'Contains Number'
        when invalid_ca_shipping_state_flag = true then 'Invalid CA State'
        when invalid_au_shipping_state_flag = true then 'Invalid AU State'
        when invalid_us_shipping_state_flag = true then 'Invalid US State'
    end as shipping_state_rule_broken,
    
    shipping_country, shipping_country_clean, 
    case when shipping_country IS NOT NULL AND shipping_country_clean is null then 'Shipping Country Null'
    when shipping_country_clean_number_flag = true then 'Contains Number'
    end as shipping_country_rule_broken,
    
    shipping_postal_code, shipping_postal_code_clean,review_invalid_shipping_postal_code_clean_flag,
    case when shipping_postal_code IS NOT NULL AND shipping_postal_code_clean is null then 'Shipping PostalCode Null'
    end as shipping_postal_code_rule_broken   
  
     from {source_catalog}.tmp.sf_account_clean_address_for_review 
     where (review_shipping_state_clean_flag = true
     or review_invalid_shipping_postal_code_clean_flag = true 
     or shipping_country_clean_number_flag = true
     or shipping_city_clean_number_flag = true
     or review_shipping_street_clean_flag = true
     or (shipping_street_clean NOT LIKE '%0%'
            AND shipping_street_clean NOT LIKE '%1%'
            AND shipping_street_clean NOT LIKE '%2%'
            AND shipping_street_clean NOT LIKE '%3%'
            AND shipping_street_clean NOT LIKE '%4%'
            AND shipping_street_clean NOT LIKE '%5%'
            AND shipping_street_clean NOT LIKE '%6%'
            AND shipping_street_clean NOT LIKE '%7%'
            AND shipping_street_clean NOT LIKE '%8%'
            AND shipping_street_clean NOT LIKE '%9%')
     )
     
    """
    df_records_to_review = spark.sql(query)
    return df_records_to_review


# COMMAND ----------

from pyspark.sql.functions import col,lit
result = pull_shipping_records_for_review() 
display(result.count())
display(result)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Manual Review Process - Shipping

# COMMAND ----------

from IPython.display import display as ipy_display, HTML
import ipywidgets as widgets
from functools import partial
import pandas as pd # Make sure pandas is imported

# define variable to avoid duplicate saves
ready_for_save = False

# GET DATA TO REVIEW 
review_data_pd = pull_shipping_records_for_review().toPandas() 
review_data_pd['sid_id'] = review_data_pd['sid_id'].str.strip()
n_records = int(review_data_pd.shape[0])
# ========================================================

# DEFINE IPYWIDGET DISPLAY
# ========================================================
display_pd = review_data_pd

orig_cols = ['sid_id', 'shipping_street', 'shipping_city', 'shipping_state', 'shipping_country', 'shipping_postal_code']
clean_cols = ['sid_id', 'shipping_street_clean', 'shipping_city_clean', 'shipping_state_clean', 'shipping_country_clean', 'shipping_postal_code_clean']
rule_cols = ['sid_id', 'shipping_street_rule_broken', 'shipping_city_rule_broken', 'shipping_state_rule_broken', 'shipping_country_rule_broken', 'shipping_postal_code_rule_broken']

# Initialize the dictionary to store user selections
# We will use this to track *actual* selections, including pre-filled clean values
user_selections = {}

# Pre-populate user_selections with the *resolved* value for each field
# This ensures that even for "No rule broken" fields, we have a selection stored
for _, row in review_data_pd.iterrows():
    sid = row['sid_id']
    user_selections[sid] = {'reviewed': False} # Add a flag to indicate if it was explicitly reviewed
    for i, col_name in enumerate(orig_cols[1:]): # Skip sid_id
        original_val = row[col_name]
        clean_val = row[clean_cols[i+1]]
        rule_broken = row[rule_cols[i+1]]
        
        selected_key = f'{col_name}_selected'

        if original_val == clean_val and rule_broken == None:
            # If no rule broken or original is clean, the original is the "selected" value
            user_selections[sid][selected_key] = original_val
        elif clean_val != '':
            # If there's a clean value and rule broken, pre-select the clean value
            user_selections[sid][selected_key] = clean_val
        else:
            # If clean_val is empty and rule is broken, user needs to pick, so set to None
            user_selections[sid][selected_key] = None

# Initialize the current record index
current_record_index = 0

# Placeholder for the dynamic content of the widget
record_display_box = widgets.VBox([])
status_label = widgets.Label(value=f"Record {current_record_index + 1} / {n_records}")

# Navigation buttons
prev_button = widgets.Button(description="Previous", disabled=True)
next_button = widgets.Button(description="Next", disabled=False)
save_button = widgets.Button(description="Save Selections")
output_messages = widgets.Output() # For displaying save messages or other feedback

def update_display(index):
    global current_record_index
    current_record_index = index

    if current_record_index < 0:
        current_record_index = 0
    if current_record_index >= n_records:
        current_record_index = n_records - 1

    # Update button states
    prev_button.disabled = (current_record_index == 0)
    next_button.disabled = (current_record_index == n_records - 1)
    status_label.value = f"Record {current_record_index + 1} / {n_records}"

    # Get the current record
    n = current_record_index
    candidate_left = display_pd[orig_cols].loc[n].to_list()
    candidate_right = display_pd[clean_cols].loc[n].to_list()
    rules = display_pd[rule_cols].loc[n].to_list()
    sid = candidate_left[0]

    # --- Re-create widgets for the current record ---
    current_record_widgets = []
    broken_rules = []

    # SID Label
    current_record_widgets.append(widgets.Label(value=f'SID ID: {candidate_left[0]}'))

    # Helper function to create the common ToggleButtons or Label logic
    def create_address_widget(field_name, orig_val, clean_val, rule_broken, current_sid):
        key = f'shipping_{field_name}_selected'
        selected_value = user_selections[current_sid].get(key) # Get previously selected value

        if orig_val == clean_val and rule_broken == None: #changing or to and  #orig_val == clean_val or rule_broken == 'No'
            # This field doesn't require explicit user action, just display
            return widgets.Label(value=f'{field_name.capitalize()}: {str(orig_val)}')
        else:
            broken_rules.append(f'{field_name.capitalize()} rule broken: {rule_broken}')
            options = [orig_val]
            if clean_val != '':
                options.append(clean_val)
            options.append('Custom') # Always offer custom if a rule is broken and user needs to pick

            custom_input = widgets.Text(description='Custom:', disabled=False, placeholder=f"Enter {field_name.capitalize()} Name")
            custom_output = widgets.Output() # Separate output for custom input

            # Determine initial value for ToggleButtons
            initial_toggle_value = None
            if selected_value in options: # If pre-selected value is one of the options
                initial_toggle_value = selected_value
            elif selected_value is not None: # If it's a custom value
                initial_toggle_value = 'Custom'

            toggle_buttons = widgets.ToggleButtons(
                options=options,
                description=f'{field_name.capitalize()}:',
                disabled=False,
                button_style='',
                style={"button_width": "auto"},
                value=initial_toggle_value # Set initial value
            )

            # Observe custom input changes
            def on_custom_change(change):
                user_selections[current_sid][key] = change['new']
                user_selections[current_sid]['reviewed'] = True # Mark as reviewed when value changes

            # Observe toggle button changes
            def on_toggle_change(change):
                with custom_output:
                    custom_output.clear_output() # Clear previous custom input if present
                    if change['new'] == 'Custom':
                        ipy_display(custom_input)
                        custom_input.observe(on_custom_change, names='value')
                        # If "Custom" is selected, and we have a previous custom value, set it
                        # This handles cases where 'Custom' was pre-selected due to a custom value
                        if selected_value not in options and selected_value is not None:
                            custom_input.value = selected_value
                    else:
                        user_selections[current_sid][key] = change['new']
                user_selections[current_sid]['reviewed'] = True # Mark as reviewed when value changes

            toggle_buttons.observe(on_toggle_change, names='value')

            # Initial display logic for custom input if a custom value was previously selected
            if initial_toggle_value == 'Custom' and selected_value not in options and selected_value is not None:
                with custom_output:
                    ipy_display(custom_input)
                    custom_input.value = selected_value
                    custom_input.observe(on_custom_change, names='value')
            
            return widgets.VBox([toggle_buttons, custom_output])

    current_record_widgets.append(create_address_widget('street', candidate_left[1], candidate_right[1], rules[1], sid))
    current_record_widgets.append(create_address_widget('city', candidate_left[2], candidate_right[2], rules[2], sid))
    current_record_widgets.append(create_address_widget('state', candidate_left[3], candidate_right[3], rules[3], sid))
    current_record_widgets.append(create_address_widget('country', candidate_left[4], candidate_right[4], rules[4], sid))
    current_record_widgets.append(create_address_widget('postal', candidate_left[5], candidate_right[5], rules[5], sid))

    # Display broken rules
    if broken_rules:
        current_record_widgets.insert(0, widgets.HTML(f"<b><font color='red'>Issues found:</font></b>"))
        current_record_widgets.insert(1, widgets.Label(value=', '.join(broken_rules)))
        # Mark as reviewed if issues are present (user sees them and should ideally act)
        user_selections[sid]['reviewed'] = True 
    else:
        current_record_widgets.insert(0, widgets.Label(value="No issues found for this record."))
        # If no issues, and all fields are already clean, we can mark as "implicitly reviewed"
        # or require explicit save. For this case, let's assume it's implicitly reviewed
        # if all its pre-populated values are not None (meaning they came from orig/clean)
        all_fields_clean = True
        for i, col_name in enumerate(orig_cols[1:]):
            if user_selections[sid].get(f'{col_name}_selected') is None:
                all_fields_clean = False
                break
        if all_fields_clean:
             user_selections[sid]['reviewed'] = True


    record_display_box.children = current_record_widgets

def on_next_button_clicked(b):
    update_display(current_record_index + 1)

def on_prev_button_clicked(b):
    update_display(current_record_index - 1)

def on_save_button_clicked(b):
    global ready_for_save
    
    # Filter user_selections to include only reviewed records
    reviewed_selections = {}
    for sid, data in user_selections.items():
        if data.get('reviewed', False): # Only include if the 'reviewed' flag is True
            # Remove the 'reviewed' flag before converting to DataFrame
            record_data = {k: v for k, v in data.items() if k != 'reviewed'}
            reviewed_selections[sid] = record_data

    if not reviewed_selections:
        with output_messages:
            output_messages.clear_output()
            print("No records have been reviewed yet. Nothing to save.")
        ready_for_save = False
        return

    selections_df = pd.DataFrame.from_dict(reviewed_selections, orient='index')
    selections_df.index.name = 'sid_id'
    selections_df = selections_df.reset_index()

    with output_messages:
        output_messages.clear_output()
        print(f"Saving {len(selections_df)} reviewed selections...")
        # For demonstration, just display it. In a real scenario, you'd save it:
        ipy_display(selections_df.head()) # Show a snippet of what's being saved
        print("Reviewed selections saved (or displayed above for verification).")
    ready_for_save = True

next_button.on_click(on_next_button_clicked)
prev_button.on_click(on_prev_button_clicked)
save_button.on_click(on_save_button_clicked)

# Initial display
update_display(0)

# Assemble the main layout
navigation_controls = widgets.HBox([prev_button, status_label, next_button])
main_app_layout = widgets.VBox([
    widgets.HTML('<h2>For each record, choose the correct value for the display address component(s).</h2>'),
    navigation_controls,
    record_display_box, # This box will be updated dynamically
    save_button,
    output_messages
])

ipy_display(main_app_layout)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify the selected values

# COMMAND ----------

# After the widgets are displayed and interacted with, you can access the user selections:
#print("\nUser Selections:")
#print(user_selections)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prepare for Merge

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType,StructField, StringType, BooleanType,TimestampType 
import pandas as pd

# Filter user_selections to include only reviewed records
reviewed_selections = {}
for sid, data in user_selections.items():
    # Only include the record if its 'reviewed' flag is True
    if data.get('reviewed', False):
        # Create a new dictionary for this record, excluding the 'reviewed' flag itself
        record_data = {k: v for k, v in data.items() if k != 'reviewed'}
        reviewed_selections[sid] = record_data

# Now, create the DataFrame ONLY from the 'reviewed_selections' dictionary
if reviewed_selections: # Check if there are any reviewed records
    selections_df = pd.DataFrame.from_dict(reviewed_selections, orient='index')
    selections_df.index.name = 'sid_id' # Set the index name
    selections_df = selections_df.reset_index() # Convert index to a column
    
    # Display only the reviewed selections
    #display(selections_df)
else:
    # Handle the case where no records have been reviewed yet
    print("No records have been reviewed yet to display.")

# Merge the original data with the selections
# Using a left merge to keep all original records, even if not selected
updated_review_data_pd = review_data_pd.merge(selections_df, on='sid_id', how='left')
# Get the list of sid_ids that were actually reviewed
reviewed_sids = selections_df['sid_id'].tolist()

# Filter the merged DataFrame
only_reviewed_merged_data_pd = updated_review_data_pd[
    updated_review_data_pd['sid_id'].isin(reviewed_sids)
]

ipy_display(only_reviewed_merged_data_pd)
#ipy_display(updated_review_data_pd)

# Define the schema explicitly
schema = StructType([
    StructField("sid_id", StringType(), True),
    StructField("shipping_street", StringType(), True),  
    StructField("shipping_street_clean", StringType(), True),  
    StructField("review_shipping_street_clean_flag", BooleanType(), True), 
    StructField("shipping_street_rule_broken", StringType(), True), 
    StructField("shipping_city", StringType(), True), 
    StructField("shipping_city_clean", StringType(), True), 
    StructField("shipping_city_rule_broken", StringType(), True), 
    StructField("shipping_state", StringType(), True), 
    StructField("shipping_state_clean", StringType(), True), 
    StructField("review_shipping_state_clean_flag", BooleanType(), True), 
    StructField("shipping_state_rule_broken", StringType(), True), 
    StructField("shipping_country", StringType(), True), 
    StructField("shipping_country_clean", StringType(), True), 
    StructField("shipping_country_rule_broken", StringType(), True), 
    StructField("shipping_postal_code", StringType(), True), 
    StructField("shipping_postal_code_clean", StringType(), True), 
    StructField("review_invalid_shipping_postal_code_clean_flag", BooleanType(), True), 
    StructField("shipping_postal_code_rule_broken", StringType(), True), 
    StructField("shipping_street_selected", StringType(), True), 
    StructField("shipping_city_selected", StringType(), True), 
    StructField("shipping_state_selected", StringType(), True), 
    StructField("shipping_country_selected", StringType(), True),
    StructField("shipping_postal_code_selected", StringType(), True)
])

# Convert Pandas DataFrame to Spark DataFrame
updated_review_data_spark_df = spark.createDataFrame(only_reviewed_merged_data_pd, schema = schema)

#print("Spark DataFrame Schema:")
#updated_review_data_spark_df.printSchema()
print("\nSpark DataFrame (first 5 rows):")
updated_review_data_spark_df.display(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Merge the Updates to table

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp, lit
import re

reviewed_by = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
reviewed_by_cleaned = re.sub(r'@.*?\.com', '', reviewed_by)
#display(reviewed_by_cleaned)

try:              
    deltaTable = DeltaTable.forName(spark, f'{source_catalog}.tmp.sf_account_clean_address_for_review')
    join_condition = "target.sid_id = source.sid_id"

    update_columns = {
        "target.shipping_street_clean": "source.shipping_street_selected",
        "target.shipping_city_clean": "source.shipping_city_selected",
        "target.shipping_state_clean": "source.shipping_state_selected",
        "target.shipping_country_clean": "source.shipping_country_selected",
        "target.shipping_postal_code_clean": "source.shipping_postal_code_selected",
        "target.address_last_cleaned_timestamp" : current_timestamp(),
        "target.review_status" : lit("Reviewed"),
        "target.reviewed_by" : lit(reviewed_by_cleaned),
        "target.final_clean_flag" : lit(0),
        "target.needs_manual_review" : lit("false"),
        "target.reviewed_date" : current_timestamp()
    }

    (deltaTable.alias("target")
     .merge(
         updated_review_data_spark_df.alias("source"),
         join_condition
     )
     .whenMatchedUpdate(set=update_columns)
     .execute())

    print(f"Successfully merged updates into table: {source_catalog}.sf_account_clean_address_for_review")

except Exception as e:
    print(f"Error merging data into table: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation - View Manually Reviewed Data

# COMMAND ----------

'''
%sql
select sid_id, shipping_street_clean, shipping_city_clean, shipping_state_clean, shipping_country_clean, shipping_postal_code_clean
from mdm_dev.tmp.sf_account_clean_address_for_review
where sid_id in ('0014Q00002IkoREQAZ','00130000012yTc8AAE')
'''

