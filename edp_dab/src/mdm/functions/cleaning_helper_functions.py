# Databricks notebook source
from pyspark.sql.functions import when,col,regexp_replace,upper,lower,lit
from pyspark.sql import functions as F
from pyspark.sql.functions import col, when, upper, lower, lit, regexp_replace, trim, explode, split, udf
from pyspark.sql.types import StringType

# COMMAND ----------

# MAGIC %md ####function to check if a version of 'acct' occurs in a column.
# MAGIC

# COMMAND ----------

def split_acct_from_string(df, column_name):
    """
    Checks if a version of the string 'acct' occurs in the value of a dataframe column

    Args:
        df (spark dataframe): The dataframe containing the data to check 'acct'.
        column_name (str): The name of the column to check values of.

    Returns:
        df : Original dataframe with cleaned column_name,additional columns {column_name}_acct_flag indicating if 'acct' was present in column_name and {column_name}_additional_data containing data removed from column_name
    """
    # if 'acct' exists in column_name, set flag to True else False
    df = df.withColumn(
        f'{column_name}_acct_flag',
        F.when(
            F.lower(F.col(column_name)).like('% acct %') | 
            F.lower(F.col(column_name)).like('acct %')|
            F.lower(F.col(column_name)).like('% acct')|
            F.lower(F.col(column_name)).like('acct'), 
            True
        ).otherwise(False)
    )
    # if the column_name contains 'acct', append the corresponding information from the 'acct' to the end of the string in the 'column_name_additional_data' column.
    df = df.withColumn(
        f"{column_name}_additional_data", 
        F.when(
            F.lower(F.col(column_name)).like('% acct %') | 
            F.lower(F.col(column_name)).like('acct %')|
            F.lower(F.col(column_name)).like('% acct')|
            F.lower(F.col(column_name)).like('acct'), 
            F.regexp_extract(F.col(column_name), '(?is)(acct.*)', 0)
        ).otherwise('')
    )
    # if 'acct' exists in column_name, set column_name by removing 'acct' and everything to right of it
    df = df.withColumn(
        column_name,
        F.trim( 
        F.when(
            F.lower(F.col(column_name)).like('% acct %') | 
            F.lower(F.col(column_name)).like('acct %')|
            F.lower(F.col(column_name)).like('% acct')|
            F.lower(F.col(column_name)).like('acct'), 
            F.regexp_replace(F.col(column_name), '(?is)acct.*', '')
        ).otherwise(F.col(column_name))
        )
    )
    return df

# COMMAND ----------

# MAGIC %md ####function to check if a version of 'on behalf of' occurs in a column.

# COMMAND ----------

def split_on_behalf_from_string(df, column_name):
    """
    Checks if a version of the string 'on behalf of' occurs in the value of a dataframe column

    Args:
        df (spark dataframe): The dataframe containing the data to check 'on behalf of'.
        column_name (str): The name of the column to check values of.

    Returns:
        df : Original dataframe with cleaned column_name,additional column {column_name}_on_behalf_flag indicating if 'on behalf of' was present in column_name and {column_name}_additional_data containing data removed from column_name
    """
    # if 'on behalf of' exists in column_name, set flag to True else False
    df = df.withColumn(
        f'{column_name}_on_behalf_flag', 
        F.when(
            F.lower(F.col(column_name)).like('% on behalf %') | 
            F.lower(F.col(column_name)).like('on behalf %')|
            F.lower(F.col(column_name)).like('% on behalf')|
            F.lower(F.col(column_name)).like('on behalf'), 
            True
        ).otherwise(False)
    )
    # if the column_name contains 'on behalf of', append the corresponding information from the 'on behalf of' to the end of the string in the 'column_name_additional_data' column.
    df = df.withColumn(
        f"{column_name}_additional_data", 
        F.concat_ws('', F.col(f"{column_name}_additional_data"),
        F.when(
            F.lower(F.col(column_name)).like('% on behalf %') | 
            F.lower(F.col(column_name)).like('on behalf %')|
            F.lower(F.col(column_name)).like('% on behalf')|
            F.lower(F.col(column_name)).like('on behalf'), 
            F.regexp_extract(F.col(column_name), '(?is)(on behalf.*)', 0)
        ).otherwise('')
    ))
    # if 'on behalf of' exists in column_name,set column_name by removing 'on behalf of' and everything to right of it
    df = df.withColumn(
        column_name,
        F.trim(
        F.when(
            F.lower(F.col(column_name)).like('% on behalf %') | 
            F.lower(F.col(column_name)).like('on behalf %')|
            F.lower(F.col(column_name)).like('% on behalf')|
            F.lower(F.col(column_name)).like('on behalf'), 
            F.regexp_replace(F.col(column_name), '(?is)on behalf.*', '')
        ).otherwise(F.col(column_name))
        )
    )
    return df

# COMMAND ----------

# MAGIC %md ####function to check if a version of 'aka' occurs in a column.

# COMMAND ----------

def split_aka_from_string(df, column_name):
    """
    Checks if a version of the string 'aka' occurs in the value of a dataframe column

    Args:
        df (spark dataframe): The dataframe containing the data to check 'aka'.
        column_name (str): The name of the column to check values of.

    Returns:
        df : Original dataframe with cleaned column_name,additional column {column_name}_aka_flag indicating if 'aka' was present in column_name and {column_name}_additional_data containing data removed from column_name
    """
    # if 'aka' exists in column_name, set flag to True else False
    df = df.withColumn(
        f'{column_name}_aka_flag', 
        F.when(
            F.lower(F.col(column_name)).like('% aka %') | 
            F.lower(F.col(column_name)).like('aka %') |
            F.lower(F.col(column_name)).like('% aka')|
            F.lower(F.col(column_name)).like('aka'), 
            True
        ).otherwise(False)
    )
    # if the column_name contains 'aka', append the corresponding information from the 'aka' to the end of the string in the 'column_name_additional_data' column.
    df = df.withColumn(
        f"{column_name}_additional_data", 
        F.concat_ws('', F.col(f"{column_name}_additional_data"),
        F.when(
            F.lower(F.col(column_name)).like('% aka %') | 
            F.lower(F.col(column_name)).like('aka %')|
            F.lower(F.col(column_name)).like('% aka')|
            F.lower(F.col(column_name)).like('aka'), 
            F.regexp_extract(F.col(column_name), '(?is)(aka.*)', 0)
        ).otherwise('')
    ))
    # if 'aka' exists in column_name,set column_name by removing 'aka' and everything to right of it
    df = df.withColumn(
        column_name,
        F.trim( 
        F.when(
            F.lower(F.col(column_name)).like('% aka %') | 
            F.lower(F.col(column_name)).like('aka %') |
            F.lower(F.col(column_name)).like('% aka')|
            F.lower(F.col(column_name)).like('aka'), 
            F.regexp_replace(F.col(column_name), '(?is)aka.*', '')
        ).otherwise(F.col(column_name))
        )
    )
    return df

# COMMAND ----------

# MAGIC %md ####function to check if a version of 'successive asterisks(**)' occurs in a column.

# COMMAND ----------

def remove_asterisks_from_string(df, column_name):
    """
    Checks if range of asterisks occurs in the value of a dataframe column

    Args:
        df (spark dataframe): The dataframe containing the data to check '**/*** etc..(range of asterisks)'.
        column_name (str): The name of the column to check values of.

    Returns:
        df : Original dataframe with cleaned column_name,additional column {column_name}_asterisks_flag indicating if successive asterisks are present in column_name and {column_name}_additional_data containing data removed from column_name
    """
    # if any range of asterisks exists in column, set flag to True
    df = df.withColumn(f'{column_name}_asterisks_flag', F.when(F.lower(F.col(column_name)).like('%**%'), True).otherwise(False))
    
    # if the column_name contains successive asterisks, append the corresponding information present b/w asterisks (including asterisks) in the 'column_name_additional_data' column.
    df = df.withColumn(
        f"{column_name}_additional_data",
        F.concat_ws('', F.col(f"{column_name}_additional_data"),
        F.when(
            F.lower(F.col(column_name)).like('%**%'), 
            F.expr(f"regexp_extract({column_name}, r'\*\*.*\*\*', 0)")
        ).otherwise('')
    ))
    # if column_name contains successive asterisks, remove them and anything between them
    df = df.withColumn(column_name,
                       F.trim(
                        F.when(F.lower(F.col(column_name)).like('%**%'),
                        F.regexp_replace((F.col(column_name)), r'\*\*.*\*\*', '')
                        ).otherwise(F.col(column_name))))
    df = df.withColumn(
        column_name,
        F.trim(
        F.when(F.lower(F.col(column_name)).like('%**%'),
         F.regexp_replace((F.col(column_name)), r'\*\*', '')
        ).otherwise(F.col(column_name))
        )
    )
    return df

# COMMAND ----------

# MAGIC %md ####function to check if a version of 'care of(c/o)' occurs in a column.
# MAGIC

# COMMAND ----------

def split_careof_from_string(df, column_name):
    """
    Checks if a version of the string 'c/o' occurs in the value of a dataframe column

    Args:
        df (spark dataframe): The dataframe containing the data to check 'c/o'.
        column_name (str): The name of the column to check values of.

    Returns:
        df : Original dataframe with cleaned column_name,additional column {column_name}_care_of_flag indicating if 
        'c/o' was present in column_name and {column_name}_additional_data containing data removed from column_name
    """
    # if c/o exists in column_name, set flag to True else False
    df = df.withColumn(f'{column_name}_care_of_flag', F.when(F.lower(F.col(column_name)).like('%c/o%'), True).otherwise(False))

    # if the column_name contains 'c/o', append the corresponding information from the 'c/o' to the end of the string in the 'column_name_additional_data' column.
    df = df.withColumn(
        f"{column_name}_additional_data",
        F.concat_ws('', F.col(f"{column_name}_additional_data"),
        F.when(
            F.lower(F.col(column_name)).like('%c/o%'),
            F.regexp_extract(F.col(column_name), '(?is)(c/o.*)', 0)
        ).otherwise('')
    ))
    # if 'c/o' exists in column_name,set column_name by removing 'c/o' and everything to right of it
    df = df.withColumn(
        column_name,
        F.trim(
            F.when(
                F.lower(F.col(column_name)).like('%c/o%'),
                F.regexp_replace(F.col(column_name), '(?is)c/o.*', '')
            ).otherwise(F.col(column_name))
        )
    )
    return df

# COMMAND ----------

# MAGIC %md ####function to check if a version of 'dba (or) d/b/a' occurs in a column.
# MAGIC

# COMMAND ----------

def split_dba_from_string(df, column_name):
    """
    Checks if a version of the string 'dba(or)d/b/a' occurs in the value of a dataframe column

    Args:
        df (spark dataframe): The dataframe containing the data to check 'dba(or)d/b/a'.
        column_name (str): The name of the column to check values of.

    Returns:
        df : Original dataframe with cleaned column_name,additional column {column_name}_dba_flag indicating if 
        'dba(or)d/b/a' was present in column_name and {column_name}_additional_data containing data removed from column_name
    """
    # if dba or d/b/a exists in column_name, set flag to True else False
    df = df.withColumn(f'{column_name}_dba_flag', F.when(F.lower(F.col(column_name)).like('% dba %') | F.lower(F.col(column_name)).like('% d/b/a %'), True).otherwise(False))
    
    # if the column_name contains 'dba or d/b/a', append the corresponding information from the 'dba or d/b/a' to the end of the string in the 'column_name_additional_data' column.
    df = df.withColumn(
        f"{column_name}_additional_data",
        F.concat_ws('', F.col(f"{column_name}_additional_data"),
        F.when(
            F.lower(F.col(column_name)).rlike(' dba | d/b/a '), 
            F.regexp_extract(F.col(column_name), '(?is) (dba .*|d/b/a .*)', 0)
        ).otherwise('')
    ))
    # if dba or d/b/a exists in column_name by removing 'dba (or)d/b/a' and everything to right of it
    df = df.withColumn(column_name, 
                       F.trim(
                       F.when(F.lower(F.col(column_name)).like('% dba %'), F.expr(f"regexp_replace({column_name}, '(?is) dba .*', '')"))
                       .when(F.lower(F.col(column_name)).like('% d/b/a %'), F.expr(f"regexp_replace({column_name}, '(?is) d/b/a .*', '')"))
                        .otherwise(F.col(column_name))
                        )
                    )
    return df

# COMMAND ----------

# MAGIC %md ####function to check if a version of 'donot/dont/do not' occurs in a column.
# MAGIC

# COMMAND ----------

def split_do_not_from_string(df, column_name):
    """
    Checks if a version of the string 'donot(or)dont' occurs in the value of a dataframe column

    Args:
        df (spark dataframe): The dataframe containing the data to check 'donot(or)dont'.
        column_name (str): The name of the column to check values of.

    Returns:
        df : Original dataframe with cleaned column_name,additional column {column_name}_donot_flag indicating if 
        'donot(or)dont' was present in column_name and {column_name}_additional_data containing data removed from column_name
    """
    # if 'do not' or 'dont' exists in column_name, set flag to True else False
    df = df.withColumn(f'{column_name}_donot_flag', 
                       F.when(F.lower(F.col(column_name)).like('% do not %') |
                              F.lower(F.col(column_name)).like('do not%') |
                              F.lower(F.col(column_name)).like('%do not') |
                              F.lower(F.col(column_name)).like('do not') | 
                              F.lower(F.col(column_name)).like('% dont %')|
                              F.lower(F.col(column_name)).like('dont %')|
                              F.lower(F.col(column_name)).like('% dont')|
                              F.lower(F.col(column_name)).like('dont'), True).otherwise(False))
    
    # if the column_name contains 'donot (or)dont', append the corresponding information from the 'donot (or)dont' to the end of the string in the 'column_name_additional_data' column.
    df = df.withColumn(
        f"{column_name}_additional_data",
        F.concat_ws('', F.col(f"{column_name}_additional_data"),
        F.when(
            F.lower(F.col(column_name)).rlike(' dont | do not |dont |do not | dont| do not|dont|do not'), 
            F.regexp_extract(F.col(column_name), '(?is)( dont .*| do not .*|dont .*|do not .*|dont|do not)', 0)
        ).otherwise('')
    )) 
    # if 'donot (or)dont' exists in column_name,set column_name by removing 'donot (or)dont' and everything to right of it
    df = df.withColumn(column_name,
                       F.trim( 
                       F.when(F.lower(F.col(column_name)).rlike(' do not |do not| dont |dont'), 
                              F.expr(f"regexp_replace({column_name}, '(?is) dont .*| do not .*|dont .*|do not .*|dont|do not', '')"))
                       .otherwise(F.col(column_name))))
    return df

# COMMAND ----------

# MAGIC %md ####function to check if a version of 'duplicate' occurs in a column.
# MAGIC

# COMMAND ----------

def split_duplicate_from_string(df, column_name):
    """
    Checks if a version of the string 'duplicate' occurs in the value of a dataframe column

    Args:
        df (spark dataframe): The dataframe containing the data to check 'duplicate'.
        column_name (str): The name of the column to check values of.

    Returns:
        df : Original dataframe with cleaned column_name,additional column {column_name}_duplicate_flag indicating if 
        'duplicate' was present in column_name and {column_name}_additional_data containing data removed from column_name
    """
    # if 'duplicate' exists in column_name, set flag to True else False
    df = df.withColumn(f'{column_name}_duplicate_flag', 
                       F.when(F.lower(F.col(column_name)).like('%duplicate%'), True).otherwise(False))
    
    # if the column_name contains 'duplicate', append the corresponding information from the 'duplicate' to the end of the string in the 'column_name_additional_data' column.
    df = df.withColumn(
        f"{column_name}_additional_data",
        F.concat_ws('', F.col(f"{column_name}_additional_data"),
        F.when(
            F.lower(F.col(column_name)).like('%duplicate%'), 
            F.regexp_extract(F.col(column_name), '(?is)duplicate.*', 0)
        ).otherwise('')
    ))
    # if 'duplicate' exists in column_name,set column_name by removing 'duplicate' and everything to right of it
    df = df.withColumn(column_name,
                       F.trim( 
                       F.when(F.lower(F.col(column_name)).like('%duplicate%'), 
                       F.expr(f"regexp_replace({column_name}, '(?is)duplicate.*', '')")).otherwise(F.col(column_name))
                       ))
    return df

# COMMAND ----------

# MAGIC %md ####function to check if a version of 'fbo' occurs in a column.
# MAGIC

# COMMAND ----------

def split_fbo_from_string(df, column_name):
    """
    Checks if a version of the string 'fbo' occurs in the value of a dataframe column

    Args:
        df (spark dataframe): The dataframe containing the data to check 'fbo'.
        column_name (str): The name of the column to check values of.

    Returns:
        df : Original dataframe with cleaned column_name,additional column {column_name}_fbo_flag indicating if 
        'fbo' was present in column_name and {column_name}_additional_data containing data removed from column_name
    """
    # if 'fbo' exists in column_name, set flag to True else False
    df = df.withColumn(f'{column_name}_fbo_flag', 
                       F.when(F.lower(F.col(column_name)).like('% fbo %'), True).otherwise(False))
    
    # if the column_name contains 'fbo', append the corresponding information from the 'fbo' to the end of the string in the 'column_name_additional_data' column.
    df = df.withColumn(
        f"{column_name}_additional_data",
        F.concat_ws('', F.col(f"{column_name}_additional_data"),
        F.when(
            F.lower(F.col(column_name)).like('% fbo %'), 
            F.regexp_extract(F.col(column_name), '(?is)( fbo .*)', 0)
        ).otherwise('')
    ))
    # if 'fbo' exists in column_name,set column_name by removing 'fbo' and everything to right of it
    df = df.withColumn(column_name,
                       F.trim( 
                       F.when(F.lower(F.col(column_name)).like('% fbo %'), 
                       F.expr(f"regexp_replace({column_name}, '(?is) fbo .*', '')")).otherwise(F.col(column_name)))
    )
    return df

# COMMAND ----------

# MAGIC %md ####function to check if a column has too long or too short values

# COMMAND ----------

def check_length_of_string(df, column_name):
    """
    Checks if length of the string is too short or too long in df column

    Args:
        df (spark dataframe): The dataframe containing the data to check length
        column_name (str): The name of the column to check length of the string.

    Returns:
        df : Original dataframe with cleaned column_name,additional column {column_name}_length_flag indicating if 
        the length is too long or too short and {column_name}_additional_data containing data removed from column_name
    """
    # if length of the string is too short or too long, set flag to True else False
    df = df.withColumn(f'{column_name}_length_flag', 
                       F.when((F.length(F.col(column_name)) < 3) | (F.length(F.col(column_name)) > 100), True).otherwise(False))
    
    # store the information in the column_name_additional_data column if the string length is less than 2 or greater than 100
    df = df.withColumn(
        f"{column_name}_additional_data",
        F.concat_ws('', F.col(f"{column_name}_additional_data"),
        F.when((F.length(F.col(column_name)) < 3) | (F.length(F.col(column_name)) > 100), F.col(column_name)).otherwise('')
    ))
    
    # if length is too short/long in column_name, set column_name to an empty string
    df = df.withColumn(column_name,
                       F.trim( 
                       F.when(F.col(f'{column_name}_length_flag'), '').otherwise(F.col(column_name))))
    return df

# COMMAND ----------

# MAGIC %md ####function to check if a version of 'none (or) n/a' occurs in a column.

# COMMAND ----------

def split_none_na_from_string(df, column_name):
    """
    Checks if a version of the string 'none (or)n/a' occurs in the value of a dataframe column

    Args:
        df (spark dataframe): The dataframe containing the data to check 'none (or)n/a'.
        column_name (str): The name of the column to check values of.

    Returns:
        df : Original dataframe with cleaned column_name,additional column {column_name}_none_na_flag indicating if 
        'none (or)n/a' was present in column_name and {column_name}_additional_data containing data removed from column_name
    """
    # if none or n/a exists in column, set flag to True else False
    df = df.withColumn(f'{column_name}_none_na_flag', 
                       F.when(F.lower(F.col(column_name)).like('% none %') | F.lower(F.col(column_name)).like('% n/a %') | (F.lower(F.col(column_name)) == 'none') | (F.lower(F.col(column_name)) == 'n/a')|F.lower(F.col(column_name)).like('n/a %')|F.lower(F.col(column_name)).like('% n/a')|F.lower(F.col(column_name)).like('none %')|F.lower(F.col(column_name)).like('% none')|F.lower(F.col(column_name)).like('n/a%')|F.lower(F.col(column_name)).like('%(none)%')|F.lower(F.col(column_name)).like('%(n/a)%')|F.lower(F.col(column_name)).like('none%')|F.lower(F.col(column_name)).like('n/a%'), True).otherwise(False))
    
    # if the column_name contains 'none (or) n/a', append the corresponding information from the 'none (or) n/a' to the end of the string in the 'column_name_additional_data' column.
    df = df.withColumn(
        f"{column_name}_additional_data",
        F.concat_ws('', F.col(f"{column_name}_additional_data"),
       F.when(
            F.lower(F.col(column_name)).rlike(' none | n/a |none |n/a | none| n/a|none|n/a'), 
            F.regexp_extract(F.col(column_name), '(?is) none .*| n/a .*|none .*|n/a .*|none|n/a', 0)
        ).otherwise('')
    ))
    # if none or n/a exists in column_name, set column_name to an empty string
    df = df.withColumn(column_name,
                       F.trim(
                       F.when(F.lower(F.col(column_name)).like('% none %') | F.lower(F.col(column_name)).like('% n/a %') | (F.lower(F.col(column_name)) == 'none') | (F.lower(F.col(column_name)) == 'n/a')|F.lower(F.col(column_name)).like('n/a %')|F.lower(F.col(column_name)).like('% n/a')|F.lower(F.col(column_name)).like('none %')|F.lower(F.col(column_name)).like('% none')|F.lower(F.col(column_name)).like('n/a%')|F.lower(F.col(column_name)).like('%(none)%')|F.lower(F.col(column_name)).like('%(n/a)%')|F.lower(F.col(column_name)).like('none%')|F.lower(F.col(column_name)).like('n/a%'), '').otherwise(F.col(column_name))))

    return df

# COMMAND ----------

# MAGIC %md ####function to check if a version of 'student' occurs in a column.
# MAGIC

# COMMAND ----------

def split_student_from_string(df, column_name):
    """
    Checks if a version of the string 'student' occurs in the value of a dataframe column

    Args:
        df (spark dataframe): The dataframe containing the data to check 'student'.
        column_name (str): The name of the column to check values of.

    Returns:
        df : Original dataframe with cleaned column_name,additional column {column_name}_student_flag indicating if 
        'student' was present in column_name and {column_name}_additional_data containing data removed from column_name
    """
    # if student exists in column_name, set flag to True else False
    df = df.withColumn(f'{column_name}_student_flag', F.when(F.lower(F.col(column_name)).like('%student%'), True).otherwise(False))

    # if the column_name contains 'student', append the corresponding information from the 'student' to the end of the string in the 'column_name_additional_data' column.
    df = df.withColumn(
        f"{column_name}_additional_data",
        F.concat_ws('', F.col(f"{column_name}_additional_data"),
       F.when(
            F.lower(F.col(column_name)).like('%student%'), 
            F.regexp_extract(F.col(column_name), '(?is)(student.*)', 0)
        ).otherwise('')
    ))
    # if student exists in column_name,set column_name by removing 'student' and everything to right of it
    df = df.withColumn(column_name,
                       F.trim( 
                       F.when(F.lower(F.col(column_name)).like('%student%'), F.expr(f"regexp_replace({column_name}, '(?is)student.*', '')")).otherwise(F.col(column_name))))
    return df

# COMMAND ----------

# MAGIC %md ####function to check if a version of 'test' occurs in a column.
# MAGIC

# COMMAND ----------

def split_test_from_string(df, column_name):
    """
    Checks if a version of the string 'test' occurs in the value of a dataframe column

    Args:
        df (spark dataframe): The dataframe containing the data to check 'test'.
        column_name (str): The name of the column to check values of.

    Returns:
        df : Original dataframe with cleaned column_name,additional column {column_name}_test_flag indicating if 
        'test' was present in column_name and {column_name}_additional_data containing data removed from column_name
    """
    # if test exists in column_name, set flag to True else False
    df = df.withColumn(
        f'{column_name}_test_flag',
        # F.when(F.lower(F.col(column_name)).rlike(r'(^test$|^test\s|\stest\s|\stest$)'), True).otherwise(False)
        F.when(F.lower(F.col(column_name)).rlike('(?is).*test.*'), True).otherwise(False)
    )

    # if the column_name contains 'test', append the entire string to the 'column_name_additional_data' column.
    df = df.withColumn(
        f"{column_name}_additional_data",
        F.concat_ws(
            '',
            F.col(f"{column_name}_additional_data"),
            F.when(
                F.lower(F.col(column_name)).rlike('(?is).*test.*'),
                F.col(column_name) # Store the entire original string
            ).otherwise('')
        )
    )
    # if 'test' exists in column_name, set column_name to an empty string
    df = df.withColumn(
        column_name,
        F.trim(
            F.when(
                F.lower(F.col(column_name)).rlike('(?is).*test.*'),
                F.lit("")
            ).otherwise(F.col(column_name))
        )
    )
    return df

# COMMAND ----------

# MAGIC %md ####function to check if a version of 'trust' occurs in a column.
# MAGIC

# COMMAND ----------

def split_trust_from_string(df, column_name):
    """
    Checks if a version of the string 'trust' occurs in the value of a dataframe column

    Args:
        df (spark dataframe): The dataframe containing the data to check 'trust'.
        column_name (str): The name of the column to check values of.

    Returns:
        df : Original dataframe with cleaned column_name,additional column {column_name}_trust_flag indicating if 
        'trust' was present in column_name and {column_name}_additional_data containing data removed from column_name
    """
    # if trust exists in column_name, set flag to True else False
    df = df.withColumn(f'{column_name}_trust_flag', 
                       F.when(F.lower(F.col(column_name)).like('%trust%'), True).otherwise(False))
    
    # if the column_name contains 'trust', append the corresponding information from the 'trust' to the end of the string in the 'column_name_additional_data' column.
    df = df.withColumn(
        f"{column_name}_additional_data",
        F.concat_ws('', F.col(f"{column_name}_additional_data"),
       F.when(
            F.lower(F.col(column_name)).like('%trust%'), 
            F.regexp_extract(F.col(column_name), '(?is)trust.*', 0)
        ).otherwise('')
    ))
    # if 'trust' exists in column_name,set column_name by removing 'trust' and everything to right of it
    df = df.withColumn(column_name,
                       F.trim( 
                       F.when(F.lower(F.col(column_name)).like('%trust%'),
                       F.expr(f"regexp_replace({column_name}, '(?is)trust.*', '')")).otherwise(F.col(column_name))))

    return df

# COMMAND ----------

# MAGIC %md ####function to check if a version of 'tbd' occurs in a column.
# MAGIC

# COMMAND ----------

def split_tbd_from_string(df, column_name):
    """
    Checks if a version of the string 'tbd' occurs in the value of a dataframe column

    Args:
        df (spark dataframe): The dataframe containing the data to check 'tbd'.
        column_name (str): The name of the column to check values of.

    Returns:
        df : Original dataframe with cleaned column_name,additional column {column_name}_tbd_flag indicating if 
        'tbd' was present in column_name and {column_name}_additional_data containing data removed from column_name
    """
    # if tbd exists in column_name, set flag to True else False
    df = df.withColumn(f'{column_name}_tbd_flag', F.when(F.lower(F.col(column_name)).like('%tbd%'), True).otherwise(False))

    # if the column_name contains 'tbd', append the entire string to the 'column_name_additional_data' column.
    df = df.withColumn(
        f"{column_name}_additional_data",
        F.concat_ws('', F.col(f"{column_name}_additional_data"),
       F.when(
            F.lower(F.col(column_name)).like('%tbd%'),
            F.col(column_name)
        ).otherwise('')
    ))
    # if 'tbd' exists in column_name, set column_name to an empty string
    df = df.withColumn(column_name,
                       F.trim(
                        F.when(F.lower(F.col(column_name)).like('%tbd%'),
                        F.lit("")
                    ).otherwise(F.col(column_name))
                    )
                )
    return df

# COMMAND ----------

# MAGIC %md ####function to check if a version of 'attn:' occurs in a column.
# MAGIC

# COMMAND ----------

def split_attn_from_string(df, column_name):
    """
    Checks if a version of the string 'attn:' occurs in the value of a dataframe column

    Args:
        df (spark dataframe): The dataframe containing the data to check 'attn:'.
        column_name (str): The name of the column to check values of.

    Returns:
        df : Original dataframe with cleaned column_name,additional column {column_name}_attn_flag indicating if 
        'attn:' was present in column_name and {column_name}_additional_data containing data removed from column_name
    """
    # if attn: exists in column_name, set flag to True else False
    df = df.withColumn(f'{column_name}_attn_flag', F.when(F.lower(F.col(column_name)).like('%attn:%'), True).otherwise(False))

    # if the column_name contains 'attn:', append the corresponding information from the 'attn:' to the end of the string in the 'column_name_additional_data' column.
    df = df.withColumn(
        f"{column_name}_additional_data",
        F.when(
            F.lower(F.col(column_name)).like('%attn:%'),
            F.regexp_extract(F.col(column_name), '(?is)(attn:.*)', 0)
        ).otherwise('')
    )
    # if 'attn:' exists in column_name,set column_name by removing 'attn:' and everything to right of it
    df = df.withColumn(
        column_name,
        F.trim(
            F.when(
                F.lower(F.col(column_name)).like('%attn:%'),
                F.regexp_replace(F.col(column_name), '(?is)attn:.*', '')
            ).otherwise(F.col(column_name))
        )
    )
    return df

# COMMAND ----------

# MAGIC %md ####function to check if numeric values present in a column.
# MAGIC

# COMMAND ----------

def check_numeric_string(df, column_name):
    """
    Checks if a string contains numeric values in a dataframe column

    Args:
        df (spark dataframe): The dataframe containing the data to check numeric values.
        column_name (str): The name of the column to check values of.

    Returns:
        df : Original dataframe with cleaned column_name,additional column {column_name}_number_flag indicating if 
        numbers are present in column_name
    """
    # Check for numeric values
    df = df.withColumn(f'{column_name}_number_flag', when(col(column_name).rlike(r'[0-9]'), True)
                       .otherwise(False))
    # Remove numeric values
    df = df.withColumn(column_name, regexp_replace(col(column_name), '[0-9]', ''))  
    return df

# COMMAND ----------

# MAGIC %md ####function to check if null values present in a column.
# MAGIC

# COMMAND ----------

def check_is_null_string(df, column_name):
    """
    Checks if a string contains null values in a dataframe column

    Args:
        df (spark dataframe): The dataframe containing the data to check null values.
        column_name (str): The name of the column to check values of.

    Returns:
        df : Original dataframe with cleaned column_name,additional column {column_name}_null_flag indicating if 
        null values are present in column_name
    """
    # Check for null values and make flag True if the column has null value else False
    df = df.withColumn(f'{column_name}_null_flag', when(col(column_name).isNull(), True)
                       .otherwise(False))
    return df

# COMMAND ----------

# MAGIC %md ####function to identify records where the country is Canada,but the state is not within the list of Canadian states/territories.

# COMMAND ----------

def check_invalid_can_state(df, state_column, country_column, valid_country_abbre):
    """
    Checks if the country is Canada but state not belongs to canadian states/territories
    in dataframe column
    
    Args:
        df (spark dataframe): The dataframe containing the data to check invalid canadian states.
        state_column (str)  : state column to check values of.
        country_column(str) : country column to check values of.
        valid_country_abbre (list) : valid canadian country abbreviations

    Returns:
        df : Original dataframe with cleaned column_name,
            additional column invalid_{valid_country_abbre[0].lower()}_{state_column.split("_")[0]}_state_flag indicating if the country_column is in valid_country_abbre but the state_column not belongs to canadian states/territories(from clean.can_states table)
    """
    # Get valid state names and state abbreviations from the can_states table
    valid_states_df = spark.sql(f"SELECT state, state_abbreviation FROM {source_catalog}.clean.can_states")
    valid_states = [row.state.upper() for row in valid_states_df.collect()] + \
                   [row.state_abbreviation.upper() for row in valid_states_df.collect()]

    # Create a flag column for invalid states
    invalid_flag_col = f'invalid_{valid_country_abbre[0].lower()}_{state_column.split("_")[0]}_state_flag'
    
    # make the flag to True and remove the state value if the country is in canada but the state not belongs to canadian states/territories
    df = df.withColumn(
        invalid_flag_col,
        when(
            (upper(col(country_column)).isin(valid_country_abbre)) &  # Check if country is in valid list
            (col(state_column).isNotNull()) &                        # Ensure state is not null
            (~upper(col(state_column)).isin(valid_states)),          # State is not in valid list
            True
        ).otherwise(False)
    ).withColumn(
        state_column,
        when(col(invalid_flag_col), "").otherwise(col(state_column))
    )
    return df

# COMMAND ----------

# MAGIC %md ####function to identify records where the country is Australia,but the state is not within the list of Australian states/territories.

# COMMAND ----------

def check_invalid_aus_state(df, state_column, country_column, valid_country_abbre):
    """
    Checks if the country is Australia but state not belongs to Australian states/territories
    in dataframe column
    
    Args:
        df (spark dataframe): The dataframe containing the data to check invalid australian states.
        state_column (str)  : state column to check values of.
        country_column(str) : country column to check values of.
        valid_country_abbre (list) : valid australian country abbreviations

    Returns:
        df : Original dataframe with cleaned column_name,
            additional column invalid_{valid_country_abbre[0].lower()}_{state_column.split("_")[0]}_state_flag indicating if the country_column is in valid_country_abbre but the state_column not belongs to australian states/territories(from clean.aus_states table)
    """
    # Get valid state names and abbreviations from the aus_states table
    valid_states_df = spark.sql(f"SELECT state, state_abbreviation FROM {source_catalog}.clean.aus_states")
    valid_states = [row.state.upper() for row in valid_states_df.collect()] + \
                   [row.state_abbreviation.upper() for row in valid_states_df.collect()]

    # Create a flag column for invalid states
    invalid_flag_col = f'invalid_{valid_country_abbre[0].lower()}_{state_column.split("_")[0]}_state_flag'
    
    # make the flag to True and remove the state value if the country is in australia but the state not belongs to australian states/territories
    df = df.withColumn(
        invalid_flag_col,
        when(
            (upper(col(country_column)).isin(valid_country_abbre)) &  # Check if country is in valid list
            (col(state_column).isNotNull()) &                        # Ensure state is not null
            (~upper(col(state_column)).isin(valid_states)),          # State is not in valid list
            True
        ).otherwise(False)
    ).withColumn(
        state_column,
        when(col(invalid_flag_col), "").otherwise(col(state_column))
    )
    return df

# COMMAND ----------

# MAGIC %md ####function to identify records where the country is US/USA/United States,but the state is not within the list of American states/territories.

# COMMAND ----------

def check_invalid_us_state(df, state_column, country_column, valid_country_abbre):
    """
    Checks if the country is America but state not belongs to American states/territories
    in dataframe column
    
    Args:
        df (spark dataframe): The dataframe containing the data to check invalid American states.
        state_column (str)  : state column to check values of.
        country_column(str) : country column to check values of.
        valid_country_abbre (list) : valid American country abbreviations

    Returns:
        df : Original dataframe with cleaned column_name,
            additional column invalid_{valid_country_abbre[0].lower()}_{state_column.split("_")[0]}_state_flag indicating if the country_column is in valid_country_abbre but the state_column not belongs to American states/territories(from clean.us_states table)
    """
    # Get valid state names and abbreviations from the us_states table
    valid_states_df = spark.sql(f"SELECT state, state_abbreviation FROM {source_catalog}.clean.us_states")
    valid_states = [row.state.upper() for row in valid_states_df.collect()] + \
                   [row.state_abbreviation.upper() for row in valid_states_df.collect()]

    # Create a flag column for invalid states
    invalid_flag_col = f'invalid_{valid_country_abbre[0].lower()}_{state_column.split("_")[0]}_state_flag'
    
    # make the flag to True and remove the state value if the country is in australia but the state not belongs to US/USA states/territories
    df = df.withColumn(
        invalid_flag_col,
        when(
            (upper(col(country_column)).isin(valid_country_abbre)) &  # Check if country is in valid list
            (col(state_column).isNotNull()) &                        # Ensure state is not null
            (~upper(col(state_column)).isin(valid_states)),          # State is not in valid list
            True
        ).otherwise(False)
    ).withColumn(
        state_column,
        when(col(invalid_flag_col), "").otherwise(col(state_column))
    )
    return df

# COMMAND ----------

# MAGIC %md ####function to identify record which has invalid postal code for the list of countries (not from clean.valid_postal_code_formats table)

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

# MAGIC %md ####function to identify record which has valid postal code for the list of countries (from clean.valid_postal_code_formats table)

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
