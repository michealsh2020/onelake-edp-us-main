# Databricks notebook source
from pyspark.sql import SparkSession
import os
import sys
import logging 

# COMMAND ----------

def udf_setup_logging(log_directory:str, log_file_path:str) -> logging.Logger:
    if not os.path.exists(log_directory):
        os.makedirs(log_directory)

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    file_handler = logging.FileHandler(log_file_path, mode='w')
    stream_handler = logging.StreamHandler(sys.stdout)

    file_handler.setLevel(logging.INFO)
    stream_handler.setLevel(logging.INFO)

    formatter = logging.Formatter('[%(asctime)s] [%(levelname)s] [%(funcName)s] : %(message)s')
    file_handler.setFormatter(formatter)
    stream_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    
    return logger
