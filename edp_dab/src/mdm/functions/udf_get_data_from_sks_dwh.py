# Databricks notebook source
import json

# COMMAND ----------

def udf_get_data_from_sks_dwh(table_name, sks_dwh_json_secret):
    try:
        config_db = dbutils.secrets.get(scope="key-vault-secret", key=sks_dwh_json_secret)
        required_keys = ["server", "database", "username", "password"]

        if not all(key in config_db for key in required_keys):
            raise KeyError("Some required keys are missing in the secret")

        connection_details = json.loads(config_db)

        server_name = connection_details["server"]
        database_name = connection_details["database"]
        username = connection_details["username"]
        password = connection_details["password"]
        driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
        port = "1433"

        url = f'jdbc:sqlserver://{server_name}:{port};database={database_name};user={username};password={password};encrypt=true;trustServerCertificate=true;hostNameInCertificate=*.database.windows.net;loginTimeout=30;'

        df = (
                spark.read.format("jdbc")
                .option("url",url)
                .option("user", username)
                .option("password", password)
                .option("database", database_name)
                .option("dbtable", table_name)
                .option("driver", driver)
                .load()
        )

        return df

    except Exception as e:
        #logger.error(f"{table_name} - Error occurred while retrieving data from SQL Server - {server_name} : {str(e)}")
        udf_send_email(processing_group_name,'Data Load Failure', str(e), metadata_database_json_secret)
        raise
