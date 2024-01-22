# Databricks notebook source
# DBTITLE 1,Storage info
container_name ="landing-zone"
storage_acct_name = "20231113desa"
external_directory = "external_data/"
assistance = "assistance"
contract = "contract"

# COMMAND ----------

# DBTITLE 1,Load CSV Data
def load_csv_data(container_name:str, storage_acct_name:str, path:str):
    """
    Parameters
        container_name (str): The name of the ADLS Gen2 container where the CSV file is stored.

        storage_acct_name (str): The name of the Azure storage account associated with the container.

        location (str): The relative path to the CSV file within the specified container.
        
    Returns
        DataFrame: A PySpark DataFrame containing the data from the CSV file.
    """
    file_path = f"abfss://{container_name}@{storage_acct_name}.dfs.core.windows.net/usa_spending/{path}"
    return spark.read.options(header='true', inferschema='true').csv(file_path)

