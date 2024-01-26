# Databricks notebook source
# Storage Account Name
storage_acct_name = '20231113desa'

# Raw Data Path
raw_cont_name = 'silver-layer'
file_path = '/usa_spending/contract/*.parquet'

# Defines our base path for raw data
raw_path = f"abfss://{raw_cont_name}@{storage_acct_name}.dfs.core.windows.net{file_path}"

# COMMAND ----------

df = spark.read.parquet(raw_path, header=True)

# COMMAND ----------

df.display()

# COMMAND ----------

df.printSchema()
