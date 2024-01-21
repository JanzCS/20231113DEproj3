# Databricks notebook source
# MAGIC %md
# MAGIC ### County GDP

# COMMAND ----------

# Read in county_gdp.csv

gdp = spark.read.format("csv").option('header', 'True').load("abfss://landing-zone@20231113desa.dfs.core.windows.net/usa_spending/external_data/county_gdp.csv")

# COMMAND ----------

# Keep relevant columns

gdp = gdp.select('county', 'gdp_2019', 'gdp_2020', 'gdp_2021', 'gdp_2022', 'state')

# COMMAND ----------

# Read in co-est2020-alldata.csv to join by fips_code

all_data = spark.read.format("csv").option('header', 'True').load("abfss://landing-zone@20231113desa.dfs.core.windows.net/usa_spending/external_data/co-est2020-alldata.csv")

# COMMAND ----------

# Generate fips_code

from pyspark.sql.functions import concat

all_data = all_data.withColumn('fips_code', concat('state', 'county'))

# COMMAND ----------

# Select relevant columns

selected_data = all_data.select('stname', 'ctyname', 'fips_code')

# COMMAND ----------

# Remove ' County' from county names

from pyspark.sql.functions import regexp_replace
selected_data = selected_data.withColumn('ctyname', regexp_replace('ctyname', ' County', ''))

# COMMAND ----------

# Join with gdp dataframe

gdp_combined = selected_data.join(gdp, [gdp.state == selected_data.stname, gdp.county == selected_data.ctyname])

# COMMAND ----------

gdp_combined.display()
