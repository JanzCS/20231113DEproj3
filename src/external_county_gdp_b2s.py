# Databricks notebook source
# MAGIC %md
# MAGIC ### County GDP

# COMMAND ----------

# Read in county_gdp.csv

gdp = (
    spark.read.format("csv")
    .option("header", "True")
    .load(
        "abfss://landing-zone@20231113desa.dfs.core.windows.net/usa_spending/external_data/county_gdp.csv"
    )
)

# COMMAND ----------

# Keep relevant columns

gdp = gdp.select("county", "gdp_2019", "gdp_2020", "gdp_2021", "gdp_2022", "state")

# COMMAND ----------

# Read in co-est2020-alldata.csv to join by fips_code

all_data = (
    spark.read.format("csv")
    .option("header", "True")
    .load(
        "abfss://landing-zone@20231113desa.dfs.core.windows.net/usa_spending/external_data/co-est2020-alldata.csv"
    )
)

# COMMAND ----------

# Generate fips_code

from pyspark.sql.functions import concat

all_data = all_data.withColumn("fips_code", concat("state", "county"))

# COMMAND ----------

# Select relevant columns

selected_data = all_data.select("stname", "ctyname", "fips_code")

# COMMAND ----------

# Remove ' County' from county names

from pyspark.sql.functions import regexp_replace

selected_data = selected_data.withColumn(
    "ctyname", regexp_replace("ctyname", " County", "")
)

# COMMAND ----------

# Join with gdp dataframe

gdp_combined = selected_data.join(
    gdp, [gdp.state == selected_data.stname, gdp.county == selected_data.ctyname]
)

# COMMAND ----------

from pyspark.sql.functions import col, regexp_replace
from pyspark.sql.types import IntegerType

# Remove commas and change fields from string to int
gdp_combined = (
    gdp_combined.withColumn(
        "gdp_2019",
        regexp_replace("gdp_2019", ",", "").cast(
            IntegerType()
        ),
    )
    .withColumn(
        "gdp_2020",
        regexp_replace("gdp_2020", ",", "").cast(
            IntegerType()
        ),
    )
    .withColumn(
        "gdp_2021",
        regexp_replace("gdp_2021", ",", "").cast(
            IntegerType()
        ),
    )
    .withColumn(
        "gdp_2022",
        regexp_replace("gdp_2022", ",", "").cast(
            IntegerType()
        ),
    )
)

# COMMAND ----------

silver_cont_name = "silver-layer"
storage_acct_name = "20231113desa"
location_from_container = "project=3/usa_spending/"

external_location = f"abfss://{silver_cont_name}@{storage_acct_name}.dfs.core.windows.net/{location_from_container}external/county/gdp"

# two dataframes for each file type
gdp_combined.repartition(1).write.mode('overwrite').parquet(external_location)
