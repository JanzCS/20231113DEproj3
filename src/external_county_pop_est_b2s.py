# Databricks notebook source
raw_cont_name = "landing-zone"
storage_acct_name = "20231113desa"
location_from_container = "usa_spending/external_data/"
external_data_location = f"abfss://{raw_cont_name}@{storage_acct_name}.dfs.core.windows.net/{location_from_container}"

county_pop_estimates_2010_through_2019 = spark.read.csv(
    external_data_location + "co-est2020-alldata.csv", header=True, inferSchema=True
)
county_pop_estimates_2020_through_2022 = spark.read.csv(
    external_data_location + "co-est2022-alldata.csv", header=True, inferSchema=True
)

# COMMAND ----------

from pyspark.sql.functions import col, concat, lpad
from pyspark.sql.types import StringType

# remove unnecessary columns
county_pop_estimates_2010_through_2019 = county_pop_estimates_2010_through_2019.select(
    "state", "county", "popestimate2019"
)
county_pop_estimates_2020_through_2022 = county_pop_estimates_2020_through_2022.select(
    "state", "county", "popestimate2020", "popestimate2021", "popestimate2022"
)

county_pop_estimates_combined = county_pop_estimates_2010_through_2019.join(
    county_pop_estimates_2020_through_2022, ["state", "county"]
)
county_pop_estimates_combined = (
    county_pop_estimates_combined.withColumn(
        "county", lpad(col("county").cast(StringType()), 3, "0")
    )
    .withColumn("state", lpad(col("state").cast(StringType()), 2, "0"))
    .withColumn("fips_code", concat(col("state"), col("county")))
    .drop("state", "county")
)

# COMMAND ----------

silver_cont_name = "silver-layer"
location_from_container = "project=3/usa_spending/"

external_location = f"abfss://{silver_cont_name}@{storage_acct_name}.dfs.core.windows.net/{location_from_container}external/county/pop_est"

# two dataframes for each file type
county_pop_estimates_combined.repartition(1).write.parquet(external_location)
