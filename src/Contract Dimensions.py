# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# load in files from silver layer
cont_name = "silver-layer"
storage_acct_name = "20231113desa"
location_from_container = "project=3/usa_spending/"

contract_location = f"abfss://{cont_name}@{storage_acct_name}.dfs.core.windows.net/{location_from_container}contract"

contract = spark.read.parquet(contract_location, header=True, inferSchema=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimension tables

# COMMAND ----------

# MAGIC %md
# MAGIC ### Small Business Dimension

# COMMAND ----------

from pyspark.sql.functions import col, when

# Filter to only valid entries S = Small Business, O = Not Small Business
small_business = col("contracting_officers_determination_of_business_size_code").isin(
    ["S"]
)
not_small_business = col(
    "contracting_officers_determination_of_business_size_code"
).isin(["O"])

# Create Primary Key
contract = contract.withColumn(
    "business_size_key",
    when(small_business, 1).when(not_small_business, 2).otherwise(None),
)

# COMMAND ----------

# Select out columns to put in dimension table, rename columns and drop duplicates
business_size = contract.select(
    "business_size_key",
    "contracting_officers_determination_of_business_size_code",
    "contracting_officers_determination_of_business_size",
)

business_size = business_size.withColumnRenamed(
    "contracting_officers_determination_of_business_size_code", "business_size_code"
).withColumnRenamed(
    "contracting_officers_determination_of_business_size", "business_size"
)

business_size = business_size.dropDuplicates()
business_size = business_size.dropna(subset=["business_size_key"])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Humanitarian 

# COMMAND ----------

humanitarian_or_peacekeeping = contract.select(
    "contingency_humanitarian_or_peacekeeping_operation_code",
    "contingency_humanitarian_or_peacekeeping_operation",
)
humanitarian_or_peacekeeping = humanitarian_or_peacekeeping.dropDuplicates().dropna()

# Add surrogate primary key
window_spec = Window.orderBy(
    "contingency_humanitarian_or_peacekeeping_operation_code",
    "contingency_humanitarian_or_peacekeeping_operation",
)
humanitarian_or_peacekeeping = humanitarian_or_peacekeeping.withColumn(
    "humanitarian_or_peacekeeping_key", F.row_number().over(window_spec)
)

# Join new primary key into contract fact table
contract = contract.join(
    humanitarian_or_peacekeeping,
    [
        "contingency_humanitarian_or_peacekeeping_operation_code",
        "contingency_humanitarian_or_peacekeeping_operation",
    ],
)
contract = contract.drop(
    "contingency_humanitarian_or_peacekeeping_operation_code",
    "contingency_humanitarian_or_peacekeeping_operation",
)

# Reorder and rename columns
humanitarian_or_peacekeeping = humanitarian_or_peacekeeping.withColumnRenamed(
    "contingency_humanitarian_or_peacekeeping_operation_code",
    "humanitarian_or_peacekeeping_code",
).withColumnRenamed(
    "contingency_humanitarian_or_peacekeeping_operation",
    "humanitarian_or_peacekeeping_operation",
)
humanitarian_or_peacekeeping = humanitarian_or_peacekeeping.select(
    "humanitarian_or_peacekeeping_key",
    "humanitarian_or_peacekeeping_code",
    "humanitarian_or_peacekeeping_operation",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Place of manufacture 

# COMMAND ----------

# Select out columns and drop duplicates and nulls
place_of_manufacture = contract.select(
    "place_of_manufacture_code", "place_of_manufacture"
)
place_of_manufacture = place_of_manufacture.dropDuplicates().dropna()

# Create surrogate primary key for each unique row
window_spec = Window.orderBy("place_of_manufacture_code", "place_of_manufacture")
place_of_manufacture = place_of_manufacture.withColumn(
    "place_of_manufacture_key", F.row_number().over(window_spec)
)

# Join new surrogate key into fact table and drop columns
contract = contract.join(
    place_of_manufacture, ["place_of_manufacture_code", "place_of_manufacture"]
)
contract = contract.drop("place_of_manufacture_code", "place_of_manufacture")

# Reorder columns
place_of_manufacture = place_of_manufacture.select(
    "place_of_manufacture_key", "place_of_manufacture_code", "place_of_manufacture"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Domestic or foreign entity

# COMMAND ----------

# Select out columns and drop duplicates and nulls
domestic_or_foreign_entity = contract.select(
    "domestic_or_foreign_entity_code", "domestic_or_foreign_entity"
)
domestic_or_foreign_entity = domestic_or_foreign_entity.dropDuplicates().dropna()

# Create a new surroage primary key
window_spec = Window.orderBy(
    "domestic_or_foreign_entity_code", "domestic_or_foreign_entity"
)
domestic_or_foreign_entity = domestic_or_foreign_entity.withColumn(
    "domestic_or_foreign_entity_key", F.row_number().over(window_spec)
)

# Join new surrogate key into fact table and drop columns
contract = contract.join(
    domestic_or_foreign_entity,
    ["domestic_or_foreign_entity_code", "domestic_or_foreign_entity"],
)
contract = contract.drop(
    "domestic_or_foreign_entity_code", "domestic_or_foreign_entity"
)

# Reorder columns
domestic_or_foreign_entity = domestic_or_foreign_entity.select(
    "domestic_or_foreign_entity_key",
    "domestic_or_foreign_entity_code",
    "domestic_or_foreign_entity",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### NAICS Table

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Get unique rows and drop null values
naics = contract.select("naics_code", "naics_description").dropDuplicates()
naics = naics.dropna(subset=["naics_code"])

# Create a new column 'naics_key' with a monotonically increasing ID starting from 1
window_spec = Window.orderBy("naics_code", "naics_description")
naics = naics.withColumn("naics_key", F.row_number().over(window_spec))

# Reorder columns
naics = naics.select("naics_key", "naics_code", "naics_description")

# Join new unique key into fact table and remove columns
contract = contract.join(naics, ["naics_code", "naics_description"])
contract = contract.drop("naics_code", "naics_description")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Country of origin

# COMMAND ----------

from pyspark.sql.functions import col

# Select out columns and drop duplicates and nulls
country_of_origin = contract.select(
    "country_of_product_or_service_origin_code", "country_of_product_or_service_origin"
)
country_of_origin = country_of_origin.dropDuplicates().dropna(how="any")

# Create a surrogate primary key
window_spec = Window.orderBy(
    col("country_of_product_or_service_origin_code").desc(),
    "country_of_product_or_service_origin",
)
country_of_origin = country_of_origin.withColumn(
    "country_of_origin_key", F.row_number().over(window_spec)
)

# Join to main fact table to retrieve new surrogate key and drop old columns
contract = contract.join(
    country_of_origin,
    [
        "country_of_product_or_service_origin_code",
        "country_of_product_or_service_origin",
    ],
)
contract = contract.drop(
    "country_of_product_or_service_origin_code", "country_of_product_or_service_origin"
)

# Rename and Reorder columns
country_of_origin = country_of_origin.withColumnRenamed(
    "country_of_product_or_service_origin_code", "country_of_origin_code"
).withColumnRenamed("country_of_product_or_service_origin", "country_of_origin")
country_of_origin = country_of_origin.select(
    "country_of_origin_key", "country_of_origin_code", "country_of_origin"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Foreign Funding Dimension

# COMMAND ----------

# Select out foreign funding columns and drop duplicates and nulls
foreign_funding = contract.select("foreign_funding", "foreign_funding_description")
foreign_funding = foreign_funding.dropDuplicates().dropna()

# Create a surrogate primary key
window_spec = Window.orderBy("foreign_funding", "foreign_funding_description")
foreign_funding = foreign_funding.withColumn(
    "foreign_funding_key", F.row_number().over(window_spec)
)

# Join contract with foreign_funding dataframe to retrieve surrogate key and drop columns
contract = contract.join(
    foreign_funding, ["foreign_funding", "foreign_funding_description"]
)
contract = contract.drop("foreign_funding", "foreign_funding_description")

# Rename and reorder columns
foreign_funding = foreign_funding.withColumnRenamed(
    "foreign_funding", "foreign_funding_code"
).select("foreign_funding_key", "foreign_funding_code", "foreign_funding_description")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Product or Service dimension

# COMMAND ----------

# Select out columns and drop duplicates
product_or_service = contract.select(
    "product_or_service_code", "product_or_service_code_description"
)
product_or_service = product_or_service.dropDuplicates().dropna()

# Create a surrogate primary key
window_spec = Window.orderBy(
    "product_or_service_code", "product_or_service_code_description"
)
product_or_service = product_or_service.withColumn(
    "product_or_service_key", F.row_number().over(window_spec)
)

# Join surrogate key into fact table and drop columns
contract = contract.join(
    product_or_service,
    ["product_or_service_code", "product_or_service_code_description"],
)
contract = contract.drop(
    "product_or_service_code", "product_or_service_code_description"
)

# Reorder columns
product_or_service = product_or_service.select(
    "product_or_service_key",
    "product_or_service_code",
    "product_or_service_code_description",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Select out contract fact table columns of all of the foreign keys 

# COMMAND ----------

contract = contract.select(
    "transaction_unique_key",
    "naics_key",
    "place_of_manufacture_key",
    "country_of_origin_key",
    "humanitarian_or_peacekeeping_key",
    "business_size_key",
    "foreign_funding_key",
    "product_or_service_key",
    "domestic_or_foreign_entity_key",
)

# COMMAND ----------

# Write each table to gold layer
gold_cont_name = "gold-layer"
storage_acct_name = "20231113desa"
location_from_container = "project=3/usa_spending/"
location_stub = f"abfss://{gold_cont_name}@{storage_acct_name}.dfs.core.windows.net/{location_from_container}"

contract_fact_location = location_stub + "contract"

business_size_dim_location = location_stub + "business_size"
humanitarian_or_peacekeeping_dim_location = (
    location_stub + "humanitarian_or_peacekeeping"
)
place_of_manufacture_dim_location = location_stub + "place_of_manufacture"
domestic_or_foreign_entity_dim_location = location_stub + "domestic_or_foreign_entity"
naics_dim_location = location_stub + "naics"
country_of_origin_dim_location = location_stub + "country_of_origin"
foreign_funding_dim_location = location_stub + "foreign_funding"
product_or_service_dim_location = location_stub + "product_or_service"

# Write each table to gold layer
contract.repartition(1).write.parquet(contract_fact_location)
business_size.repartition(1).write.parquet(business_size_dim_location)
humanitarian_or_peacekeeping.repartition(1).write.parquet(
    humanitarian_or_peacekeeping_dim_location
)
place_of_manufacture.repartition(1).write.parquet(place_of_manufacture_dim_location)
domestic_or_foreign_entity.repartition(1).write.parquet(
    domestic_or_foreign_entity_dim_location
)
naics.repartition(1).write.parquet(naics_dim_location)
country_of_origin.repartition(1).write.parquet(country_of_origin_dim_location)
foreign_funding.repartition(1).write.parquet(foreign_funding_dim_location)
product_or_service.repartition(1).write.parquet(product_or_service_dim_location)
