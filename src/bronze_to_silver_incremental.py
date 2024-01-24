# Databricks notebook source
dbutils.widgets.text("year", "2023")

# COMMAND ----------

year = dbutils.widgets.get("year")

# COMMAND ----------

# load in files from bronze layer
raw_cont_name = "landing-zone"
storage_acct_name = "20231113desa"
location_from_container = "usa_spending/"

assistance_location = f"abfss://{raw_cont_name}@{storage_acct_name}.dfs.core.windows.net/{location_from_container}assistance/FY{year}*"
contract_location = f"abfss://{raw_cont_name}@{storage_acct_name}.dfs.core.windows.net/{location_from_container}contract/FY{year}*"

# two dataframes for each file type
assistance = spark.read.csv(assistance_location, header=True, inferSchema=True)
contract = spark.read.csv(contract_location, header=True, inferSchema=True)

# external data processing
# remove columns, rename, rorder
# make sure tables are dimensionalized

# COMMAND ----------

wanted_cols = [
    "contract_transaction_unique_key",
    "contract_award_unique_key",
    "assistance_transaction_unique_key",
    "assistance_award_unique_key",
    "modification_number",
    "federal_action_obligation",
    "total_obligated_amount",
    "total_dollars_obligated",
    "total_outlayed_amount_for_overall_award",
    "face_value_of_loan",
    "original_loan_subsidy_cost",
    "total_face_value_of_loan",
    "total_loan_subsidy_cost",
    "outlayed_amount_from_COVID-19_supplementals_for_overall_award",
    "obligated_amount_from_COVID-19_supplementals_for_overall_award",
    "action_date",
    "action_date_fiscal_year",
    "awarding_agency_code",
    "awarding_agency_name",
    "awarding_office_code",
    "awarding_office_name",
    "funding_agency_code",
    "funding_agency_name",
    "funding_office_code",
    "funding_office_name",
    "program_activities_funding_this_award",
    "recipient_uei",
    "recipient_name_raw",
    "recipient_parent_uei",
    "recipient_parent_name_raw",
    "recipient_country_code",
    "recipient_country_name",
    "recipient_city_code",
    "recipient_city_name",
    "prime_award_transaction_recipient_county_fips_code",
    "recipient_county_name",
    "prime_award_transaction_recipient_state_fips_code",
    "recipient_state_code",
    "recipient_state_name",
    "prime_award_transaction_recipient_cd_original",
    "primary_place_of_performance_scope",
    "primary_place_of_performance_country_code",
    "primary_place_of_performance_country_name",
    "prime_award_transaction_place_of_performance_county_fips_code",
    "primary_place_of_performance_county_name",
    "prime_award_transaction_place_of_performance_state_fips_code",
    "primary_place_of_performance_state_name",
    "prime_award_transaction_place_of_performance_cd_original",
    "prime_award_transaction_place_of_performance_cd_current",
    "cfda_number",
    "cfda_title",
    "naics_code",
    "naics_description",
    "country_of_product_or_service_origin_code",
    "country_of_product_or_service_origin",
    "contracting_officers_determination_of_business_size",
    "contracting_officers_determination_of_business_size_code",
    "correction_delete_indicator_code",
    "correction_delete_indicator_description",
    "action_type_code",
    "action_type_description",
    "record_type_code",
    "record_type_description",
    "foreign_funding",
    "highly_compensated_officer_1_name",
    "highly_compensated_officer_1_amount",
    "highly_compensated_officer_2_name",
    "highly_compensated_officer_2_amount",
    "highly_compensated_officer_3_name",
    "highly_compensated_officer_3_amount",
    "highly_compensated_officer_4_name",
    "highly_compensated_officer_4_amount",
    "highly_compensated_officer_5_name",
    "highly_compensated_officer_5_amount",
    "last_modified_date",
]

# COMMAND ----------

# remove any unecessary columns from each dataframe
assistance = assistance.select(
    [col for col in wanted_cols if col in assistance.columns]
)
contract = contract.select([col for col in wanted_cols if col in contract.columns])

# COMMAND ----------

from pyspark.sql.functions import lit

# rename? (make sure that if columns are shared you rename to the same name)
assistance = (
    assistance.withColumnRenamed(
        "assistance_transaction_unique_key", "transaction_unique_key"
    )
    .withColumnRenamed("assistance_award_unique_key", "award_unique_key")
    .withColumn("award_type", lit("assistance"))
)
contract = (
    contract.withColumnRenamed(
        "contract_transaction_unique_key", "transaction_unique_key"
    )
    .withColumnRenamed("total_dollars_obligated", "total_obligated_amount")
    .withColumnRenamed("contract_award_unique_key", "award_unique_key")
    .withColumn("award_type", lit("contract"))
)

# COMMAND ----------

from pyspark.sql.types import DecimalType
from pyspark.sql.functions import col

# Fix monetary value columns to be decimal types

assistance = (
    assistance.withColumn(
        "federal_action_obligation",
        col("federal_action_obligation").cast(DecimalType(18, 2)),
    )
    .withColumn(
        "total_obligated_amount", col("total_obligated_amount").cast(DecimalType(18, 2))
    )
    .withColumn(
        "total_outlayed_amount_for_overall_award",
        col("total_outlayed_amount_for_overall_award").cast(DecimalType(18, 2)),
    )
    .withColumn(
        "face_value_of_loan", col("face_value_of_loan").cast(DecimalType(18, 2))
    )
    .withColumn(
        "original_loan_subsidy_cost",
        col("original_loan_subsidy_cost").cast(DecimalType(18, 2)),
    )
    .withColumn(
        "total_face_value_of_loan",
        col("total_face_value_of_loan").cast(DecimalType(18, 2)),
    )
    .withColumn(
        "total_loan_subsidy_cost",
        col("total_loan_subsidy_cost").cast(DecimalType(18, 2)),
    )
    .withColumn(
        "highly_compensated_officer_1_amount",
        col("highly_compensated_officer_1_amount").cast(DecimalType(18, 2)),
    )
    .withColumn(
        "highly_compensated_officer_2_amount",
        col("highly_compensated_officer_2_amount").cast(DecimalType(18, 2)),
    )
    .withColumn(
        "highly_compensated_officer_3_amount",
        col("highly_compensated_officer_3_amount").cast(DecimalType(18, 2)),
    )
    .withColumn(
        "highly_compensated_officer_4_amount",
        col("highly_compensated_officer_4_amount").cast(DecimalType(18, 2)),
    )
    .withColumn(
        "highly_compensated_officer_5_amount",
        col("highly_compensated_officer_5_amount").cast(DecimalType(18, 2)),
    )
)

contract = (
    contract.withColumn(
        "federal_action_obligation",
        col("federal_action_obligation").cast(DecimalType(18, 2)),
    )
    .withColumn(
        "total_obligated_amount", col("total_obligated_amount").cast(DecimalType(18, 2))
    )
    .withColumn(
        "total_outlayed_amount_for_overall_award",
        col("total_outlayed_amount_for_overall_award").cast(DecimalType(18, 2)),
    )
    .withColumn(
        "outlayed_amount_from_COVID-19_supplementals_for_overall_award",
        col("outlayed_amount_from_COVID-19_supplementals_for_overall_award").cast(
            DecimalType(18, 2)
        ),
    )
    .withColumn(
        "obligated_amount_from_COVID-19_supplementals_for_overall_award",
        col("obligated_amount_from_COVID-19_supplementals_for_overall_award").cast(
            DecimalType(18, 2)
        ),
    )
    .withColumn(
        "highly_compensated_officer_1_amount",
        col("highly_compensated_officer_1_amount").cast(DecimalType(18, 2)),
    )
    .withColumn(
        "highly_compensated_officer_2_amount",
        col("highly_compensated_officer_2_amount").cast(DecimalType(18, 2)),
    )
    .withColumn(
        "highly_compensated_officer_3_amount",
        col("highly_compensated_officer_3_amount").cast(DecimalType(18, 2)),
    )
    .withColumn(
        "highly_compensated_officer_4_amount",
        col("highly_compensated_officer_4_amount").cast(DecimalType(18, 2)),
    )
    .withColumn(
        "highly_compensated_officer_5_amount",
        col("highly_compensated_officer_5_amount").cast(DecimalType(18, 2)),
    )
)

# Drop any rows that have no monetary value columns or a primary key

assistance = assistance.dropna(
    how="all",
    subset=[
        "federal_action_obligation",
        "total_obligated_amount",
        "total_outlayed_amount_for_overall_award",
        "face_value_of_loan",
        "original_loan_subsidy_cost",
        "total_face_value_of_loan",
        "total_loan_subsidy_cost",
        "highly_compensated_officer_1_amount",
        "highly_compensated_officer_2_amount",
        "highly_compensated_officer_3_amount",
        "highly_compensated_officer_4_amount",
        "highly_compensated_officer_5_amount",
    ],
)

contract = contract.dropna(
    how="all",
    subset=[
        "federal_action_obligation",
        "total_obligated_amount",
        "total_outlayed_amount_for_overall_award",
        "outlayed_amount_from_COVID-19_supplementals_for_overall_award",
        "obligated_amount_from_COVID-19_supplementals_for_overall_award",
        "highly_compensated_officer_1_amount",
        "highly_compensated_officer_2_amount",
        "highly_compensated_officer_3_amount",
        "highly_compensated_officer_4_amount",
        "highly_compensated_officer_5_amount",
    ],
)

assistance = assistance.dropna(subset=["transaction_unique_key"])
contract = contract.dropna(subset=["transaction_unique_key"])

assistance_expr = '.+_.+_.+_.+_.+'
contract_expr = '.+_.+_.+_.+_.+_.+'
assistance = assistance.filter(assistance['transaction_unique_key'].rlike(assistance_expr))
contract = contract.filter(contract['transaction_unique_key'].rlike(contract_expr))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read in the currrent silver layer and union it with the new year of data

# COMMAND ----------

# Set up locations to write to silver layer
silver_cont_name = "silver-layer"
location_from_container = "project=3/usa_spending/"

assistance_location = f"abfss://{silver_cont_name}@{storage_acct_name}.dfs.core.windows.net/{location_from_container}assistance/abc"
contract_location = f"abfss://{silver_cont_name}@{storage_acct_name}.dfs.core.windows.net/{location_from_container}contract/abc"

silver_data_exists = True
# two dataframes for each file type
try:
    assistance_current_silver = spark.read.parquet(assistance_location, header=True, inferSchema=True)
    contract_current_silver = spark.read.parquet(contract_location, header=True, inferSchema=True)
except:
    silver_data_exists = False

if (silver_data_exists):
    assistance = assistance_current_silver.union(assistance).distinct()
    contract = contract_current_silver.union(contract).distinct()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write all of the data to the silver layer

# COMMAND ----------

# Set up locations to write to silver layer
silver_cont_name = "silver-layer"
location_from_container = "project=3/usa_spending/"

assistance_location = f"abfss://{silver_cont_name}@{storage_acct_name}.dfs.core.windows.net/{location_from_container}assistance"
contract_location = f"abfss://{silver_cont_name}@{storage_acct_name}.dfs.core.windows.net/{location_from_container}contract"

# Dataframe for each file type
assistance.repartition(1).write.mode('overwrite').parquet(assistance_location)
contract.repartition(1).write.mode('overwrite').parquet(contract_location)
