# Databricks notebook source
from pyspark.sql.functions import (
    monotonically_increasing_id,
    explode,
    sequence,
    to_date,
)

# load in files from bronze layer
silver_cont_name = "silver-layer"
storage_acct_name = "20231113desa"
location_from_container = "project=3/usa_spending/"

assistance_location = f"abfss://{silver_cont_name}@{storage_acct_name}.dfs.core.windows.net/{location_from_container}assistance"
contract_location = f"abfss://{silver_cont_name}@{storage_acct_name}.dfs.core.windows.net/{location_from_container}contract"

# two dataframes for each file type
assistance = spark.read.parquet(assistance_location)
contract = spark.read.parquet(contract_location)

# COMMAND ----------

# Create assistance fact table and its dimensions

# Assistance Fact
assistance_fact = assistance.select(
    "transaction_unique_key",
    "award_unique_key",
    "cfda_number",
    "cfda_title",
    "face_value_of_loan",
    "total_face_value_of_loan",
    "original_loan_subsidy_cost",
    "total_loan_subsidy_cost",
    # "business_types_code",
    # "business_types_description",
    "primary_place_of_performance_scope",
)

# # Business Types Dimension
# business_types_dim = assistance_fact.select(
#     "business_types_code", "business_types_description"
# ).distinct().dropna()
# assistance_fact = assistance_fact.drop("business_types_description")

# CFDA Dimension
cfda_dim = assistance_fact.select("cfda_number", "cfda_title").distinct().dropna()
assistance_fact = assistance_fact.drop("cfda_title")

# Primary Place of Performance Dimension
primary_place_of_performance_scope_dim = (
    assistance_fact.select("primary_place_of_performance_scope").distinct().dropna()
)
primary_place_of_performance_scope_dim = primary_place_of_performance_scope_dim.select(
    "*", monotonically_increasing_id().alias("primary_place_of_performance_scope_code")
)

assistance_fact = assistance_fact.join(
    how="left",
    on="primary_place_of_performance_scope",
    other=primary_place_of_performance_scope_dim,
).drop("primary_place_of_performance_scope")

# COMMAND ----------

# Now Create Transaction Fact Table by joining the shared columns of contract and transaction

shared_cols = list(set(assistance.columns).intersection(contract.columns))
transaction_fact = assistance.select(shared_cols).union(contract.select(shared_cols))

# common dimensions
# transaction date dimensions
beginDate = "2018-10-01"
endDate = "2025-09-30"

(
    spark.sql(
        f"select explode(sequence(to_date('{beginDate}'), to_date('{endDate}'), interval 1 day)) as calendarDate"
    ).createOrReplaceTempView("dates")
)

time_dimension = spark.sql(
    f"""
select
  year(calendarDate) * 10000 + month(calendarDate) * 100 + day(calendarDate) as DateKey,
  CalendarDate,
  year(calendarDate) AS CalendarYear,
  date_format(calendarDate, 'MMMM') as CalendarMonth,
  month(calendarDate) as MonthOfYear,
  date_format(calendarDate, 'EEEE') as CalendarDay,
  dayofweek(calendarDate) AS DayOfWeek,
  case
    when weekday(calendarDate) < 5 then 'Y'
    else 'N'
  end as IsWeekDay,
  dayofmonth(calendarDate) as DayOfMonth,
  case
    when calendarDate = last_day(calendarDate) then 'Y'
    else 'N'
  end as IsLastDayOfMonth,
  weekofyear(calendarDate) as WeekOfYearIso,
  quarter(calendarDate) as QuarterOfYear,
  /* Use fiscal periods needed by organization fiscal calendar */
  case
    when month(calendarDate) >= 10 then year(calendarDate) + 1
    else year(calendarDate)
  end as FiscalYear,
  (month(calendarDate) + 2) % 12 + 1 AS FiscalMonthOctToSep
from
  dates
order by
  calendarDate
  """
)

time_key_df = time_dimension.select("DateKey", "CalendarDate")
transaction_fact = transaction_fact.join(
    time_key_df, transaction_fact.action_date == time_key_df.CalendarDate, "left"
)
transaction_fact = transaction_fact.withColumnRenamed(
    "DateKey", "action_date_key"
).drop("CalendarDate", "action_date", "action_date_fiscal_year")

# Add the new INT date key to fact table for action_date and last_modified_date
transaction_fact = transaction_fact.join(
    time_key_df, transaction_fact.last_modified_date == time_key_df.CalendarDate, "left"
)
transaction_fact = transaction_fact.withColumnRenamed(
    "DateKey", "last_modifed_key"
).drop("CalendarDate", "last_modified_date")

# agency
awarding_agency_list = (
    transaction_fact.select("awarding_agency_code", "awarding_agency_name")
    .withColumnRenamed("awarding_agency_code", "agency_code")
    .withColumnRenamed("awarding_agency_name", "agency_name")
)
funding_agency_list = (
    transaction_fact.select("funding_agency_code", "funding_agency_name")
    .withColumnRenamed("funding_agency_code", "agency_code")
    .withColumnRenamed("funding_agency_name", "agency_name")
)
agency_dim = awarding_agency_list.union(funding_agency_list).distinct().dropna()
transaction_fact = transaction_fact.drop("awarding_agency_name", "funding_agency_name")

# office
awarding_office_list = (
    transaction_fact.select("awarding_office_code", "awarding_office_name")
    .withColumnRenamed("awarding_office_code", "office_code")
    .withColumnRenamed("awarding_office_name", "office_name")
)
funding_office_list = (
    transaction_fact.select("funding_office_code", "funding_office_name")
    .withColumnRenamed("funding_office_code", "office_code")
    .withColumnRenamed("funding_office_name", "office_name")
)
office_dim = awarding_office_list.union(funding_office_list).distinct().dropna()
transaction_fact = transaction_fact.drop("awarding_office_name", "funding_office_name")

# # object_class
# object_class_dim = (
#     transaction_fact.select("object_classes_funding_this_award").distinct().dropna()
# )
# object_class_dim = object_class_dim.select(
#     "*", monotonically_increasing_id().alias("object_class_code")
# # )
# transaction_fact = transaction_fact.join(
#     how="left",
#     on=(
#         transaction_fact.object_classes_funding_this_award
#         == object_class_dim.object_classes_funding_this_award
#     ),
#     other=object_class_dim,
# ).drop("object_classes_funding_this_award")

# recipient - should add recipient location columns to this?
recipient_dim = (
    transaction_fact.select("recipient_uei", "recipient_name_raw").distinct().dropna()
)
transaction_fact = transaction_fact.drop("recipient_name_raw")

# county
recipient_county = (
    transaction_fact.select(
        "prime_award_transaction_recipient_county_fips_code", "recipient_county_name"
    )
    .withColumnRenamed(
        "prime_award_transaction_recipient_county_fips_code", "county_fips"
    )
    .withColumnRenamed("recipient_county_name", "county_name")
)
place_of_performance_county = (
    transaction_fact.select(
        "prime_award_transaction_place_of_performance_county_fips_code",
        "primary_place_of_performance_county_name",
    )
    .withColumnRenamed(
        "prime_award_transaction_place_of_performance_county_fips_code", "county_fips"
    )
    .withColumnRenamed("primary_place_of_performance_county_name", "county_name")
)
county_dim = recipient_county.union(place_of_performance_county).distinct().dropna()
transaction_fact = transaction_fact.drop(
    "recipient_county_name", "primary_place_of_performance_county_name"
)

# Add external data to county dimension
education_location = f"abfss://{silver_cont_name}@{storage_acct_name}.dfs.core.windows.net/{location_from_container}/external/county/education"
gdp_location = f"abfss://{silver_cont_name}@{storage_acct_name}.dfs.core.windows.net/{location_from_container}/external/county/gdp"
pop_est_location = f"abfss://{silver_cont_name}@{storage_acct_name}.dfs.core.windows.net/{location_from_container}/external/county/pop_est"
pres_location = f"abfss://{silver_cont_name}@{storage_acct_name}.dfs.core.windows.net/{location_from_container}/external/county/pres"

education_ext = spark.read.parquet(education_location)
gdp_ext = spark.read.parquet(gdp_location)
pop_est_ext = spark.read.parquet(pop_est_location)
pres_ext = spark.read.parquet(pres_location)

county_dim = county_dim.join(
    education_ext, on=(county_dim.county_fips == education_ext.fips_code)
).drop("fips_code")
county_dim = county_dim.join(
    gdp_ext, on=(county_dim.county_fips == gdp_ext.fips_code)
).drop("fips_code")
county_dim = county_dim.join(
    pop_est_ext, on=(county_dim.county_fips == pop_est_ext.fips_code)
).drop("fips_code")
county_dim = county_dim.join(pres_ext, on="county_fips")
county_dim = county_dim.drop("county_name", "county", "state")

# congressional_district
recipient_cd = transaction_fact.select(
    "prime_award_transaction_recipient_cd_original"
).withColumnRenamed("prime_award_transaction_recipient_cd_original", "cd_original")
place_of_performance_cd = transaction_fact.select(
    "prime_award_transaction_place_of_performance_cd_original"
).withColumnRenamed(
    "prime_award_transaction_place_of_performance_cd_original", "cd_original"
)
combined_cd = recipient_cd.union(place_of_performance_cd).distinct().dropna()
combined_cd = combined_cd.select(
    "*", monotonically_increasing_id().alias("cd_original_code")
)
transaction_fact = (
    transaction_fact.join(
        how="left",
        on=(
            transaction_fact.prime_award_transaction_recipient_cd_original
            == combined_cd.cd_original_code
        ),
        other=combined_cd,
    )
    .withColumnRenamed("cd_original_code", "recipient_cd_code")
    .drop("cd_original")
)
transaction_fact = (
    transaction_fact.join(
        how="left",
        on=(
            transaction_fact.prime_award_transaction_place_of_performance_cd_original
            == combined_cd.cd_original_code
        ),
        other=combined_cd,
    )
    .withColumnRenamed("cd_original_code", "place_of_performance_cd_code")
    .drop("cd_original")
)
transaction_fact = transaction_fact.drop(
    "prime_award_transaction_recipient_cd_original"
)

# Add external data to CD Dimension
cd_location = f"abfss://{silver_cont_name}@{storage_acct_name}.dfs.core.windows.net/{location_from_container}/external/cd/house"
cd_house_ext = spark.read.parquet(cd_location)

combined_cd = combined_cd.join(
    cd_house_ext, on=(combined_cd.cd_original == cd_house_ext.congressional_district)
).drop("congressional_district")

# FINISH THIS - maybe drop current cd?

# state
recipient_state = (
    transaction_fact.select("recipient_state_code", "recipient_state_name")
    .withColumnRenamed("recipient_state_code", "state_code")
    .withColumnRenamed("recipient_state_name", "state_name")
)
place_of_performance_state = (
    transaction_fact.select(
        "prime_award_transaction_place_of_performance_state_fips_code",
        "primary_place_of_performance_state_name",
    )
    .withColumnRenamed(
        "prime_award_transaction_place_of_performance_state_fips_code", "state_fips"
    )
    .withColumnRenamed("primary_place_of_performance_state_name", "state_name")
)
state_dim = recipient_state.union(place_of_performance_state).distinct().dropna()
transaction_fact = transaction_fact.drop(
    "recipient_state_name", "primary_place_of_performance_state_name"
)

# country
recipient_country = (
    transaction_fact.select("recipient_country_code", "recipient_country_name")
    .withColumnRenamed("recipient_country_code", "country_code")
    .withColumnRenamed("recipient_country_name", "country_name")
)
place_of_performance_country = (
    transaction_fact.select(
        "primary_place_of_performance_country_code",
        "primary_place_of_performance_country_name",
    )
    .withColumnRenamed("primary_place_of_performance_country_code", "country_code")
    .withColumnRenamed("primary_place_of_performance_country_name", "country_name")
)
country_dim = recipient_country.union(place_of_performance_country).distinct().dropna()
transaction_fact = transaction_fact.drop(
    "recipient_country_name", "primary_place_of_performance_country_name"
)

# highly_compensated_officer
hco1 = transaction_fact.select("highly_compensated_officer_1_name").withColumnRenamed(
    "highly_compensated_officer_1_name", "hco_name"
)
hco2 = transaction_fact.select("highly_compensated_officer_2_name").withColumnRenamed(
    "highly_compensated_officer_2_name", "hco_name"
)
hco3 = transaction_fact.select("highly_compensated_officer_3_name").withColumnRenamed(
    "highly_compensated_officer_3_name", "hco_name"
)
hco4 = transaction_fact.select("highly_compensated_officer_4_name").withColumnRenamed(
    "highly_compensated_officer_4_name", "hco_name"
)
hco5 = transaction_fact.select("highly_compensated_officer_5_name").withColumnRenamed(
    "highly_compensated_officer_5_name", "hco_name"
)
all_hco = hco1.union(hco2).union(hco3).union(hco4).union(hco5).distinct().dropna()
all_hco = all_hco.select("*", monotonically_increasing_id().alias("hco_code"))

transaction_fact = (
    transaction_fact.join(
        how="left",
        on=(transaction_fact.highly_compensated_officer_1_name == all_hco.hco_name),
        other=all_hco,
    )
    .withColumnRenamed("hco_code", "highly_compensated_officer_1_code")
    .drop("highly_compensated_officer_1_name", "hco_name")
)

transaction_fact = (
    transaction_fact.join(
        how="left",
        on=(transaction_fact.highly_compensated_officer_2_name == all_hco.hco_name),
        other=all_hco,
    )
    .withColumnRenamed("hco_code", "highly_compensated_officer_2_code")
    .drop("highly_compensated_officer_2_name", "hco_name")
)

transaction_fact = (
    transaction_fact.join(
        how="left",
        on=(transaction_fact.highly_compensated_officer_3_name == all_hco.hco_name),
        other=all_hco,
    )
    .withColumnRenamed("hco_code", "highly_compensated_officer_3_code")
    .drop("highly_compensated_officer_3_name", "hco_name")
)

transaction_fact = (
    transaction_fact.join(
        how="left",
        on=(transaction_fact.highly_compensated_officer_4_name == all_hco.hco_name),
        other=all_hco,
    )
    .withColumnRenamed("hco_code", "highly_compensated_officer_4_code")
    .drop("highly_compensated_officer_4_name", "hco_name")
)

transaction_fact = (
    transaction_fact.join(
        how="left",
        on=(transaction_fact.highly_compensated_officer_5_name == all_hco.hco_name),
        other=all_hco,
    )
    .withColumnRenamed("hco_code", "highly_compensated_officer_5_code")
    .drop("highly_compensated_officer_5_name", "hco_name")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Implement partitioning
# MAGIC - Assistance 10 Million rows/ 256 MB
# MAGIC - Transaction 4.3 Million rows/ 256 MB

# COMMAND ----------

import math

assistance_num_rows = assistance_fact.count()
transaction_num_rows = transaction_fact.count()

assistance_num_partitions = math.ceil(assistance_num_rows / 10000000)
transaction_num_partitions = math.ceil(transaction_num_rows / 4300000)

# COMMAND ----------

# Write each table to gold layer
gold_cont_name = "gold-layer"
location_from_container = "project=3/usa_spending/"
location_stub = f"abfss://{gold_cont_name}@{storage_acct_name}.dfs.core.windows.net/{location_from_container}"

assistance_fact_location = location_stub + "assistance"
transaction_fact_location = location_stub + "transaction"

business_types_dim_location = location_stub + "business_types"
cfda_dim_location = location_stub + "cfda"
primary_place_of_performance_scope_dim_location = (
    location_stub + "primary_place_of_performance_scope"
)

time_dim_location = location_stub + "date"
agency_dim_location = location_stub + "agency"
office_dim_location = location_stub + "office"
object_class_dim_location = location_stub + "object_class"
recipient_dim_location = location_stub + "recipient"
county_dim_location = location_stub + "county"
cd_dim_location = location_stub + "cd"
state_dim_location = location_stub + "state"
country_dim_location = location_stub + "country"
hco_dim_location = location_stub + "hco"

# Write each table to gold layer
assistance_fact.repartition(assistance_num_partitions).write.mode('overwrite').parquet(assistance_fact_location)
transaction_fact.repartition(transaction_num_partitions).write.mode('overwrite').parquet(transaction_fact_location)

# business_types_dim.repartition(1).write.parquet(business_types_dim_location)
cfda_dim.repartition(1).write.mode('overwrite').parquet(cfda_dim_location)
primary_place_of_performance_scope_dim.repartition(1).write.mode('overwrite').parquet(
    primary_place_of_performance_scope_dim_location
)

time_dimension.repartition(1).write.mode('overwrite').parquet(time_dim_location)
agency_dim.repartition(1).write.mode('overwrite').parquet(agency_dim_location)
office_dim.repartition(1).write.mode('overwrite').parquet(office_dim_location)
# object_class_dim.repartition(1).write.parquet(object_class_dim_location)
recipient_dim.repartition(1).write.mode('overwrite').parquet(recipient_dim_location)
county_dim.repartition(1).write.mode('overwrite').parquet(county_dim_location)
combined_cd.repartition(1).write.mode('overwrite').parquet(cd_dim_location)
state_dim.repartition(1).write.mode('overwrite').parquet(state_dim_location)
country_dim.repartition(1).write.mode('overwrite').parquet(country_dim_location)
all_hco.repartition(1).write.mode('overwrite').parquet(hco_dim_location)
