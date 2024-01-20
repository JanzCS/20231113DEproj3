# Databricks notebook source
# load in files from silver layer
cont_name = "silver-layer"
storage_acct_name = "20231113desa"
location_from_container = "usa_spending/"

contract_location = f"abfss://{cont_name}@{storage_acct_name}.dfs.core.windows.net/{location_from_container}contract"

contract = spark.read.parquet(contract_location, header=True, inferSchema=True)

# COMMAND ----------

display(contract)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Select out contract cols

# COMMAND ----------

contract.select('transaction_unique_key', 
                'naics_code', 
                'place_of_manufacture_code',
                'national_interest_action_code', 
                'country_of_product_or_service_origin_code',
                'contingency_humanitarian_or_peacekeeping_operation_code',
                'contracting_officers_determination_of_business_size_code',
                'foreign_funding',
                'product_or_service_code',
                'domestic_or_foreign_entity_code').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimension tables

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Small Business Dimension

# COMMAND ----------

from pyspark.sql.functions import col, when

#Filter to only valid entries S = Small Business, O = Not Small Business
small_business = (col('contracting_officers_determination_of_business_size_code').isin(['S']))
not_small_business = (col('contracting_officers_determination_of_business_size_code').isin(['O']))

#Create Primary Key
contract = contract.withColumn('business_size_key', 
                                         when(small_business, 1)
                                         .when(not_small_business, 2)
                                         .otherwise(None))

# COMMAND ----------

#Select out columns to put in dimension table, rename columns and drop duplicates
business_size = contract.select('business_size_key',  
                                'contracting_officers_determination_of_business_size_code',      
                                'contracting_officers_determination_of_business_size')

business_size = business_size\
    .withColumnRenamed('contracting_officers_determination_of_business_size_code', 'business_size_code')\
    .withColumnRenamed('contracting_officers_determination_of_business_size', 'business_size')

business_size = business_size.dropDuplicates()
business_size = business_size.dropna(subset=['business_size_key'])

# COMMAND ----------

business_size.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Humanitarian 

# COMMAND ----------

humanitarian_or_peacekeeping = contract.select('contingency_humanitarian_or_peacekeeping_operation_code', 'contingency_humanitarian_or_peacekeeping_operation')
humanitarian_or_peacekeeping.groupBy('contingency_humanitarian_or_peacekeeping_operation_code', 'contingency_humanitarian_or_peacekeeping_operation').count().display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Place of manufacture 

# COMMAND ----------

place_of_manufacture = contract.select('place_of_manufacture_code', 'place_of_manufacture')
place_of_manufacture.groupBy('place_of_manufacture_code', 'place_of_manufacture').count().display()
#display(place_of_manufacture)

# COMMAND ----------

domestic_or_foreign_entity = contract.select('domestic_or_foreign_entity_code', 'domestic_or_foreign_entity')
display(domestic_or_foreign_entity)

# COMMAND ----------

naics = contract.select('naics_code', 'naics_description')
display(naics)

# COMMAND ----------

country_of_origin = contract.select('country_of_product_or_service_origin_code','country_of_product_or_service_origin')
display(country_of_origin)

# COMMAND ----------

foreign_funding = contract.select('foreign_funding', 'foreign_funding_description')
display(foreign_funding)

# COMMAND ----------

product_or_service = contract.select('product_or_service_code', 'product_or_service_code_description')
display(product_or_service)

# COMMAND ----------

cols = ['action_date',
 'action_date_fiscal_year',
 'action_type_code',
 'awarding_office_code',
 'awarding_office_name',
 'awarding_sub_agency_code',
 'awarding_sub_agency_name',
 'country_of_product_or_service_origin',
 'country_of_product_or_service_origin_code',
 'disaster_emergency_fund_codes_for_overall_award',
 'funding_office_code',
 'funding_office_name',
 'funding_sub_agency_code',
 'funding_sub_agency_name',
 'last_modified_date',
 'modification_number',
 'object_classes_funding_this_award',
 'obligated_amount_from_COVID-19_supplementals_for_overall_award',
 'outlayed_amount_from_COVID-19_supplementals_for_overall_award',
 'primary_place_of_performance_city_name',
 'primary_place_of_performance_country_code',
 'primary_place_of_performance_country_name',
 'primary_place_of_performance_county_name',
 'primary_place_of_performance_state_name',
 'primary_place_of_performance_zip_4',
 'prime_award_transaction_place_of_performance_cd_current',
 'prime_award_transaction_place_of_performance_cd_original',
 'prime_award_transaction_place_of_performance_county_fips_code',
 'prime_award_transaction_place_of_performance_state_fips_code',
 'prime_award_transaction_recipient_cd_current',
 'prime_award_transaction_recipient_cd_original',
 'prime_award_transaction_recipient_county_fips_code',
 'prime_award_transaction_recipient_state_fips_code',
 'program_activities_funding_this_award',
 'recipient_city_name',
 'recipient_country_code',
 'recipient_country_name',
 'recipient_county_name',
 'recipient_name_raw',
 'recipient_parent_name_raw',
 'recipient_parent_uei',
 'recipient_state_code',
 'recipient_state_name',
 'recipient_uei'
 ]

# COMMAND ----------


