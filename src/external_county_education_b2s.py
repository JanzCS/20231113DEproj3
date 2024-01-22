# Databricks notebook source
# load in files from bronze layer
raw_cont_name = "landing-zone"
storage_acct_name = "20231113desa"
location_from_container = "usa_spending/external_data/"
education_file = "Education.csv"
count_pres_file = "countypres_2000-2020.csv"

education_location = f"abfss://{raw_cont_name}@{storage_acct_name}.dfs.core.windows.net/{location_from_container}{education_file}"

education = spark.read.csv(education_location, header=True, inferSchema=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Process education 

# COMMAND ----------

# Select out necessary columns
education_cols = ['Federal Information Processing Standard (FIPS) Code',
 'Less than a high school diploma, 2017-21',
 'High school diploma only, 2017-21',
 "Some college or associate's degree, 2017-21",
 "Bachelor's degree or higher, 2017-21",
 'Percent of adults with less than a high school diploma, 2017-21',
 'Percent of adults with a high school diploma only, 2017-21',
 "Percent of adults completing some college or associate's degree, 2017-21",
 "Percent of adults with a bachelor's degree or higher, 2017-21"]

education = education.select(education_cols)

# COMMAND ----------

from pyspark.sql.functions import lpad, col, regexp_replace
from pyspark.sql.types import IntegerType

# Rename fips code column and add the pad
education = education.withColumnRenamed("Federal Information Processing Standard (FIPS) Code", "fips_code")\
    .withColumn("fips_code", lpad(col("fips_code"), 5, "0"))

# Remove commas and change fields from string to int
education = \
    education.withColumn("Less than a high school diploma, 2017-21", regexp_replace('Less than a high school diploma, 2017-21', ",","").cast(IntegerType())) \
    .withColumn('High school diploma only, 2017-21', regexp_replace('High school diploma only, 2017-21', ",","").cast(IntegerType())) \
    .withColumn("Some college or associate's degree, 2017-21", regexp_replace("Some college or associate's degree, 2017-21", ",","").cast(IntegerType())) \
    .withColumn("Bachelor's degree or higher, 2017-21", regexp_replace("Bachelor's degree or higher, 2017-21", ",","").cast(IntegerType()))

# Rename Columns
education = education.withColumnRenamed('Less than a high school diploma, 2017-21', 'less_than_hs_17-21') \
    .withColumnRenamed('High school diploma only, 2017-21', 'hs_only_17-21') \
    .withColumnRenamed("Some college or associate's degree, 2017-21", "some_college_17-21") \
    .withColumnRenamed("Bachelor's degree or higher, 2017-21", "bach_or_higher_17-21") \
    .withColumnRenamed('Percent of adults with less than a high school diploma, 2017-21', "perc_less_than_hs_17-21") \
    .withColumnRenamed('Percent of adults with a high school diploma only, 2017-21', "perc_hs_only_17-21") \
    .withColumnRenamed("Percent of adults completing some college or associate's degree, 2017-21", "perc_some_college_17-21") \
    .withColumnRenamed("Percent of adults with a bachelor's degree or higher, 2017-21", "perc_bach_or_higher_17-21")

# COMMAND ----------

display(education)
