# Databricks notebook source
# load in files from bronze layer
raw_cont_name = "landing-zone"
storage_acct_name = "20231113desa"
location_from_container = "usa_spending/external_data/"
count_pres_file = "countypres_2000-2020.csv"

county_pres_location = f"abfss://{raw_cont_name}@{storage_acct_name}.dfs.core.windows.net/{location_from_container}{count_pres_file}"

county_pres = spark.read.csv(county_pres_location, header=True, inferSchema=True)

# COMMAND ----------

from pyspark.sql.functions import lpad
# Change column types
county_pres = county_pres.withColumn('year', county_pres.year.cast('integer'))\
    .withColumn('candidatevotes', county_pres.candidatevotes.cast('integer'))

#Filter to 2016 and 2020 elections
county_pres = county_pres.where((county_pres.year == 2016) | (county_pres.year == 2020))

#Drop columns and duplicates
county_pres = county_pres.drop('office', 'version')
county_pres = county_pres.dropDuplicates()

#Fix county fips code
county_pres = county_pres.withColumn("county_fips", lpad("county_fips", 5, "0"))

# COMMAND ----------

#Retrieve total votes for each candidate per county per election
county_pres = county_pres.groupBy('year', 'state', 'state_po','county_name', 'county_fips','candidate','party')\
    .sum('candidatevotes')
county_pres = county_pres.withColumnRenamed('sum(candidatevotes)', 'totalcandidatevotes')

# COMMAND ----------

#Filter down to only the winner of each election
county_pres_with_maximums = county_pres.groupBy('year', 'state', 'state_po','county_name', 'county_fips').max('totalcandidatevotes')
county_pres_with_maximums = county_pres_with_maximums.withColumnRenamed('max(totalcandidatevotes)', 'totalcandidatevotes')

#Join back to main table to retrieve the candidate info and select out necessary columns
county_pres = county_pres_with_maximums.join(county_pres, ['year', 'county_fips', 'totalcandidatevotes'])
county_pres = county_pres.orderBy('year', 'county_fips')
county_pres = county_pres.select('year', 'county_fips', 'party')

# COMMAND ----------

from pyspark.sql.functions import initcap, col, first

# Convert party to title case
county_pres = county_pres.withColumn('party', initcap(county_pres.party))

#Pivot each year to a column and display which party won each election
county_pres = county_pres.groupBy("county_fips").pivot("year").agg(first("party").alias("party"))
county_pres = county_pres.withColumnRenamed('2016', '2016_party').withColumnRenamed('2020', '2020_party')

# COMMAND ----------

silver_cont_name = "silver-layer"
location_from_container = "project=3/usa_spending/"

external_location = f"abfss://{silver_cont_name}@{storage_acct_name}.dfs.core.windows.net/{location_from_container}external/county/pres"

county_pres.repartition(1).write.mode('overwrite').parquet(external_location)
