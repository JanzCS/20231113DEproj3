# Databricks notebook source
# Storage Account Name
storage_acct_name = '20231113desa'

# Raw Data Path
raw_cont_name = 'landing-zone'
file_path = '/usa_spending/external_data/countypres_2000-2020.csv'

# Silver Container Path
silver_cont_name = 'silver-layer'
silver_dest = '/Project=2/Team=2/GitHub Event Archive/'

# Defines our base path for raw data
raw_path = f"abfss://{raw_cont_name}@{storage_acct_name}.dfs.core.windows.net{file_path}"

# COMMAND ----------

df = spark.read.csv(raw_path, header=True)

# COMMAND ----------

df = df.withColumn('year', df.year.cast('integer')).withColumn('candidatevotes', df.candidatevotes.cast('integer'))

# COMMAND ----------

df = df.where((df.year == 2016) | (df.year == 2020))

# COMMAND ----------

df = df.drop('office', 'version')

# COMMAND ----------

df = df.dropDuplicates()

# COMMAND ----------

from pyspark.sql.functions import expr

df = df.withColumn("county_fips", expr("CASE WHEN length(county_fips) < 5 THEN concat('0', county_fips) ELSE county_fips END"))

# COMMAND ----------

df = df.groupBy('year', 'state', 'state_po','county_name', 'county_fips','candidate','party').sum('candidatevotes')

# COMMAND ----------

df = df.withColumnRenamed('sum(candidatevotes)', 'totalcandidatevotes')

# COMMAND ----------

df_with_maximums = df.groupBy('year', 'state', 'state_po','county_name', 'county_fips').max('totalcandidatevotes')

# COMMAND ----------

df_with_maximums = df_with_maximums.withColumnRenamed('max(totalcandidatevotes)', 'totalcandidatevotes')

# COMMAND ----------

display(df_with_maximums.orderBy('year','county_fips'))

# COMMAND ----------

df = df_with_maximums.join(df, ['year', 'county_fips', 'totalcandidatevotes'])

# COMMAND ----------

df = df.orderBy('year', 'county_fips')

# COMMAND ----------

df = df.select('year', 'county_fips', 'party')

# COMMAND ----------

from pyspark.sql.functions import initcap

df = df.withColumn('party', initcap(df.party))
