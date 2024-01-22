# Databricks notebook source
# MAGIC %run "./functions"

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql import Window

# COMMAND ----------

# DBTITLE 1,Load in CountyPres data
countypres_path = external_directory + "countypres_2000-2020.csv"
country_pres_df = load_csv_data(container_name, storage_acct_name, countypres_path)
display(country_pres_df.limit(5))


# COMMAND ----------

# DBTITLE 1,Load in the Unemployment data
unemployement_path = external_directory + "Unemployment.csv"
unemployement_df = (
    load_csv_data(container_name, storage_acct_name, unemployement_path)
        .select(
            'fips_code', 
            'unemployment_rate_2019', 
            'unemployment_rate_2020', 
            'unemployment_rate_2021',
            'unemployment_rate_2022',
            'median_household_income_2021'
        )
        .filter(col('State') != "US")
)
display(unemployement_df.limit(5))

# COMMAND ----------

# DBTITLE 1,Load the House data
house_path = external_directory + "1976-2022-house.csv"
house_df = (
    load_csv_data(container_name, storage_acct_name, house_path)
        .select(
            'year',
            'state_po',
            'district',
            'candidate',
            'candidatevotes',
            'party'
        )
        .filter(col('year') >= 2018)
        
)
display(house_df.limit(5))

# COMMAND ----------

# DBTITLE 1,Get the winner by year, state, district, candidate, party
winner_df =(
    house_df
        .groupBy('year','state_po','district', 'candidate', 'party')
        .agg(max('candidatevotes').alias('votes'))
        .orderBy('year', 'state_po', 'district', desc('votes'))
        .withColumn(
            'rank',
             rank().over(
                 Window
                    .partitionBy('year','state_po','district')
                    .orderBy(desc('votes'))
             )
        )
        .select(
            'year',
            'state_po',
            'district',
            'candidate',
            'party'
        )
        .filter(col('rank') == 1)
)

display(winner_df.limit(5))

# COMMAND ----------

# DBTITLE 1,create dataframe for the specific year
winner_2018_df = winner_df.filter(col('year') == 2018).withColumnRenamed('party', 'party_2018')
winner_2020_df = winner_df.filter(col('year') == 2020).withColumnRenamed('party', 'party_2020')
winner_2022_df = winner_df.filter(col('year') == 2022).withColumnRenamed('party', 'party_2022')


# COMMAND ----------

display(winner_2018_df)
display(winner_2020_df)
display(winner_2022_df)

# COMMAND ----------

# DBTITLE 1,Drop candidate and year
winner_joined_df = (
    winner_2018_df
        .join(winner_2020_df, ['state_po', 'district'], how="outer")
        .join(winner_2022_df, ['state_po','district'], how="outer")
        .drop('candidate', 'year')
)
winner_joined_df.display()

# COMMAND ----------

from pyspark.sql.functions import concat_ws,col, lpad
winner_joined_df = winner_joined_df.select(concat_ws('-', 'state_po', lpad(col('district'), 2, '0')).alias('congressional_district'), 'party_2018', 'party_2020', 'party_2022')


# COMMAND ----------

silver_cont_name = "silver-layer"
storage_acct_name = "20231113desa"
location_from_container = "usa_spending/"

external_location = f"abfss://{silver_cont_name}@{storage_acct_name}.dfs.core.windows.net/{location_from_container}external/cd/house"

# two dataframes for each file type
winner_joined_df.repartition(1).write.parquet(external_location)
