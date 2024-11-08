# Databricks notebook source
# MAGIC %md
# MAGIC Take vars from init repository in github

# COMMAND ----------

company_prefix = 'dip'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create catalogs
# MAGIC The destinations catalogs need to be present before creating a use case with template repository

# COMMAND ----------

for env in ['dev', 'stg', 'prd']:
    print(f'Running commands for {env}...')
    spark.sql(f'CREATE CATALOG IF NOT EXISTS {company_prefix}_ml_{env}')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Schemas and prediction tables in the catalogs
# MAGIC Create the schemas and predictions tables for all the environments

# COMMAND ----------

for env in ['dev', 'stg', 'prd']:
    print(f'Running commands for {env}...')
    spark.sql(f'CREATE SCHEMA IF NOT EXISTS {company_prefix}_ml_{env}.ml_usecase_test')
    spark.sql(f'CREATE TABLE IF NOT EXISTS {company_prefix}_ml_{env}.ml_usecase_test.predictions_usecase_test (prediction INT)')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Schemas and prediction tables in the catalogs
# MAGIC Create the schemas and predictions tables for all the environments

# COMMAND ----------

df = spark.read.load('/databricks-datasets/nyctaxi-with-zipcodes/subsampled')
df.display()
for env in ['dev', 'stg', 'prd']:
    print(f'Running commands for {env}...')
    df.write.mode('overwrite').saveAsTable(f'{company_prefix}_ml_{env}.ml_usecase_test.taxi_scoring_sample')

# COMMAND ----------


