# Databricks notebook source
# MAGIC %md
# MAGIC # ML Use Case Setup Notebook
# MAGIC
# MAGIC This notebook is a **prerequisite** for initializing and configuring a new ML use case before deployment. It ensures that the required **catalogs, schemas, and inference tables** are created and properly set up in **staging and production** environments.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 1. Databricks Environment Consistency
# MAGIC - Ensure that the **development cluster** runs the same **Databricks Runtime** version as the one configured in the workflows.
# MAGIC - Mismatched runtime versions can cause unexpected issues.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 2. Catalog and Schema Initialization
# MAGIC This notebook initializes the following catalogs to store ML inference data:
# MAGIC
# MAGIC - **Production Catalog:** `production_catalog`
# MAGIC - **Staging Catalog:** `staging_catalog`
# MAGIC - **Development Catalog:** `dev_catalog`
# MAGIC
# MAGIC It also creates the **schema** `schema_name` and the **inference input example table** `table_name` in each environment. Which need to be created before deploying the new use case.
# MAGIC
# MAGIC 📌 **Note:** The table name should match the value assigned to `table_name` in this notebook and the yml file `<ml-use-case-name>/resources/batch-inference-workflow-resource.yml`
# MAGIC For example:  `${var.catalog_name}.<ml-use-case-schema-name>.inference_input_example`
# MAGIC
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 3. GitHub Workflow Configuration
# MAGIC - Modify the GitHub workflow file:  
# MAGIC   `.github/workflows/<ml-use-case-name>-run-tests.yml`
# MAGIC - On **line 28**, wrap the Python version in double quotes:
# MAGIC   ```yaml
# MAGIC   "3.10"  # Instead of 3.10
# MAGIC   ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 4. Databricks Permissions
# MAGIC - Grant **editor permissions** to the appropriate **service principals**, this can be done via the Catalog UI in permissions tab.:
# MAGIC   
# MAGIC   **Staging Permissions:**
# MAGIC   - `DATABRICKS_STAGING_CLIENT_SECRET` → `DATABRICKS_TEST_CATALOG_NAME` and `DATABRICKS_STAGING_CATALOG_NAME`
# MAGIC
# MAGIC   **Production Permissions:**
# MAGIC   - `DATABRICKS_PROD_CLIENT_SECRET` → `DATABRICKS_PROD_CATALOG_NAME`
# MAGIC
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 5. Deployment Process
# MAGIC - Before deploying a new ML use case, run this notebook to ensure the required catalogs, schemas, and tables exist.
# MAGIC - Deploy your branch to **staging** before proceeding to production.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Next steps
# MAGIC Once this notebook has been executed successfully:
# MAGIC 1. Validate that the catalogs and schemas have been created in Databricks.
# MAGIC 2. Validate the resource YAML files to reflect the correct `input_table_name` for inference jobs.
# MAGIC 3. Proceed with deploying and testing your ML use case.
# MAGIC
# MAGIC

# COMMAND ----------

from datetime import timedelta, timezone
import math
import mlflow.pyfunc
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType

# COMMAND ----------

# This functions are the functions used to create the an input example table for inference according the template.
def rounded_unix_timestamp(dt, num_minutes=15):
    """
    Ceilings datetime dt to interval num_minutes, then returns the unix timestamp.
    """
    nsecs = dt.minute * 60 + dt.second + dt.microsecond * 1e-6
    delta = math.ceil(nsecs / (60 * num_minutes)) * (60 * num_minutes) - nsecs
    return int((dt + timedelta(seconds=delta)).replace(tzinfo=timezone.utc).timestamp())


rounded_unix_timestamp_udf = F.udf(rounded_unix_timestamp, IntegerType())


def rounded_taxi_data(taxi_data_df):
    # Round the taxi data timestamp to 15 and 30 minute intervals so we can join with the pickup and dropoff features
    # respectively.
    taxi_data_df = (
        taxi_data_df.withColumn(
            "rounded_pickup_datetime",
            F.to_timestamp(
                rounded_unix_timestamp_udf(
                    taxi_data_df["tpep_pickup_datetime"], F.lit(15)
                )
            ),
        )
        .withColumn(
            "rounded_dropoff_datetime",
            F.to_timestamp(
                rounded_unix_timestamp_udf(
                    taxi_data_df["tpep_dropoff_datetime"], F.lit(30)
                )
            ),
        )
        .drop("tpep_pickup_datetime")
        .drop("tpep_dropoff_datetime")
    )
    taxi_data_df.createOrReplaceTempView("taxi_data")
    return taxi_data_df

# Load raw data
df = spark.read.format('delta').load('/databricks-datasets/nyctaxi-with-zipcodes/subsampled')
# sample data
tmp_df = df.sample(fraction=0.01, seed=1)
# Prepare for prediction
new_tmp_df = rounded_taxi_data(tmp_df)
new_tmp_df.cache()
new_tmp_df.display()

# COMMAND ----------

production_catalog = 'dip_mlops_prd'
staging_catalog = 'dip_mlops_stg'
dev_catalog = 'dip_mlops_dev'

catalogs = [production_catalog, staging_catalog, dev_catalog]

schema_name = 'use_case_02'
table_name = 'inference_input_example'

# COMMAND ----------

for catalog in catalogs:
    print(f'Running commands for catalog \'{catalog}\':')
    
    # Create Catalog
    query_i = f'CREATE CATALOG IF NOT EXISTS {catalog}'
    print(f'\tCreating catalog: \'{query_i}\'')
    spark.sql(query_i)

    # Create Schema
    query_i = f'CREATE SCHEMA IF NOT EXISTS {catalog}.{schema_name}'
    print(f'\tCreating schema: \'{query_i}\'')
    spark.sql(query_i)
    
    # Create input example table for inference, according the default template
    print(f'\tCreating dummy input table for inference at {catalog}.{schema_name}.{table_name}')
    new_tmp_df.write.saveAsTable(f'{catalog}.{schema_name}.{table_name}')

# COMMAND ----------

# MAGIC %md