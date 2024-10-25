# Databricks notebook source
# Using Databricks Runtime 15.2 ML
import sys

import plotly.express as px
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

from ydata_profiling import ProfileReport
from databricks.connect import DatabricksSession

%matplotlib inline

sys.path.append("../helpers")
from readers import read_from_volume

# COMMAND ----------

spark = DatabricksSession.builder.getOrCreate()

# COMMAND ----------

df = read_from_volume(spark, "/databricks-datasets/nyctaxi-with-zipcodes/subsampled", as_pandas=True)
output_file = "nyc.parquet"

# COMMAND ----------

df.display()

# COMMAND ----------

profile = ProfileReport(df, title="Profiling Report")
profile.to_notebook_iframe()

# COMMAND ----------

fig = px.scatter_matrix(df.drop(labels=['tpep_pickup_datetime', 'tpep_dropoff_datetime'], axis=1))
fig.update_traces(diagonal_visible=False)
fig.update_layout(
    width=1500,
    height=1500)
fig
