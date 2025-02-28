# Databricks notebook source
from typing import Union

import pandas as pd

import pyspark
import pyspark.pandas as ps
import pyspark.sql.functions as F
from helpers.volume import VolumeOperator

dataframetype = Union[pd.DataFrame, pyspark.sql.DataFrame, ps.DataFrame]

# COMMAND ----------

# Function to split a three namespace table name into catalog, schema, and table parts
def split_table_name(table_name: str):
    parts = table_name.split(".")
    catalog = "hive_metastore"
    schema = "default"

    if len(parts) == 1:
        # Case when there is only a single level
        table = parts[0]
    elif len(parts) == 2:
        # Case when there are two levels
        schema = parts[0]
        table = parts[1]
    elif len(parts) == 3:
        # Case when there are three levels
        catalog = parts[0]
        schema = parts[1]
        table = parts[2]
    else:
        raise ValueError("Invalid table name format")

    return catalog, schema, table


def create_catalog(spark_context: pyspark.SparkContext, catalog_name: str):
    spark_context.sql(f"CREATE CATALOG IF NOT EXISTS `{catalog_name}`")


def create_schema(spark_context: pyspark.SparkContext, schema_name: str, catalog_name: str = "hive_metastore"):
    spark_context.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog_name}`.`{schema_name}`")


def save_as_table(
    spark_context: pyspark.SparkContext,
    df: dataframetype,
    table_name: str,
    mode: str = 'append',
    table_properties: dict = None,
    create_requirements: bool = False
):
    if isinstance(df, pd.DataFrame):
        df = spark_context.createDataFrame(df)

    if create_requirements:
        catalog, schema, table = split_table_name(table_name)
        create_catalog(spark_context, catalog)
        create_schema(spark_context, catalog, schema)

    writer = df.write.mode(mode)

    if table_properties is not None:

        if 'partitionBy' in table_properties:
            partition_cols = table_properties.pop('partitionBy')
            writer = writer.partitionBy(partition_cols)

        writer = writer.options(table_properties)

    writer.saveAsTable(table_name)


# TODO: Find a way to automatically get profile name
def upload_to_volume(
    catalog: str,
    schema: str,
    volume_name: str,
    single_file: bool,
    source_path: str,
    source_file_name: str = "",
    target_path: str = "",
    profile: str = None

):
    """
    Uploads a file or directory to a volume in Databricks.

    Args:
        profile (str): The name of the Databricks CLI profile to use.
        catalog (str): The name of the catalog in Databricks.
        schema (str): The name of the schema in Databricks.
        volume_name (str): The name of the volume in Databricks.
        single_file (bool): Whether to upload a single file or a directory.
        source_path (str): The path to the file or directory to upload.
        source_file_name (str, optional): The name of the file to upload (only used if single_file is True).
        target_path (str, optional): The target path in the volume to upload the file or directory to.

    Returns:
        None
    """
    voperator = VolumeOperator(catalog=catalog, schema=schema, volume_name=volume_name, profile=profile)

    if single_file:
        voperator.upload_file_from_directory(source_path=source_path, source_file_name=source_file_name, target_path=target_path)
    else:
        voperator.upload_directory(source_path=source_path, target_path=target_path)


# TODO: Find a way to automatically get profile name
def upload_pandas_dataframe_to_volume(
    catalog: str,
    schema: str,
    volume_name: str,
    df: pd.DataFrame,
    file_name: str,
    target_path: str = "",
    profile: str = None,
):
    """
    Uploads a Pandas DataFrame to a volume in Databricks.

    Args:
        profile (str): The name of the Databricks CLI profile to use.
        catalog (str): The name of the catalog in Databricks.
        schema (str): The name of the schema in Databricks.
        volume_name (str): The name of the volume in Databricks.
        df (pd.DataFrame): The DataFrame to upload.
        file_name (str): The name of the file to upload.
        target_path (str, optional): The target path in the volume to upload the file to. Defaults to "".

    Returns:
        None
    """
    voperator = VolumeOperator(catalog=catalog, schema=schema, volume_name=volume_name, profile=profile)
    voperator.upload_pandas_dataframe(df=df, file_name=file_name, target_path=target_path)


def delete_from_volume(
    catalog: str,
    schema: str,
    volume_name: str,
    single_file: bool,
    path: str,
    profile: str = None
):
    """
    Deletes a file or directory from a volume in Databricks.

    Args:
        profile (str): The name of the Databricks CLI profile to use.
        catalog (str): The name of the catalog in Databricks.
        schema (str): The name of the schema in Databricks.
        volume_name (str): The name of the volume in Databricks.
        single_file (bool): Whether to delete a single file or a directory.
        path (str): The path to the file or directory to delete.

    Returns:
        None
    """
    voperator = VolumeOperator(catalog=catalog, schema=schema, volume_name=volume_name, profile=profile)

    if single_file:
        voperator.delete_volume_file(file_path=path)
    else:
        voperator.delete_volume_folder(folder_name=path)