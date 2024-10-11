import functools
from collections import Counter

import pandas as pd

import pyspark
import pyspark.pandas as ps
import pyspark.sql.functions as F

# COMMAND ----------

MB128 = 128 * 1024 * 1024


@functools.cache
def get_dbutils(profile: str = None):
    """Imports dbutils is available in local development."""

    # try to import dbutils
    # if no issues -> noop
    try:
        _ = dbutils.help()

    # otherwise import it from databricks sdk
    except NameError as e:
        print("Could not find dbutils, importing.")
        from databricks.sdk import WorkspaceClient

        # if profile is specified
        # set auth type to PAT
        # see: https://github.com/databricks/databricks-vscode/issues/916
        if profile is not None:
            from databricks.sdk.core import Config

            os.environ["DATABRICKS_AUTH_TYPE"] = 'pat'
            w = WorkspaceClient(config=Config(profile=profile))

        # otherwise: try to use native auth
        else:
            w = WorkspaceClient()

        # extract dbutils from databricks sdk
        dbutils = w.dbutils

    return dbutils


@functools.cache
def estimate_size(df: pyspark.sql.DataFrame) -> int:
    """Estimates the size of the DataFrame"""
    num_rows = df.count()
    sample_rate = min(10 / num_rows, 1.0)
    sample_rows = df.sample(fraction=sample_rate, withReplacement=False).toPandas()
    row_size = sample_rows.memory_usage(deep=True, index=True).sum() / len(sample_rows)
    est_data_size = num_rows * row_size
    return est_data_size


def is_oversized(df: pyspark.sql.DataFrame, size: int = None, threshold_bytes: int = MB128) -> bool:
    """Returns whether the size of a dataset in a df is over a given threshold."""
    data_size = estimate_size(df) if size is None else size
    return data_size > threshold_bytes


def limit_df(
    df: pyspark.sql.DataFrame,
    method: str,
    size: int,
    threshold_bytes: int = MB128
) -> pyspark.sql.DataFrame:
    """Limits the size of a spark DataFrame using the `size` and `threshold_bytes` parameter."""
    if method == 'sample':
        # target sample is the ratio of the threshold and the estimated size
        #
        # consider the following case:
        # - threshold = 100 bytes
        # - df has 100 records and 2 columns
        # - estimated data size is 100 (records) * 2 (columns) * 8 (avg data size) = 1600 (bytes)
        # - target sample will be 100 (bytes) / 1600 (bytes) = 1 / 16 = 0.0625
        # -> sample will contain ~6 records.
        target_sample = threshold_bytes / size
        limited_df = df.sample(target_sample)

    elif method == 'limit':
        # target limit is the ratio of the number of records and the multiplicative factor
        # of the estimated size and the threshold
        #
        # consider the following case:
        # - threshold = 100 bytes
        # - df has 100 records and 2 columns
        # - estimated data size is 100 (records) * 2 (columns) * 8 (avg data size) = 1600 (bytes)
        # - multiplicative factor is 1600 (bytes) / 100 (bytes) = 16
        # - target limit will be 100 (records) / 16 = ~6 records
        target_limit = int(df.count() / (size / threshold_bytes))
        limited_df = df.limit(target_limit)

    else:
        raise ValueError(f"Invalid method `{method}`. Available methods are: `sample` and `limit`.")

    return limited_df


def read_from_table(
    spark: pyspark.sql.SparkSession,
    table_name: str,
    as_pandas: bool = False,
    threshold_bytes: int = MB128,
    force_pandas: bool = False,
    force_method: str = 'sample'
):
    """
    Reads a (Unity Catalog) table as a dataframe.

    The type of the returned dataframe depends on:
    - whether the user requested a pandas dataframe
    - the estimated size of the dataset

    The following scenarios are available:
    - no pandas: spark.DataFrame
    - pandas, within threshold bytes: pandas.DataFrame
    - pandas, over threshold bytes:
        - no force pandas: pyspark.pandas.DataFrame
        - force pandas: pandas.DataFrame

    Forcing pandas dataframe option will use the force method function
    to reduce the size of the dataframe to be around the threshold bytes
    size. There are two methods available: `sample` and `limit`.
    `sample` will subsample the dataset, `limit` will limit the number of
    records.

    Parameters:
    -----------
        - table_name (str): the fully qualified name of the table (catalog.schema.table)
        - as_pandas (bool): whether the user would like to work with the pandas API
        - threshold_bytes (int): the maximum size that can be collected to the driver
        - force_pandas (bool): whether to force the dataframe to be a pd.DataFrame
        - force_method (str): function to use for downsampling for pandas df forcing
                              available functions are 'sample' and 'limit'

    Returns:
    --------
        - df (pandas.DataFrame, pyspark.pandas.DataFrame, pyspark.sql.DataFrame):
            see description.
    """
    df = spark.read.table(table_name)

    # if user requested a pandas dataframe
    if as_pandas:

        # in case it is oversized, return a pandas API df
        if is_oversized(df, threshold_bytes):
            if force_pandas:
                df = (
                    limit_df(df,
                             method=force_method,
                             size=estimate_size(df),
                             threshold_bytes=threshold_bytes)
                    .toPandas()
                )

            else:
                df = ps.DataFrame(df)

        # otherwise collect the data to the driver as a pandas df
        else:
            df = df.toPandas()

    return df

# COMMAND ----------

def get_filetype(paths: list) -> str:
    files = [fileinfo.path for fileinfo in paths if not fileinfo.size > 0]
    dirs = [fileinfo.name for fileinfo in paths if fileinfo.size == 0]
    
    filetypes = list(set([fileinfo.rsplit('.', 1)[-1] for fileinfo in files]))

    # if there are only same type files
    if len(filetypes) == 1:
        filetype = filetypes[0]

    elif len(filetypes) == 2 and 'txt' in filetypes:
        filetype = [ft for ft in filetypes if not ft == 'txt'][0]

    else:
        raise ValueError(f"Ambiguous filetypes in `{paths[0]}`: {filetypes}. "
                         f"Please specify the exact file to read.")

    if '_delta_log' in dirs or '_delta_log/' in dirs:
        filetype = 'delta'

    return filetype


def read_with_pandas(spark, path, filetype, **kwargs):
    """Reads a dataset from `path` in format `filetype` as a pandas DataFrame."""
    if filetype  == 'csv':
        df = pd.read_csv(path, **kwargs)

    elif filetype == 'tsv':
        df = pd.read_csv(path, sep='\t', **kwargs)

    elif filetype in ('xls', 'xlsx'):
        df = pd.read_excel(path, **kwargs)

    elif filetype == 'parquet':
        df = pd.read_parquet(path, **kwargs)

    elif filetype == 'delta':
        df = read_with_spark(spark, path, filetype, **kwargs).toPandas()

    else:
        raise NotImplementedError(f"No reader implemented for filetype `{filetype}`.")

    return df

def read_with_spark(spark, path, filetype, **kwargs):
    """Reads a dataset from `path` in format `filetype` as a spark DataFrame."""
    if filetype  == 'tsv':
        filetype = 'csv'
        kwargs['sep'] = '\t'

    if filetype in ('xls', 'xlsx'):
        filetype = 'com.crealytics.spark.excel'

    df = spark.read.format(filetype).options(**kwargs).load(path)

    return df

def read_from_volume(
    spark: pyspark.sql.SparkSession,
    path: str,
    as_pandas: bool = False,
    threshold_bytes: int = MB128,
    force_pandas: bool = False,
    force_method: str = 'sample',profile: str = None,
    **kwargs
):
    """
    Reads files from volume.
    """
    paths = get_dbutils(profile).fs.ls(path)
    size = sum([fileinfo.size for fileinfo in paths if not fileinfo if fileinfo.size > 0])
    
    filetype = get_filetype(paths)

    if as_pandas:
        # Under the threshold -> read using pandas
        if size <= threshold_bytes:
            file_to_read = paths[0].path.replace('dbfs:', '') if not filetype == 'delta' else path
            df = read_with_pandas(spark, path=file_to_read, filetype=filetype, **kwargs)

        else:
            # if it is a single file -> warn + read using pandas
            if len(paths) == 1:
                print(f"Large single file detected {size / 1024 / 1024 / 1024:.2f} GB. "
                      f"Attempting to read using pandas.")

                file_to_read = paths[0].path.replace('dbfs:', '') if not filetype == 'delta' else path
                df = read_with_pandas(spark, path=file_to_read, filetype=filetype, **kwargs)

            # more files, but forced -> read using spark, limit size, convert to pandas
            elif force_pandas:
                spark_df = read_with_spark(spark, path, filetype, **kwargs)
                df = (
                    limit_df(spark_df,
                             method=force_method,
                             size=estimate_size(df),
                             threshold_bytes=threshold_bytes,
                             **kwargs)
                    .toPandas()
                )

            # more files, not forced -> read using spark, convert it to spark.pandas
            else:
                spark_df = read_with_spark(spark, path, filetype, **kwargs)
                df = ps.DataFrame(spark_df)

    # pandas not requested -> read with spark
    else:
        df = read_with_spark(spark, path, filetype, **kwargs)

    return df
