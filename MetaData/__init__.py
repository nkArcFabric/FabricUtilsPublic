import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import SparkSession
from typing import Dict, Union, List, Optional
from pyspark.sql.functions import lit, current_timestamp, col, sha2, concat_ws
from pyspark.sql.types import StructType, StructField, StringType
from delta.tables import DeltaTable
from pyspark.sql.utils import AnalysisException
import json

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("CreateMetaData") \
    .getOrCreate()

# Create schema if it doesn't exist
SCHEMA = StructType([
    StructField("solution_name", StringType(), True),
    StructField("metadata_workspace_name", StringType(), True),
    StructField("metadata_lakehouse_name", StringType(), True),
    StructField("metadata_table_name", StringType(), True),
    StructField("metadata_table_path", StringType(), True),
    StructField("raw_workspace_name", StringType(), True),
    StructField("raw_lakehouse_name", StringType(), True),
    StructField("standardized_workspace_name", StringType(), True),
    StructField("standardized_lakehouse_name", StringType(), True),
    StructField("schema_name", StringType(), True),
    StructField("table_name", StringType(), True),
    StructField("raw_hash_columns", StringType(), True),
    StructField("raw_primary_keys", StringType(), True),
    StructField("unwanted_columns", StringType(), True)
])

# ************************************************************************
# Constants
# ************************************************************************
ENV = ['DEV', 'TEST', 'PROD']
METADATA_WORKSPACE_NAME = 'WS_Administration'
METADATA_LAKEHOUSE_NAME = 'LH_Utility'
METADATA_TABLE_NAME = 'metadata_'

# ************************************************************************
# Functions
# ************************************************************************
def get_fields(required:bool=True) -> List[str]:
    """
    Returns list of fields for the configuration.

    **Args:**
        **required** (*bool*): If True, returns the _required_ fields. Defaults to True. Otherwise, returns the _allowed_ fields.

    **Returns:**
        **List[str]**: List of fields.
    """
    spark=SparkSession.getActiveSession()
    if not isinstance(required, bool):
        raise ValueError(f"argument: required is supposed to be `bool`, {type(required)} given")
    if required:
        table_name = 'RequiredFields'
        col_name = 'RequiredFieldName'
    else:
        table_name = 'AllowedFields'
        col_name = 'AllowedFieldName'

    lakehouse_path = (
    f"abfss://{METADATA_WORKSPACE_NAME}@onelake.dfs.fabric.microsoft.com/"
    f"{METADATA_LAKEHOUSE_NAME}.Lakehouse/Tables/{table_name}"
    )

    # Read data from the table
    _fields = spark.read.parquet(lakehouse_path)
    fields = [row[col_name] for row in _fields.collect()]

    return fields

