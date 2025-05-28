# Spark
from pyspark.sql import SparkSession
# Python
from typing import List, Dict, Union
# Own
from MetaData import *
from MetaData.functions import (_sanity_check_table_inputs, _write_metadata_json,
                                _write_metadata_table, _sanity_check_env_config,
                                _sanity_check_env)

# ************************************************************************
# Configuration class for metadata management
# ************************************************************************
class MetatdataConfig:
    """
    Configuration class to hold environment-specific settings.
    """
    def __init__(self,
                 solution_name: str,
                 table_inputs:Dict[str, Union[str, List]],
                 spark):
        """
        **Args:**
            **solution_name** (*str*):
            The name of the solution to configure.

            **table_inputs** (*Dict*):
            A dictionary containing input metadata for the solution's tables.
            Must include the following keys:
            - `"schema_name"`
            - `"table_name"`
            - `"raw_hash_columns"`
            - `"raw_primary_keys"`
            - `"unwanted_columns"`
        """
        # Double-check that the solution name is not empty
        if not solution_name or solution_name.strip() == "":
            raise ValueError("Solution name cannot be empty.")
        self.solution_name = solution_name

        # Overwrite global variables with solution name
        globals()['METADATA_TABLE_NAME'] = f"{globals()['METADATA_TABLE_NAME']}{solution_name}"
        self.table_inputs = _sanity_check_table_inputs(table_inputs)
        self.env_config: Dict
        self.populate_env_config()
        self.spark = spark
        self.metadata_created = False

        # Define schema for metadata table
        self.schema = SCHEMA

    def populate_env_config(self):
        for env in globals()['ENV']:
            self.env_config[env] = {
                "solution_name":self.solution_name.title(),
                "metadata_workspace_name": METADATA_WORKSPACE_NAME,
                "metadata_lakehouse_name": METADATA_LAKEHOUSE_NAME,
                "metadata_table_name": f"{METADATA_TABLE_NAME}{self.solution_name.title()}",
                "metadata_storage_path": f"abfss://{METADATA_WORKSPACE_NAME}@onelake.dfs.fabric.microsoft.com/{METADATA_LAKEHOUSE_NAME}.Lakehouse/TO_BE_REPLACED/",
                "raw_workspace_name": f"WS_INT_{self.solution_name.title()}_{env.lower()}",
                "raw_lakehouse_name": f"LH_Raw_{self.solution_name.title()}",
                "standardized_workspace_name": f"WS_INT_{self.solution_name.title()}_{env.lower()}",
                "standardized_lakehouse_name": f"LH_Standardized_{self.solution_name.title()}",
                "schema_name": self.table_inputs["schema_name"],
                "table_name": self.table_inputs["table_name"],
                "raw_hash_columns": self.table_inputs["raw_hash_columns"],
                "raw_primary_keys": self.table_inputs["raw_primary_keys"],
                "unwanted_columns": self.table_inputs["unwanted_columns"]
            }
    # ************************************************************************
    # Create Metadata Table/JSON
    # ************************************************************************

    def create_metadata_object(self,output_type:str='json') -> None:
        """
        Creates a metadata table in the specified environment.

        **Args:**
            **env_config** (*Dict*):
            A dictionary containing the configuration for the environment.

            **spark** (*SparkSession*):
            The Spark session to use for creating the metadata table.

            **output_type** (*str*):
                The type of output to create. Choose from:
                - `'json'`: Creates a JSON file with the metadata.
                - `'delta'`: Creates a Delta table with the metadata.
        """
        # Sanity checks
        output_type = output_type.lower()
        if output_type not in ['json', 'delta']:
            raise ValueError(f"output_type must be either 'json' or 'delta', got: {output_type}")
        for env_type in self.env_config.keys():
            # Sanity checks for environment type and structure
            _sanity_check_env(env_type)
            _sanity_check_env_config(self.env_config[env_type])
        # Write metadata table or JSON based on output_type
        if output_type == 'json':
            self.env_config = _write_metadata_json(env_config=self.env_config)
        elif output_type == 'delta':
            if spark is None:
                raise ValueError("Spark session must be provided for Delta table creation.")
            if not isinstance(spark, SparkSession):
                raise TypeError(f"spark must be a SparkSession, got: {type(spark)}")
            _write_metadata_table(self.spark, self.env_config)
        self.metadata_created = True
