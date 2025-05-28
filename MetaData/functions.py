from MetaData.data_models import TableInputs, FieldModel, ValidationError, EnvModel
from typing import Dict
from MetaData import *
# ************************************************************************
# SANITY CHECKS
# ************************************************************************
def _sanity_check_table_inputs(table_inputs: Dict) -> Dict:
    """
    Perform sanity checks on the table inputs.

    **Args:**
        **table_inputs** (*Dict*): A dictionary containing input metadata for the solution's tables.
        Must include the following keys:
        - `"schema_name"`
        - `"table_name"`
        - `"raw_hash_columns"`
        - `"raw_primary_keys"`
        - `"unwanted_columns"`
    **Returns**:
        **Dict**: The validated and formatted table inputs dictionary.
    **Raises**:
        ValueError: If any of the required keys are missing or if the values are not of the expected type.
    """
    model = TableInputs.model_validate(table_inputs)   # validate & coerce
    return model.model_dump()

def _sanity_check_env_config(env_config: Dict[str, str]) -> None:
    """
    Performs sanity checks on the environment configuration using Pydantic.
    **Args:**
        **env_config** (*Dict[str, str]*): A dictionary containing the environment configuration for the metadata table.
    **Raises**:
        It raises a ValueError if the configuration is invalid.
    """
    try:
        FieldModel.model_validate(env_config)
    except ValidationError as e:
        raise ValueError(f"Invalid environment configuration: {e}") from e

def _sanity_check_env(env: str) -> None:
    """
    Performs sanity checks on the environment name.

    **Args:**
        **env** (*str*): The environment name to check.
    **Raises**:
        It raises a ValueError if the environment name is invalid.
    """
    try:
        EnvModel.model_validate(env)
    except ValidationError as e:
        raise ValueError(f"Invalid environment name: {e}") from e
# ************************************************************************
# Write metadata to Delta table or JSON file
# ************************************************************************
def _write_metadata_table(spark: SparkSession,
                         env_config: Dict[str, str]) -> None:
    """
    Writes the metadata configuration to a Delta table.

    **Args:**
        **spark** (*SparkSession*):
        The Spark session to use for writing the metadata table.

        **env_config** (*Dict[str, str]*):
        A dictionary containing the environment configuration for the metadata table.

    **Returns**:
        **None**: This function does not return anything. It writes the metadata configuration to a Delta table.
    """

    # Create DataFrame from env_config
    for env_var in globals()['ENV']:
        df = spark.createDataFrame([env_config[env_var]], SCHEMA)

        # Add metadata columns
        df = df.withColumn("created_at", current_timestamp()) \
            .withColumn("created_by", lit("ConfigCreator")) \
            .withColumn("updated_at", current_timestamp()) \
            .withColumn("updated_by", lit("ConfigCreator"))

        # Write to Delta table
        env_config[env_var]["metadata_storage_path"] = env_config[env_var]["metadata_storage_path"].replace("TO_BE_REPLACED", "Tables")
        env_config[env_var]["metadata_table_name"] = f"{env_var}_{env_config[env_var]['metadata_table_name']}"
        env_config[env_var]["metadata_table_path"] = f"{env_config[env_var]['metadata_table_name']}{env_config[env_var]['metadata_table_name']}"
        path = env_config[env_var]["metadata_storage_path"] + env_config[env_var]["metadata_table_name"]
        df.write.format("delta") \
            .option("mergeSchema", "true") \
            .mode("overwrite") \
            .save(path)

def _write_metadata_json(env_config: Dict[str, str]) -> None:
    """
    Writes the metadata configuration to a JSON file in the specified Lakehouse.
    **Args:**
        **env_config** (*Dict[str, str]*):
        A dictionary containing the environment configuration for the metadata table.
    **Returns**:
        **env_config** (*Dict[str,str]*): The updated environment configuration with the metadata storage path and table name.
    """
    for env_var in globals()['ENV']:
        # Overwrite the metadata storage path to point to files
        env_config[env_var]["metadata_storage_path"] = env_config[env_var]["metadata_storage_path"].replace("TO_BE_REPLACED", "Files")
        # Overwrite the metadata table name to include ".json"
        env_config[env_var]["metadata_table_name"] = f"{env_var}_{env_config[env_var]['metadata_table_name']}.json"
        env_config[env_var]["metadata_table_path"] = f"{env_config[env_var]['metadata_table_name']}{env_config[env_var]['metadata_table_name']}"

        path = env_config[env_var]["metadata_storage_path"] + env_config[env_var]["metadata_table_name"]

        # Write the dictionary to a JSON file
        mssparkutils.fs.put(file = path, content = json.dumps(env_config[env_var]), overwrite=True)
    return env_config

# ************************************************************************
# Allowed custom fields
# ************************************************************************
def add_allowed_custom_field(custom_field_name:str) -> None:
    """
    Adds a custom field to the allowed custom fields list in the Lakehouse metadata.

    **Args:**
        **custom_field_name** (*str*):
        The name of the custom field to add.

        **custom_field_value** (*Union[str, int, float]*):
        The value of the custom field to add.
    """

    row_df = (
    spark.range(1)                         # dummy row id
         .select(                          # Set the actual columns
             F.lit(custom_field_name)       .alias("AllowedFieldName"),
             F.current_timestamp()          .alias("CreatedAt"),
             F.lit(mssparkutils
             .env
             .getUserName())                .alias("CreatedBy"),
             F.current_timestamp()          .alias("UpdatedAt"),
             F.lit(mssparkutils
             .env
             .getUserName())                .alias("UpdatedBy"),
         )
    )

    target_path = (
    f"abfss://{METADATA_WORKSPACE_NAME}@onelake.dfs.fabric.microsoft.com/"
    f"{METADATA_LAKEHOUSE_NAME}.Lakehouse/Tables/AllowedFields"
    )

    row_df.write.format("delta").mode("append").save(target_path)

# ************************************************************************
# Add required fields
# ************************************************************************
def add_required_field(required_field:str) -> None:
    """
    Adds a custom field to the allowed custom fields list in the Lakehouse metadata.

    **Args:**
        **required_field** (*str*):
        The name of the custom field to add.
    """

    row_df = (
    spark.range(1)                         # dummy row id
         .select(                          # Set the actual columns
             F.lit(required_field)          .alias("RequiredFieldName"),
             F.current_timestamp()          .alias("CreatedAt"),
             F.lit(mssparkutils
             .env
             .getUserName())                .alias("CreatedBy"),
             F.current_timestamp()          .alias("UpdatedAt"),
             F.lit(mssparkutils
             .env
             .getUserName())                .alias("UpdatedBy"),
         )
    )

    target_path = (
    f"abfss://{METADATA_WORKSPACE_NAME}@onelake.dfs.fabric.microsoft.com/"
    f"{METADATA_LAKEHOUSE_NAME}.Lakehouse/Tables/RequiredFields"
    )

    row_df.write.format("delta").mode("append").save(target_path)
