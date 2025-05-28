from typing import List, Dict, Any
from pydantic import BaseModel, Field, field_serializer, RootModel, model_validator, ValidationError
from MetaData import get_fields, ENV

REQUIRED_FIELDS = set(get_fields(True))
ALLOWED_FIELDS = set(get_fields(False))
ALLOWED_ENVS = set(ENV)

# ************************************************************************
# VALIDATION MODELS
# ************************************************************************

class TableInputs(BaseModel):
    schema_name: str                = Field(..., description="Database schema, e.g. dbo")
    table_name: str                 = Field(..., description="Target table name")
    raw_hash_columns: List[str]
    raw_primary_keys: List[str]
    unwanted_columns: List[str]

    # ────────────────────────────────────────────────────────
    # Custom serialisation → collapse list into of strings into a single string
    # ────────────────────────────────────────────────────────
    @field_serializer("raw_hash_columns", "raw_primary_keys", "unwanted_columns")
    def _list_to_bracketed(cls, v: List[str]) -> str:
        return f"[{','.join(v)}]"

class FieldModel(RootModel):
    """
    Validates and serializes a list of fields.
    """
    root: Dict[str, Any]

    model_config = {
        "extra" : "allow"
    }

    @model_validator(mode="after")
    def _check_keys(self):
        data_keys = set(self.root)

        # Are alle required keys present?
        missing = REQUIRED_FIELDS - data_keys
        if missing:
            raise ValueError(f"Missing required fields: {', '.join(missing)}")

        # Are there any unallowed keys?
        extra = data_keys - ALLOWED_FIELDS
        if extra:
            raise ValueError(f"Extra fields not allowed: {', '.join(extra)}")
        return self

class EnvModel(RootModel):
    """
    Validates and serializes a list of environment names.
    """
    root: List[str]

    @model_validator(mode="after")
    def _check_env_keys(self):
        env_keys = set(self.root)

        illegal = env_keys - ALLOWED_ENVS
        if illegal:
            raise ValueError(
                f"env_config contains invalid env names: {sorted(illegal)}; "
                f"expected one of {sorted(ALLOWED_ENVS)}"
            )

        return self
