from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, StrictBool, StrictStr, field_validator


class KafkaSourceSpec(BaseModel):
    """
    Kafka connection + subscription configuration.

    Notes:
    - `kafka_options` is passed directly to Spark's Kafka source via `.options(**kafka_options)`.
    - Exactly one of `topics` or `subscribe_pattern` must be provided.
    """

    model_config = ConfigDict(extra="forbid")

    kafka_options: Dict[StrictStr, StrictStr] = Field(default_factory=dict)
    topics: Optional[List[StrictStr]] = None
    subscribe_pattern: Optional[StrictStr] = None

    # Optional allowlist applied when `topics` is used
    allowed_topics: Optional[List[StrictStr]] = None

    @field_validator("topics", mode="before")
    @classmethod
    def _coerce_topics(cls, v: Any) -> Any:
        # Allow comma-separated topics string for convenience
        if v is None:
            return None
        if isinstance(v, str):
            return [t.strip() for t in v.split(",") if t.strip()]
        return v

    @field_validator("allowed_topics", mode="before")
    @classmethod
    def _coerce_allowed_topics(cls, v: Any) -> Any:
        if v is None:
            return None
        if isinstance(v, str):
            coerced = [t.strip() for t in v.split(",") if t.strip()]
            return coerced or None
        return v

    @field_validator("subscribe_pattern")
    @classmethod
    def _subscribe_pattern_not_empty(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return None
        if not v.strip():
            raise ValueError("'subscribe_pattern' must be a non-empty string if provided")
        return v

    @field_validator("topics")
    @classmethod
    def _topics_not_empty(cls, v: Optional[List[str]]) -> Optional[List[str]]:
        if v is None:
            return None
        if not v:
            raise ValueError("'topics' must be a non-empty list if provided")
        return v

    @field_validator("kafka_options")
    @classmethod
    def _kafka_options_keys_nonempty(cls, v: Dict[str, str]) -> Dict[str, str]:
        for k in v.keys():
            if not str(k).strip():
                raise ValueError("kafka_options contains an empty key")
        return v

    @field_validator("allowed_topics")
    @classmethod
    def _allowed_topics_not_empty(cls, v: Optional[List[str]]) -> Optional[List[str]]:
        if v is None:
            return None
        # Treat empty list as "not provided" (no allowlist).
        if not v:
            return None
        return v

    @field_validator("subscribe_pattern")
    @classmethod
    def _mutual_exclusion_placeholder(cls, v: Optional[str]) -> Optional[str]:
        # mutual exclusion enforced in KafkaPipelineSpec validator where both fields are visible
        return v


class SilverVariantJsonSpec(BaseModel):
    """
    Parse Kafka `value` as JSON into a Databricks Variant column using `try_parse_json`.
    """

    model_config = ConfigDict(extra="forbid")

    value_column: StrictStr = "value"
    parsed_column: StrictStr = "parsed"

    @field_validator("value_column")
    @classmethod
    def _value_column_not_empty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("'value_column' must be a non-empty string")
        return v

    @field_validator("parsed_column")
    @classmethod
    def _parsed_column_not_empty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("'parsed_column' must be a non-empty string")
        return v


class SilverJsonSpec(BaseModel):
    """
    Parse Kafka `value` as JSON into a Struct column using Spark `from_json`.

    Provide exactly one of:
    - schema_ddl: Spark SQL DDL string (recommended)
    - schema_json: StructType JSON (StructType.fromJson compatible)
    """

    model_config = ConfigDict(extra="forbid")

    schema_ddl: Optional[StrictStr] = None
    schema_json: Optional[Dict[str, Any]] = None
    value_column: StrictStr = "value"
    parsed_column: StrictStr = "parsed"

    @field_validator("value_column")
    @classmethod
    def _json_value_column_not_empty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("'value_column' must be a non-empty string")
        return v

    @field_validator("parsed_column")
    @classmethod
    def _json_parsed_column_not_empty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("'parsed_column' must be a non-empty string")
        return v


class SilverSchemaRegistryAvroSpec(BaseModel):
    """
    Parse Kafka `value` as Avro using Confluent Schema Registry.

    Design goal: avoid hard-coding schemas in code/spec by fetching them at runtime.

    `subjects` can contain multiple subjects; the pipeline will try them in order and
    keep the first successful decode (PERMISSIVE mode yields nulls on mismatch).
    """

    model_config = ConfigDict(extra="forbid")

    schema_registry_address: StrictStr
    subjects: List[StrictStr]
    options: Dict[StrictStr, Any] = Field(default_factory=dict)

    value_column: StrictStr = "value"
    parsed_column: StrictStr = "parsed"

    @field_validator("schema_registry_address")
    @classmethod
    def _sr_address_not_empty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("'schema_registry_address' must be a non-empty string")
        return v

    @field_validator("subjects")
    @classmethod
    def _subjects_not_empty(cls, v: List[str]) -> List[str]:
        if not v:
            raise ValueError("'subjects' must be a non-empty list")
        cleaned = [s.strip() for s in v if str(s).strip()]
        if not cleaned:
            raise ValueError("'subjects' must contain at least one non-empty subject")
        return cleaned

    @field_validator("value_column")
    @classmethod
    def _sr_value_column_not_empty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("'value_column' must be a non-empty string")
        return v

    @field_validator("parsed_column")
    @classmethod
    def _sr_parsed_column_not_empty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("'parsed_column' must be a non-empty string")
        return v


class SilverSpec(BaseModel):
    """
    Silver layer parsing strategy.

    - variant_json: parse JSON strings into a Variant column (no schema required)
    - json: parse JSON strings into a Struct column (schema required)
    - schema_registry_avro: decode Avro using Schema Registry (schema inferred at runtime),
      then convert to Variant so fanout expressions can remain `parsed:<field>::type`.
    """

    model_config = ConfigDict(extra="forbid")

    mode: Literal["variant_json", "json", "schema_registry_avro"] = "variant_json"

    variant_json: Optional[SilverVariantJsonSpec] = None
    json: Optional[SilverJsonSpec] = None
    schema_registry_avro: Optional[SilverSchemaRegistryAvroSpec] = None


class FanoutColumnSpec(BaseModel):
    """
    Defines one output column for a fanout table using a Spark SQL expression.
    Example: {"name": "player_id", "expr": "parsed:player_id::string"}
    """

    model_config = ConfigDict(extra="forbid")

    name: StrictStr
    expr: StrictStr

    @field_validator("name")
    @classmethod
    def _name_not_empty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("'name' must be a non-empty string")
        return v

    @field_validator("expr")
    @classmethod
    def _expr_not_empty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("'expr' must be a non-empty string")
        return v


class FanoutTableSpec(BaseModel):
    """
    One destination table in the fanout.
    """

    model_config = ConfigDict(extra="forbid")

    match: StrictStr
    table: StrictStr
    # If True, project all fields from the decoded record (supported for schema_registry_avro mode).
    # When enabled, `columns` can be omitted.
    select_all_fields: StrictBool = False
    columns: Optional[List[FanoutColumnSpec]] = None
    include_kafka_metadata: StrictBool = True

    @field_validator("match")
    @classmethod
    def _match_not_empty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("'match' must be a non-empty string")
        return v

    @field_validator("table")
    @classmethod
    def _table_not_empty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("'table' must be a non-empty string")
        return v

    @field_validator("columns")
    @classmethod
    def _columns_normalize(cls, v: Optional[List[FanoutColumnSpec]]) -> Optional[List[FanoutColumnSpec]]:
        # Cross-field validation (whether columns are required) is enforced in
        # KafkaMedallionPipelineSpec where `silver.mode` is visible.
        if v is None:
            return None
        if not v:
            return None
        return v


class FanoutSpec(BaseModel):
    """
    Gold fan-out configuration.

    Fan-out configuration.

    Key behavior:
    - Fanout key is derived from `key_expr` (preferred) or `key_field`.
    - Each destination table can have its own schema via `tables[*].columns`,
      OR can project all decoded fields via `tables[*].select_all_fields` (Schema Registry mode).
    - Any row that fails parsing OR doesn't match any fanout table is sent to
      `dead_letter_table`.
    """

    model_config = ConfigDict(extra="forbid")

    # Preferred: a Spark SQL expression that yields the fanout key as string.
    # Example for Variant: "parsed:entity_type::string"
    key_expr: Optional[StrictStr] = None
    # Convenience: a plain field name. For Variant silver, this is interpreted as "parsed:<key_field>::string".
    key_field: Optional[StrictStr] = None

    # New, flexible fanout definition
    tables: List[FanoutTableSpec]

    dead_letter_table: StrictStr = "gold_dead_letter_queue"
    include_kafka_metadata_in_dead_letter: StrictBool = True

    @field_validator("dead_letter_table")
    @classmethod
    def _dead_letter_table_not_empty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("'dead_letter_table' must be a non-empty string")
        return v

    @field_validator("key_expr")
    @classmethod
    def _key_expr_not_empty(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return None
        if not v.strip():
            raise ValueError("'key_expr' must be a non-empty string if provided")
        return v

    @field_validator("key_field")
    @classmethod
    def _key_field_not_empty(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return None
        if not v.strip():
            raise ValueError("'key_field' must be a non-empty string if provided")
        return v

    @field_validator("tables")
    @classmethod
    def _tables_not_empty(cls, v: List[FanoutTableSpec]) -> List[FanoutTableSpec]:
        if not v:
            raise ValueError("'tables' must be a non-empty list")
        return v


class TableNamesSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    bronze: StrictStr = "bronze_kafka_raw"
    silver: StrictStr = "silver_kafka_parsed"


class KafkaMedallionPipelineSpec(BaseModel):
    """
    Spec for a medallion-style Kafka ingestion pipeline using SDP/DLT.
    """

    model_config = ConfigDict(extra="forbid")

    tables: TableNamesSpec = Field(default_factory=TableNamesSpec)
    source: KafkaSourceSpec
    silver: SilverSpec = Field(default_factory=SilverSpec)
    fanout: FanoutSpec

    live_prefix: StrictStr = "LIVE"
    table_properties: Dict[StrictStr, StrictStr] = Field(default_factory=dict)

    @field_validator("live_prefix")
    @classmethod
    def _live_prefix_not_empty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("'live_prefix' must be a non-empty string")
        return v

    @field_validator("table_properties")
    @classmethod
    def _table_properties_keys_nonempty(cls, v: Dict[str, str]) -> Dict[str, str]:
        for k in v.keys():
            if not str(k).strip():
                raise ValueError("table_properties contains an empty key")
        return v

    @field_validator("source")
    @classmethod
    def _validate_source_subscription(cls, v: KafkaSourceSpec) -> KafkaSourceSpec:
        has_topics = v.topics is not None
        has_pattern = v.subscribe_pattern is not None
        if has_topics == has_pattern:
            raise ValueError("Exactly one of source.topics or source.subscribe_pattern must be provided")
        return v

    @field_validator("silver")
    @classmethod
    def _validate_silver(cls, v: SilverSpec) -> SilverSpec:
        if v.mode == "variant_json":
            if v.variant_json is None:
                v.variant_json = SilverVariantJsonSpec()
            if v.json is not None or v.schema_registry_avro is not None:
                raise ValueError("Provide only silver.variant_json when silver.mode == 'variant_json'")
        elif v.mode == "json":
            if v.json is None:
                raise ValueError("silver.mode == 'json' requires silver.json")
            if v.json.schema_json is not None and v.json.schema_ddl is not None:
                raise ValueError("Provide only one of silver.json.schema_json or silver.json.schema_ddl")
            if v.json.schema_json is None and v.json.schema_ddl is None:
                raise ValueError("silver.json requires either schema_json or schema_ddl")
            if v.variant_json is not None or v.schema_registry_avro is not None:
                raise ValueError("Provide only silver.json when silver.mode == 'json'")
        elif v.mode == "schema_registry_avro":
            if v.schema_registry_avro is None:
                raise ValueError("silver.mode == 'schema_registry_avro' requires silver.schema_registry_avro")
            if v.variant_json is not None or v.json is not None:
                raise ValueError(
                    "Provide only silver.schema_registry_avro when silver.mode == 'schema_registry_avro'"
                )
        return v

    @field_validator("fanout")
    @classmethod
    def _validate_fanout_key(cls, v: FanoutSpec) -> FanoutSpec:
        if v.key_expr is None and v.key_field is None:
            raise ValueError("fanout requires either key_expr or key_field")
        return v

    @field_validator("fanout")
    @classmethod
    def _validate_fanout_table_projections(cls, v: FanoutSpec, info) -> FanoutSpec:
        """
        Enforce which fanout projection modes are allowed based on `silver.mode`.
        """
        silver = info.data.get("silver")  # type: ignore[assignment]
        if silver is None:
            return v

        for t in v.tables:
            if silver.mode == "schema_registry_avro":
                if t.select_all_fields:
                    continue
                if not t.columns:
                    raise ValueError(
                        "fanout.tables[*].columns is required unless select_all_fields == true "
                        "when silver.mode == 'schema_registry_avro'"
                    )
            else:
                if t.select_all_fields:
                    raise ValueError(
                        "fanout.tables[*].select_all_fields is only supported when "
                        "silver.mode == 'schema_registry_avro'"
                    )
                if not t.columns:
                    raise ValueError("fanout.tables[*].columns must be provided")

        return v


