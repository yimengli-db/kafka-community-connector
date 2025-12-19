from __future__ import annotations

from typing import Any, Dict, List, Optional

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


#
# NOTE: Silver is intentionally NOT configurable via the pipeline spec.
# The implementation hard-codes:
# - input column: "value" (Kafka value)
# - output column: "parsed" (Variant)
# - expression: try_parse_json(CAST(value AS STRING))
#


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
    columns: List[FanoutColumnSpec]
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
    def _columns_not_empty(cls, v: List[FanoutColumnSpec]) -> List[FanoutColumnSpec]:
        if not v:
            raise ValueError("'columns' must be a non-empty list")
        return v


class FanoutSpec(BaseModel):
    """
    Gold fan-out configuration.

    Fan-out configuration.

    Key behavior:
    - Fanout key is derived from `key_expr` (preferred) or `key_field`.
    - Each destination table can have its own schema via `tables[*].columns`.
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

    @field_validator("fanout")
    @classmethod
    def _validate_fanout_key(cls, v: FanoutSpec) -> FanoutSpec:
        if v.key_expr is None and v.key_field is None:
            raise ValueError("fanout requires either key_expr or key_field")
        return v


