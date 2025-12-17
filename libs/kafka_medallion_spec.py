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
            return [t.strip() for t in v.split(",") if t.strip()]
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
        if not v:
            raise ValueError("'allowed_topics' must be a non-empty list if provided")
        return v

    @field_validator("subscribe_pattern")
    @classmethod
    def _mutual_exclusion_placeholder(cls, v: Optional[str]) -> Optional[str]:
        # mutual exclusion enforced in KafkaPipelineSpec validator where both fields are visible
        return v


class SilverJsonSpec(BaseModel):
    """
    JSON parsing configuration for the Silver layer.

    Provide exactly one of:
    - schema_ddl: Spark SQL DDL string, e.g. "id STRING, ts TIMESTAMP, payload STRUCT<...>"
    - schema_json: StructType JSON (as a dict) compatible with StructType.fromJson(...)
    """

    model_config = ConfigDict(extra="forbid")

    schema_ddl: Optional[StrictStr] = None
    schema_json: Optional[Dict[str, Any]] = None
    value_column: StrictStr = "value"

    @field_validator("value_column")
    @classmethod
    def _value_column_not_empty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("'value_column' must be a non-empty string")
        return v


class SilverSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    mode: Literal["json"] = "json"
    json: SilverJsonSpec


class FanoutSpec(BaseModel):
    """
    Gold fan-out configuration.

    `mapping` drives the static set of gold tables that will be created:
      { "<field_value>": "<table_name>" }
    """

    model_config = ConfigDict(extra="forbid")

    field: StrictStr
    mapping: Dict[StrictStr, StrictStr]
    include_unknown: StrictBool = False
    unknown_table: StrictStr = "gold_unknown"

    @field_validator("field")
    @classmethod
    def _field_not_empty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("'field' must be a non-empty string")
        return v

    @field_validator("mapping")
    @classmethod
    def _mapping_not_empty(cls, v: Dict[str, str]) -> Dict[str, str]:
        if not v:
            raise ValueError("'mapping' must be a non-empty mapping")
        for key, table in v.items():
            if not str(key).strip():
                raise ValueError("fanout.mapping contains an empty key")
            if not str(table).strip():
                raise ValueError(f"fanout.mapping for '{key}' has an empty table name")
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
    silver: SilverSpec
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


