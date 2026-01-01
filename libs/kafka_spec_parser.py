"""
Kafka Pipeline Spec - Configuration for Kafka ingestion pipelines.

Supports reading from Kafka topics and writing to one or more tables,
with optional fanout routing based on a key expression.
"""

from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, StrictStr, field_validator, model_validator


# =============================================================================
# Schema Options
# =============================================================================


class SchemaRegistryOptions(BaseModel):
    """Schema Registry configuration for Avro/Protobuf decoding."""

    model_config = ConfigDict(extra="allow")  # Allow additional SR options

    schema_registry_address: StrictStr
    subject: Optional[StrictStr] = None

    @field_validator("schema_registry_address")
    @classmethod
    def address_not_empty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("'schema_registry_address' must be non-empty")
        return v

class SchemaSpec(BaseModel):
    """Schema configuration for parsing Kafka key or value."""

    model_config = ConfigDict(extra="forbid")

    format: Literal["STRING", "JSON", "AVRO", "PROTOBUF"]
    schema_str: Optional[StrictStr] = Field(default=None, alias="schema")
    schema_registry_options: Optional[SchemaRegistryOptions] = None

    @model_validator(mode="after")
    def validate_schema_source(self) -> "SchemaSpec":
        if self.format in ("AVRO", "PROTOBUF"):
            if self.schema_str is None and self.schema_registry_options is None:
                raise ValueError(
                    f"'{self.format}' requires either 'schema' or 'schema_registry_options'"
                )
        elif self.format == "JSON":
            if self.schema_str is None:
                raise ValueError("'schema' is required for 'JSON' format")
        # STRING format requires no schema
        return self


class SchemaOptions(BaseModel):
    """Schema options for key and/or value parsing."""

    model_config = ConfigDict(extra="forbid")

    key: Optional[SchemaSpec] = None
    value: Optional[SchemaSpec] = None


# =============================================================================
# Fanout Routing
# =============================================================================


class FanoutRoute(BaseModel):
    """A single fanout route: maps a match value to a destination table."""

    model_config = ConfigDict(extra="forbid")

    match: StrictStr  # Value to match against the fanout key
    table: StrictStr  # Destination table name

    @field_validator("match", "table")
    @classmethod
    def not_empty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("Value must be non-empty")
        return v


class FanoutSpec(BaseModel):
    """
    Fanout configuration for routing records to multiple tables.

    The `key_expr` is a Spark SQL expression that extracts the routing key
    from the record. This can reference:
    - Raw columns: key, value, topic, partition, offset, timestamp
    - Variant path syntax: value:field_name::string

    Example key_expr values:
    - "value:entity_type::string"  (Variant path)
    - "CAST(value.entity_type AS STRING)"  (Struct field)
    - "topic"  (Route by topic name)
    """

    model_config = ConfigDict(extra="forbid")

    key_expr: StrictStr  # SQL expression to extract routing key
    routes: List[FanoutRoute]  # List of value -> table mappings
    default_table: Optional[StrictStr] = None  # Table for unmatched values
    case_sensitive: bool = False  # Whether matching is case-sensitive

    @field_validator("key_expr")
    @classmethod
    def key_expr_not_empty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("'key_expr' must be non-empty")
        return v

    @field_validator("routes")
    @classmethod
    def routes_not_empty(cls, v: List[FanoutRoute]) -> List[FanoutRoute]:
        if not v:
            raise ValueError("'routes' must be a non-empty list")
        return v

    @model_validator(mode="after")
    def validate_unique_matches(self) -> "FanoutSpec":
        """Ensure no duplicate match values."""
        matches = [r.match.lower() if not self.case_sensitive else r.match for r in self.routes]
        if len(matches) != len(set(matches)):
            raise ValueError("Duplicate match values in fanout routes")
        return self


# =============================================================================
# Source Spec
# =============================================================================


class KafkaSourceSpec(BaseModel):
    """
    Configuration for a single Kafka source.

    A source reads from one or more topics (via kafka_options) and writes to
    either a single destination table or multiple tables via fanout routing.
    """

    model_config = ConfigDict(extra="forbid")

    kafka_options: Dict[StrictStr, Any] = Field(default_factory=dict)

    # Destination (used when no fanout, or as namespace prefix for fanout tables)
    destination_catalog: Optional[StrictStr] = None
    destination_schema: Optional[StrictStr] = None
    destination_table: Optional[StrictStr] = None  # Single table if no fanout

    # Schema parsing
    schema_options: Optional[SchemaOptions] = None

    # Fanout routing (if set, overrides destination_table)
    fanout_options: Optional[FanoutSpec] = None

    # Options
    include_kafka_metadata: bool = True  # Include topic, partition, offset columns

    @model_validator(mode="after")
    def validate_destination(self) -> "KafkaSourceSpec":
        """Ensure a destination is specified (either single table or fanout)."""
        if self.fanout_options is None and self.destination_table is None:
            raise ValueError(
                "Must specify either 'destination_table' or 'fanout_options'"
            )
        return self

    def should_partition_by_topic(self) -> bool:
        """Determine if bronze table should be partitioned by topic."""
        if not self.fanout_options:
            return False
        # Partition by topic if fanout key involves topic
        return "topic" in self.fanout_options.key_expr.lower()

    def get_bronze_table_name(self, index: int = 0) -> str:
        """Generate bronze table name for this source."""
        if self.destination_table:
            return f"_bronze_{self.destination_table}"
        return f"_bronze_source_{index}"


# =============================================================================
# Pipeline Spec
# =============================================================================


class KafkaPipelineSpec(BaseModel):
    """
    Top-level Kafka pipeline specification.

    See pipeline-spec/README.md for full documentation and examples.
    """

    model_config = ConfigDict(extra="forbid")

    connection_name: StrictStr
    destination_catalog: Optional[StrictStr] = None
    destination_schema: Optional[StrictStr] = None
    sources: List[KafkaSourceSpec]

    # Global table properties
    table_properties: Dict[StrictStr, StrictStr] = Field(default_factory=dict)

    @field_validator("connection_name")
    @classmethod
    def connection_name_not_empty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("'connection_name' must be non-empty")
        return v

    @field_validator("sources")
    @classmethod
    def sources_not_empty(cls, v: List[KafkaSourceSpec]) -> List[KafkaSourceSpec]:
        if not v:
            raise ValueError("'sources' must be a non-empty list")
        return v


# =============================================================================
# Parser
# =============================================================================


class KafkaPipelineSpecParser:
    """Parser and helper methods for Kafka pipeline specifications."""

    def __init__(self, spec: Dict[str, Any]) -> None:
        if not isinstance(spec, dict):
            raise ValueError("Spec must be a dictionary")
        self._spec = KafkaPipelineSpec(**spec)

    @property
    def connection_name(self) -> str:
        return self._spec.connection_name

    @property
    def sources(self) -> List[KafkaSourceSpec]:
        return self._spec.sources

    @property
    def table_properties(self) -> Dict[str, str]:
        return self._spec.table_properties
