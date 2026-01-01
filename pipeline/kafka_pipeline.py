"""
Kafka Pipeline - Reads from Kafka and writes to Delta tables.

Architecture:
1. Bronze: Raw Kafka data (partitioned by topic if fanout uses topic)
2. Silver/Final: Parsed key/value with Kafka metadata in __kafka_metadata column
"""

from typing import Any, Dict, List, Optional

from pyspark import pipelines as dp  # type: ignore
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    expr,
    from_json,
    lower,
    struct,
    trim,
)
from pyspark.sql.avro.functions import from_avro  # type: ignore
from pyspark.sql.protobuf.functions import from_protobuf  # type: ignore
from pyspark.sql.types import StructType

from libs.kafka_spec_parser import (
    FanoutRoute,
    FanoutSpec,
    KafkaPipelineSpecParser,
    KafkaSourceSpec,
    SchemaOptions,
    SchemaSpec,
)


# =============================================================================
# Schema Parsing
# =============================================================================


def _parse_column(
    df: DataFrame,
    column_name: str,
    schema_spec: SchemaSpec,
    output_name: str,
) -> DataFrame:
    """Parse a binary column (key or value) according to the schema spec."""
    col_str = col(column_name).cast("string")

    if schema_spec.format == "STRING":
        return df.withColumn(output_name, col_str)

    elif schema_spec.format == "JSON":
        schema = StructType.fromDDL(schema_spec.schema_str)  # type: ignore
        return df.withColumn(output_name, from_json(col_str, schema))

    elif schema_spec.format == "AVRO":
        sr_opts = schema_spec.schema_registry_options
        if sr_opts:
            return df.withColumn(
                output_name,
                from_avro(
                    col(column_name),
                    subject=sr_opts.subject,
                    schemaRegistryAddress=sr_opts.schema_registry_address,
                    options=sr_opts.model_extra or {},
                ),
            )
        else:
            return df.withColumn(
                output_name,
                from_avro(col(column_name), schema_spec.schema_str),
            )

    elif schema_spec.format == "PROTOBUF":
        sr_opts = schema_spec.schema_registry_options
        if sr_opts:
            return df.withColumn(
                output_name,
                from_protobuf(
                    col(column_name),
                    subject=sr_opts.subject,
                    schemaRegistryAddress=sr_opts.schema_registry_address,
                    options=sr_opts.model_extra or {},
                ),
            )
        else:
            return df.withColumn(
                output_name,
                from_protobuf(col(column_name), schema_spec.schema_str),
            )

    return df


def _apply_schema_parsing(
    df: DataFrame,
    schema_options: Optional[SchemaOptions],
) -> DataFrame:
    """Apply key and value schema parsing if configured."""
    if not schema_options:
        return df

    if schema_options.key:
        df = _parse_column(df, "key", schema_options.key, "key")

    if schema_options.value:
        df = _parse_column(df, "value", schema_options.value, "value")

    return df


# =============================================================================
# Kafka Metadata
# =============================================================================


def _add_kafka_metadata_column(df: DataFrame) -> DataFrame:
    """Bundle Kafka metadata fields into a single struct column."""
    return df.withColumn(
        "__kafka_metadata",
        struct(
            col("topic"),
            col("partition"),
            col("offset"),
            col("timestamp").alias("kafka_timestamp"),
            col("timestampType"),
            col("ingested_at"),
        ),
    )


# =============================================================================
# Bronze Layer
# =============================================================================


def _read_kafka_stream(spark: SparkSession, source: KafkaSourceSpec) -> DataFrame:
    """Create a streaming DataFrame from Kafka with standard bronze columns."""
    return (
        spark.readStream
        .format("kafka")
        .options(**source.kafka_options)
        .load()
        .select(
            col("key"),
            col("value"),
            col("topic"),
            col("partition"),
            col("offset"),
            col("timestamp"),
            col("timestampType"),
            current_timestamp().alias("ingested_at"),
        )
    )


def _create_bronze_table(
    spark: SparkSession,
    source: KafkaSourceSpec,
    bronze_table_name: str,
    partition_by_topic: bool,
    table_properties: Dict[str, str],
) -> None:
    """Create bronze staging table with raw Kafka data."""
    table_kwargs: Dict[str, Any] = {
        "name": bronze_table_name,
        "comment": "Bronze: raw Kafka messages",
        "table_properties": {"quality": "bronze", **table_properties},
    }
    if partition_by_topic:
        table_kwargs["partition_cols"] = ["topic"]

    @dp.table(**table_kwargs)
    def _bronze():
        return _read_kafka_stream(spark, source)


# =============================================================================
# Final Tables
# =============================================================================


def _build_output_columns(
    schema_options: Optional[SchemaOptions],
    include_metadata: bool,
) -> List[str]:
    """Determine which columns to select for the final output."""
    columns = []

    # Expand parsed key columns (if key was parsed into struct, not string)
    if schema_options and schema_options.key and schema_options.key.format != "STRING":
        columns.append("key.*")
    else:
        columns.append("key")

    # Expand parsed value columns (if value was parsed into struct, not string)
    if schema_options and schema_options.value and schema_options.value.format != "STRING":
        columns.append("value.*")
    else:
        columns.append("value")

    # Add metadata column
    if include_metadata:
        columns.append("__kafka_metadata")

    return columns


def _create_single_table(
    spark: SparkSession,
    source: KafkaSourceSpec,
    bronze_table_name: str,
    destination_table: str,
    table_properties: Dict[str, str],
) -> None:
    """Create a single destination table from bronze."""
    table_props = {"quality": "silver", **table_properties}

    @dp.table(
        name=destination_table,
        comment="Parsed Kafka data",
        table_properties=table_props,
    )
    def _final():
        df = spark.readStream.table(f"LIVE.{bronze_table_name}")
        df = _apply_schema_parsing(df, source.schema_options)
        df = _add_kafka_metadata_column(df)

        output_cols = _build_output_columns(
            source.schema_options, source.include_kafka_metadata
        )
        return df.selectExpr(*output_cols)


def _create_fanout_tables(
    spark: SparkSession,
    source: KafkaSourceSpec,
    bronze_table_name: str,
    fanout: FanoutSpec,
    table_properties: Dict[str, str],
) -> None:
    """Create multiple destination tables based on fanout routing."""
    table_props = {"quality": "silver", **table_properties}
    bronze_ref = f"LIVE.{bronze_table_name}"

    def _define_routed_table(route: FanoutRoute) -> None:
        match_value = route.match.lower() if not fanout.case_sensitive else route.match

        @dp.table(
            name=route.table,
            comment=f"Fanout: {fanout.key_expr} == '{route.match}'",
            table_properties=table_props,
        )
        def _routed():
            df = spark.readStream.table(bronze_ref)
            df = _apply_schema_parsing(df, source.schema_options)
            df = _add_kafka_metadata_column(df)

            # Build key expression inside the function against the parsed DataFrame
            key_expr_col = expr(fanout.key_expr).cast("string")
            key_normalized = trim(lower(key_expr_col)) if not fanout.case_sensitive else key_expr_col

            # Filter to matching records
            filtered = df.where(key_normalized == match_value)

            output_cols = _build_output_columns(
                source.schema_options, source.include_kafka_metadata
            )
            return filtered.selectExpr(*output_cols)

    # Create a table for each route
    for route in fanout.routes:
        _define_routed_table(route)

    # Create default table for unmatched records
    # TODO: Consider converting to @dp.materialized_view if fanout routes change frequently.
    # A materialized view will automatically exclude records that match newly added routes
    # on refresh, whereas a streaming table requires manual cleanup or full pipeline refresh.
    if fanout.default_table:
        match_values = [
            r.match.lower() if not fanout.case_sensitive else r.match
            for r in fanout.routes
        ]

        @dp.table(
            name=fanout.default_table,
            comment="Fanout: unmatched records",
            table_properties=table_props,
        )
        def _default():
            df = spark.readStream.table(bronze_ref)
            df = _apply_schema_parsing(df, source.schema_options)
            df = _add_kafka_metadata_column(df)

            # Build key expression inside the function against the parsed DataFrame
            key_expr_col = expr(fanout.key_expr).cast("string")
            key_normalized = trim(lower(key_expr_col)) if not fanout.case_sensitive else key_expr_col

            # Filter to non-matching records
            unmatched = df.where(~key_normalized.isin(match_values))

            output_cols = _build_output_columns(
                source.schema_options, source.include_kafka_metadata
            )
            return unmatched.selectExpr(*output_cols)


# =============================================================================
# Pipeline Registration
# =============================================================================

def register_kafka_pipeline(
    spark: SparkSession,
    pipeline_spec: Dict[str, Any],
) -> None:
    """
    Register Kafka ingestion pipeline with SDP/DLT.

    Args:
        spark: SparkSession
        pipeline_spec: Pipeline specification dict
    """
    parser = KafkaPipelineSpecParser(pipeline_spec)
    table_props = dict(parser.table_properties)

    for idx, source in enumerate(parser.sources):
        bronze_table = source.get_bronze_table_name(idx)
        partition_by_topic = source.should_partition_by_topic()

        # Create bronze staging table
        _create_bronze_table(spark, source, bronze_table, partition_by_topic, table_props)

        # Create final table(s)
        if source.fanout_options:
            _create_fanout_tables(
                spark, source, bronze_table, source.fanout_options, table_props
            )
        elif dest_table := source.destination_table:
            _create_single_table(spark, source, bronze_table, dest_table, table_props)
