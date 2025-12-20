from __future__ import annotations

from typing import Any, Dict, Optional

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, coalesce, current_timestamp, expr, from_json, to_json
from pyspark.sql.types import StructType

from libs.kafka_medallion_spec import (
    FanoutTableSpec,
    KafkaMedallionPipelineSpec,
    SilverJsonSpec,
    SilverSchemaRegistryAvroSpec,
)

SCHEMA_REGISTRY_DECODED_COLUMN = "decoded"


def _struct_type_from_json_spec(spec: SilverJsonSpec) -> StructType:
    if spec.schema_json is not None:
        return StructType.fromJson(spec.schema_json)
    # `fromDDL` is available in Spark 3.4+. This project targets pyspark>=3.5.
    return StructType.fromDDL(spec.schema_ddl)  # type: ignore[arg-type]


def _try_parse_json_variant(df: DataFrame, *, value_col: str, parsed_col: str) -> DataFrame:
    value_str = col(value_col).cast("string")
    try:
        # Databricks Runtime provides try_parse_json for Variant
        from pyspark.sql.functions import try_parse_json  # type: ignore

        return df.withColumn(parsed_col, try_parse_json(value_str))
    except Exception:
        # Fallback: rely on SQL function resolution at runtime
        return df.withColumn(parsed_col, expr(f"try_parse_json(CAST({value_col} AS STRING))"))


def _parse_schema_registry_avro_to_variant(
    df: DataFrame, *, cfg: SilverSchemaRegistryAvroSpec
) -> DataFrame:
    """
    Decode Avro using Schema Registry, then convert the decoded struct into Variant.

    If multiple subjects are provided, we try them in order and coalesce the first
    successful decode into `parsed_column`.
    """
    value_col = cfg.value_column
    parsed_col = cfg.parsed_column

    # Try each subject; decode (struct), then coalesce to a single decoded struct.
    variant_col_names: list[str] = []
    struct_col_names: list[str] = []
    temp_cols: list[str] = []
    for i, subject in enumerate(cfg.subjects):
        decoded_struct_col = f"__decoded_sr_{i}"
        decoded_json_col = f"__decoded_sr_json_{i}"
        decoded_variant_col = f"__decoded_sr_variant_{i}"
        try:
            from pyspark.sql.avro.functions import from_avro  # type: ignore

            df = df.withColumn(
                decoded_struct_col,
                from_avro(
                    col(value_col),
                    subject=subject,
                    schemaRegistryAddress=cfg.schema_registry_address,
                    options=cfg.options,
                ),
            )
        except Exception as e:
            raise RuntimeError(
                "Schema Registry Avro parsing requires a Databricks runtime that supports "
                "Schema Registry-aware `from_avro(...)`."
            ) from e

        df = df.withColumn(decoded_json_col, to_json(col(decoded_struct_col)))
        df = _try_parse_json_variant(df, value_col=decoded_json_col, parsed_col=decoded_variant_col)
        variant_col_names.append(decoded_variant_col)
        struct_col_names.append(decoded_struct_col)
        temp_cols.extend([decoded_struct_col, decoded_json_col, decoded_variant_col])

    # Keep the decoded struct around for "project all fields" fanout, and also keep Variant for keying.
    df = df.withColumn(SCHEMA_REGISTRY_DECODED_COLUMN, coalesce(*[col(c) for c in struct_col_names]))
    df = df.withColumn(parsed_col, coalesce(*[col(c) for c in variant_col_names]))
    return df.drop(*temp_cols)


def _kafka_read_stream_df(spark, spec: KafkaMedallionPipelineSpec) -> DataFrame:
    source = spec.source

    df = spark.readStream.format("kafka").options(**source.kafka_options)

    if source.topics is not None:
        topics = [t.strip() for t in source.topics if t.strip()]
        if source.allowed_topics:
            allowed = {t.strip() for t in source.allowed_topics if t.strip()}
            topics = [t for t in topics if t in allowed]
        if not topics:
            raise ValueError("No Kafka topics left to ingest after applying allowlist/filtering")
        df = df.option("subscribe", ",".join(topics))
    else:
        df = df.option("subscribePattern", source.subscribe_pattern)

    return df.load()


def _fanout_key_expr(spec: KafkaMedallionPipelineSpec) -> str:
    """
    Return a Spark SQL expression that yields the fanout key as a string.
    """
    if spec.fanout.key_expr is not None:
        return spec.fanout.key_expr

    # Derive from key_field
    if spec.fanout.key_field is None:
        raise ValueError("fanout requires key_expr or key_field")

    if spec.silver.mode == "variant_json":
        parsed_col = spec.silver.variant_json.parsed_column  # type: ignore[union-attr]
    elif spec.silver.mode == "schema_registry_avro":
        parsed_col = spec.silver.schema_registry_avro.parsed_column  # type: ignore[union-attr]
    else:
        parsed_col = spec.silver.json.parsed_column  # type: ignore[union-attr]

    if spec.silver.mode in ("variant_json", "schema_registry_avro"):
        return f"{parsed_col}:{spec.fanout.key_field}::string"

    # json(struct) mode
    return f"CAST({parsed_col}.{spec.fanout.key_field} AS STRING)"


def _project_fanout_table(
    df: DataFrame, table_spec: FanoutTableSpec, *, spec: KafkaMedallionPipelineSpec
) -> DataFrame:
    """
    Project a fanout table.

    Special behavior for Schema Registry Avro:
    - If `table_spec.select_all_fields` is True, project all decoded Avro fields via `decoded.*`.
    - Always include `kafka_timestamp` and `ingestion_timestamp` without requiring explicit spec columns.
    """

    if spec.silver.mode == "schema_registry_avro":
        # Always include these two timestamps (hard-coded requirement).
        base_cols: list = [col("kafka_timestamp"), col("ingestion_timestamp")]
        if table_spec.include_kafka_metadata:
            base_cols = [col("topic"), col("partition"), col("offset")] + base_cols

        if table_spec.select_all_fields:
            # decoded.* expands all Avro fields into top-level columns
            decoded_star = f"{SCHEMA_REGISTRY_DECODED_COLUMN}.*"
            return df.select(*base_cols, decoded_star)

        projected_cols = [expr(c.expr).alias(c.name) for c in (table_spec.columns or [])]
        return df.select(*base_cols, *projected_cols)

    # Default behavior (non-schema-registry modes)
    kafka_meta_cols = [
        col("topic"),
        col("partition"),
        col("offset"),
        col("kafka_timestamp"),
        col("ingestion_timestamp"),
    ]
    projected_cols = [expr(c.expr).alias(c.name) for c in (table_spec.columns or [])]
    if table_spec.include_kafka_metadata:
        return df.select(*kafka_meta_cols, *projected_cols)
    return df.select(*projected_cols)


def register_kafka_medallion_pipeline(
    spark, pipeline_spec: Dict[str, Any], dp_module: Optional[Any] = None
) -> None:
    """
    Register a generic Kafka medallion (bronze/silver) + gold fanout pipeline with SDP/DLT.

    Usage (in a DLT notebook):
        from pipeline.kafka_medallion_pipeline import register_kafka_medallion_pipeline
        register_kafka_medallion_pipeline(spark, pipeline_spec)

    Notes:
    - This function defines decorated SDP tables dynamically at runtime (which is supported
      as long as the set of tables is static for a given spec).
    - Gold tables are driven by `fanout.tables`, so you get a predictable, static set.
    """

    dp = dp_module
    if dp is None:
        from pyspark import pipelines as dp  # type: ignore

    spec = KafkaMedallionPipelineSpec(**pipeline_spec)

    bronze_table = spec.tables.bronze
    silver_table = spec.tables.silver
    fanout_tables = spec.fanout.tables
    dead_letter_table = spec.fanout.dead_letter_table

    table_props = {"pipelines.autoOptimize.managed": "true", **spec.table_properties}
    # Enables Variant on Delta tables in DLT when Silver produces Variant
    if spec.silver.mode in ("variant_json", "schema_registry_avro"):
        table_props = {"delta.feature.variantType-preview": "supported", **table_props}

    # -------------------------
    # BRONZE: raw Kafka ingest
    # -------------------------
    @dp.table(
        name=bronze_table,
        comment="Bronze: raw Kafka messages with key/value and Kafka metadata",
        table_properties={"quality": "bronze", **table_props},
    )
    def _bronze_kafka_raw():
        df = _kafka_read_stream_df(spark, spec)
        return df.select(
            col("key"),
            col("value"),
            col("topic"),
            col("partition"),
            col("offset"),
            col("timestamp").alias("kafka_timestamp"),
            current_timestamp().alias("ingestion_timestamp"),
        )

    # -------------------------
    # SILVER: parsed value (Variant)
    # -------------------------
    live_ref = f"{spec.live_prefix}.{bronze_table}"

    @dp.table(
        name=silver_table,
        comment=f"Silver: parsed Kafka value (mode={spec.silver.mode}) plus Kafka metadata",
        table_properties={"quality": "silver", **table_props},
    )
    def _silver_kafka_parsed():
        bronze_df = spark.readStream.table(live_ref)

        if spec.silver.mode == "variant_json":
            cfg = spec.silver.variant_json
            parsed_df = _try_parse_json_variant(
                bronze_df, value_col=cfg.value_column, parsed_col=cfg.parsed_column
            )
            value_col = cfg.value_column
            parsed_col = cfg.parsed_column
        elif spec.silver.mode == "json":
            cfg = spec.silver.json
            schema = _struct_type_from_json_spec(cfg)
            value_str = col(cfg.value_column).cast("string")
            parsed_df = bronze_df.withColumn(cfg.parsed_column, from_json(value_str, schema))
            value_col = cfg.value_column
            parsed_col = cfg.parsed_column
        else:
            cfg = spec.silver.schema_registry_avro
            parsed_df = _parse_schema_registry_avro_to_variant(bronze_df, cfg=cfg)
            value_col = cfg.value_column
            parsed_col = cfg.parsed_column

        # Keep raw Kafka columns + parsed column
        out = parsed_df.select(
            col("key"),
            col("value"),
            col("topic"),
            col("partition"),
            col("offset"),
            col("kafka_timestamp"),
            col("ingestion_timestamp"),
            col(parsed_col),
        )
        if spec.silver.mode == "schema_registry_avro":
            out = out.select("*", col(SCHEMA_REGISTRY_DECODED_COLUMN))
        return out

    # -------------------------
    # GOLD: fan-out into tables
    # -------------------------
    silver_live_ref = f"{spec.live_prefix}.{silver_table}"
    key_expr = _fanout_key_expr(spec)
    key_col = expr(key_expr).cast("string")

    if spec.silver.mode == "variant_json":
        parsed_col_name = spec.silver.variant_json.parsed_column  # type: ignore[union-attr]
    elif spec.silver.mode == "schema_registry_avro":
        parsed_col_name = spec.silver.schema_registry_avro.parsed_column  # type: ignore[union-attr]
    else:
        parsed_col_name = spec.silver.json.parsed_column  # type: ignore[union-attr]

    def _define_gold_table(t: FanoutTableSpec) -> None:
        @dp.table(
            name=t.table,
            comment=f"Gold: fanout where ({key_expr}) == '{t.match}'",
            table_properties={"quality": "gold", **table_props},
        )
        def _gold():
            df = spark.readStream.table(silver_live_ref)
            matched = df.where(key_col == t.match)
            return _project_fanout_table(matched, t, spec=spec)

    for t in fanout_tables:
        _define_gold_table(t)

    match_values = [t.match for t in fanout_tables]

    @dp.table(
        name=dead_letter_table,
        comment="Gold: dead letter queue for parse errors or unmatched fanout keys",
        table_properties={"quality": "gold", **table_props},
    )
    def _gold_dead_letter_queue():
        df = spark.readStream.table(silver_live_ref)
        parsed_is_null = col(parsed_col_name).isNull()
        key_is_null = key_col.isNull()
        key_unmatched = ~key_col.isin(match_values)
        dlq = df.where(parsed_is_null | key_is_null | key_unmatched)
        return dlq if spec.fanout.include_kafka_metadata_in_dead_letter else dlq.select(
            col(parsed_col_name)
        )


