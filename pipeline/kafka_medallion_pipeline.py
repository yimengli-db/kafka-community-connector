from __future__ import annotations

from typing import Any, Dict, Optional

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp, expr, from_json
from pyspark.sql.types import StructType

from libs.kafka_medallion_spec import FanoutTableSpec, KafkaMedallionPipelineSpec


def _struct_type_from_spec(spec: KafkaMedallionPipelineSpec) -> StructType:
    if spec.silver.json is None:
        raise ValueError("silver.json is required when silver.mode == 'json'")

    json_spec = spec.silver.json
    if json_spec.schema_json is not None and json_spec.schema_ddl is not None:
        raise ValueError("Provide only one of silver.json.schema_json or silver.json.schema_ddl")

    if json_spec.schema_json is not None:
        return StructType.fromJson(json_spec.schema_json)

    if json_spec.schema_ddl is not None:
        # `fromDDL` is available in Spark 3.4+. This project targets pyspark>=3.5.
        return StructType.fromDDL(json_spec.schema_ddl)

    raise ValueError("silver.json requires either schema_json or schema_ddl")


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

    if spec.silver.mode == "variant":
        parsed_col = spec.silver.variant.parsed_column  # type: ignore[union-attr]
        return f"{parsed_col}:{spec.fanout.key_field}::string"

    # json mode: parsed is a struct; users can still override with key_expr if needed
    parsed_col = spec.silver.json.parsed_column  # type: ignore[union-attr]
    return f"{parsed_col}.{spec.fanout.key_field}"


def _project_fanout_table(df: DataFrame, table_spec: FanoutTableSpec) -> DataFrame:
    kafka_meta_cols = [
        col("topic"),
        col("partition"),
        col("offset"),
        col("kafka_timestamp"),
        col("ingestion_timestamp"),
    ]
    projected_cols = [expr(c.expr).alias(c.name) for c in table_spec.columns]
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
    if spec.silver.mode == "variant":
        # Enables Variant on Delta tables in DLT
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
    # SILVER: parsed value (Variant or Struct)
    # -------------------------
    live_ref = f"{spec.live_prefix}.{bronze_table}"

    @dp.table(
        name=silver_table,
        comment="Silver: parsed Kafka value (Variant or Struct) plus Kafka metadata",
        table_properties={"quality": "silver", **table_props},
    )
    def _silver_kafka_parsed():
        bronze_df = spark.readStream.table(live_ref)
        if spec.silver.mode == "variant":
            variant_cfg = spec.silver.variant
            value_str = col(variant_cfg.value_column).cast("string")
            try:
                # Databricks Runtime provides try_parse_json for Variant
                from pyspark.sql.functions import try_parse_json  # type: ignore

                parsed_df = bronze_df.withColumn(
                    variant_cfg.parsed_column, try_parse_json(value_str)
                )
            except Exception:
                # Fallback: rely on SQL function resolution at runtime
                parsed_df = bronze_df.withColumn(
                    variant_cfg.parsed_column,
                    expr(f"try_parse_json(CAST({variant_cfg.value_column} AS STRING))"),
                )
        else:
            json_cfg = spec.silver.json
            parsed_schema = _struct_type_from_spec(spec)
            value_str = col(json_cfg.value_column).cast("string")
            parsed_df = bronze_df.withColumn(
                json_cfg.parsed_column, from_json(value_str, parsed_schema)
            )

        # Keep raw Kafka columns + parsed column
        return parsed_df.select(
            col("key"),
            col("value"),
            col("topic"),
            col("partition"),
            col("offset"),
            col("kafka_timestamp"),
            col("ingestion_timestamp"),
            col(spec.silver.variant.parsed_column)
            if spec.silver.mode == "variant"
            else col(spec.silver.json.parsed_column),
        )

    # -------------------------
    # GOLD: fan-out into tables
    # -------------------------
    silver_live_ref = f"{spec.live_prefix}.{silver_table}"
    key_expr = _fanout_key_expr(spec)
    key_col = expr(key_expr).cast("string")

    parsed_col_name = (
        spec.silver.variant.parsed_column
        if spec.silver.mode == "variant"
        else spec.silver.json.parsed_column
    )

    def _define_gold_table(t: FanoutTableSpec) -> None:
        @dp.table(
            name=t.table,
            comment=f"Gold: fanout where ({key_expr}) == '{t.match}'",
            table_properties={"quality": "gold", **table_props},
        )
        def _gold():
            df = spark.readStream.table(silver_live_ref)
            matched = df.where(key_col == t.match)
            return _project_fanout_table(matched, t)

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


