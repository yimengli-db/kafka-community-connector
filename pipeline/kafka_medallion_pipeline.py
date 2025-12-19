from __future__ import annotations

from typing import Any, Dict, Optional

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp, expr

from libs.kafka_medallion_spec import FanoutTableSpec, KafkaMedallionPipelineSpec


SILVER_VALUE_COLUMN = "value"
SILVER_PARSED_COLUMN = "parsed"


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

    return f"{SILVER_PARSED_COLUMN}:{spec.fanout.key_field}::string"


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
    # Enables Variant on Delta tables in DLT (Silver always uses Variant parsing)
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
        comment="Silver: parsed Kafka value (Variant) plus Kafka metadata",
        table_properties={"quality": "silver", **table_props},
    )
    def _silver_kafka_parsed():
        bronze_df = spark.readStream.table(live_ref)
        value_str = col(SILVER_VALUE_COLUMN).cast("string")
        try:
            # Databricks Runtime provides try_parse_json for Variant
            from pyspark.sql.functions import try_parse_json  # type: ignore

            parsed_df = bronze_df.withColumn(SILVER_PARSED_COLUMN, try_parse_json(value_str))
        except Exception:
            # Fallback: rely on SQL function resolution at runtime
            parsed_df = bronze_df.withColumn(
                SILVER_PARSED_COLUMN,
                expr(f"try_parse_json(CAST({SILVER_VALUE_COLUMN} AS STRING))"),
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
            col(SILVER_PARSED_COLUMN),
        )

    # -------------------------
    # GOLD: fan-out into tables
    # -------------------------
    silver_live_ref = f"{spec.live_prefix}.{silver_table}"
    key_expr = _fanout_key_expr(spec)
    key_col = expr(key_expr).cast("string")

    parsed_col_name = SILVER_PARSED_COLUMN

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


