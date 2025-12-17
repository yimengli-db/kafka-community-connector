from __future__ import annotations

from typing import Any, Dict, Optional

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp, from_json
from pyspark.sql.types import StructType

from libs.kafka_medallion_spec import KafkaMedallionPipelineSpec


def _struct_type_from_spec(spec: KafkaMedallionPipelineSpec) -> StructType:
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
    - Gold tables are driven by `fanout.mapping`, so you get a predictable, static set.
    """

    dp = dp_module
    if dp is None:
        from pyspark import pipelines as dp  # type: ignore

    spec = KafkaMedallionPipelineSpec(**pipeline_spec)

    bronze_table = spec.tables.bronze
    silver_table = spec.tables.silver
    gold_mapping = spec.fanout.mapping

    table_props = {"pipelines.autoOptimize.managed": "true", **spec.table_properties}

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
    # SILVER: parsed JSON
    # -------------------------
    json_cfg = spec.silver.json
    parsed_schema = _struct_type_from_spec(spec)
    live_ref = f"{spec.live_prefix}.{bronze_table}"

    @dp.table(
        name=silver_table,
        comment="Silver: parsed Kafka value as structured columns",
        table_properties={"quality": "silver", **table_props},
    )
    def _silver_kafka_parsed():
        bronze_df = spark.readStream.table(live_ref)
        # Parse Kafka value (binary) as string then JSON -> struct
        value_str = col(json_cfg.value_column).cast("string")
        parsed = bronze_df.withColumn("_parsed", from_json(value_str, parsed_schema))
        # Flatten parsed struct to top-level columns while keeping Kafka metadata
        return parsed.select(
            col("topic"),
            col("partition"),
            col("offset"),
            col("kafka_timestamp"),
            col("ingestion_timestamp"),
            col("_parsed.*"),
        )

    # -------------------------
    # GOLD: fan-out into tables
    # -------------------------
    silver_live_ref = f"{spec.live_prefix}.{silver_table}"
    fanout_field = spec.fanout.field

    def _define_gold_table(table_name: str, match_value: str) -> None:
        @dp.table(
            name=table_name,
            comment=f"Gold: fanout where {fanout_field} == {match_value}",
            table_properties={"quality": "gold", **table_props},
        )
        def _gold():
            df = spark.readStream.table(silver_live_ref)
            return df.where(col(fanout_field) == match_value)

    for value, table_name in gold_mapping.items():
        _define_gold_table(table_name=table_name, match_value=value)

    if spec.fanout.include_unknown:
        known_values = list(gold_mapping.keys())
        unknown_table = spec.fanout.unknown_table

        @dp.table(
            name=unknown_table,
            comment=f"Gold: fanout rows where {fanout_field} not in configured mapping",
            table_properties={"quality": "gold", **table_props},
        )
        def _gold_unknown():
            df = spark.readStream.table(silver_live_ref)
            return df.where(~col(fanout_field).isin(known_values))


