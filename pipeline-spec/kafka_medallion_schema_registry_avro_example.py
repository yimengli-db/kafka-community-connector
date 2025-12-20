"""
Example: Kafka medallion (bronze/silver) + gold fanout using Confluent Schema Registry (Avro).

This example shows how to avoid hard-coding Avro schemas in code:
- Silver decodes Avro by fetching schemas from Schema Registry at runtime.
- The decoded record is converted into a Variant column `parsed`, so fanout expressions
  can keep using the familiar Variant syntax: `parsed:<field>::<type>`.
"""

from pipeline.kafka_medallion_pipeline import register_kafka_medallion_pipeline

# NOTE: In a DLT notebook, `spark` is available. This example assumes that.

# Kafka credentials/config
KAFKA_API_KEY = spark.conf.get("kafka.api.key")
KAFKA_API_SECRET = spark.conf.get("kafka.api.secret")
BOOTSTRAP_SERVERS = spark.conf.get("kafka.bootstrap.servers")

# Schema Registry credentials/config
SCHEMA_REGISTRY_URL = spark.conf.get("schema.registry.url")
SCHEMA_REGISTRY_API_KEY = spark.conf.get("schema.registry.api.key")
SCHEMA_REGISTRY_API_SECRET = spark.conf.get("schema.registry.api.secret")

schema_registry_options = {
    # If needed, include auth credentials:
    "confluent.schema.registry.basic.auth.credentials.source": "USER_INFO",
    "confluent.schema.registry.basic.auth.user.info": (
        f"{SCHEMA_REGISTRY_API_KEY}:{SCHEMA_REGISTRY_API_SECRET}"
    ),
    # PERMISSIVE yields null on failure (lets us try multiple subjects and coalesce)
    "mode": "PERMISSIVE",
    # Optional (example) - control evolution behavior as desired
    "schema.registry.schema.evolution.mode": "none",
}

pipeline_spec = {
    "tables": {"bronze": "bronze_kafka_raw", "silver": "silver_kafka_parsed"},
    "source": {
        "kafka_options": {
            "kafka.bootstrap.servers": BOOTSTRAP_SERVERS,
            "kafka.security.protocol": "SASL_SSL",
            "kafka.sasl.mechanism": "PLAIN",
            "kafka.sasl.jaas.config": (
                'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule '
                f'required username="{KAFKA_API_KEY}" password="{KAFKA_API_SECRET}";'
            ),
            "kafka.ssl.endpoint.identification.algorithm": "https",
            "startingOffsets": "earliest",
        },
        "topics": ["topic_0"],
    },
    "silver": {
        "mode": "schema_registry_avro",
        "schema_registry_avro": {
            "schema_registry_address": SCHEMA_REGISTRY_URL,
            # Try subjects in order; the first successful decode becomes `parsed`.
            # This works well with RecordNameStrategy where one topic carries multiple record types.
            "subjects": [
                "com.example.events.Player-value",
                "com.example.events.Match-value",
            ],
            "options": schema_registry_options,
        },
    },
    "fanout": {
        # Fanout key derived from Variant column: parsed:entity_type::string
        "key_field": "entity_type",
        "tables": [
            {
                "match": "player",
                "table": "gold_players",
                # Project all Avro fields from Schema Registry (no explicit columns list).
                # The pipeline will also add kafka_timestamp + ingestion_timestamp automatically.
                "select_all_fields": True,
                # Optional: set to True to also include topic/partition/offset (timestamps are always included).
                "include_kafka_metadata": False,
            },
            {
                "match": "match",
                "table": "gold_matches",
                "select_all_fields": True,
                "include_kafka_metadata": False,
            },
        ],
        "dead_letter_table": "gold_dead_letter_queue",
    },
    "live_prefix": "LIVE",
}


def _resolve_conf_templates(d: dict, spark) -> dict:
    """
    Resolve ${conf:<key>} templates using spark.conf.get(<key>).
    This keeps the spec file portable while letting secrets live in pipeline config.
    """

    def resolve_value(v):
        if isinstance(v, str) and v.startswith("${conf:") and v.endswith("}"):
            key = v[len("${conf:") : -1]
            return spark.conf.get(key)
        return v

    out = {}
    for k, v in d.items():
        if isinstance(v, dict):
            out[k] = _resolve_conf_templates(v, spark)
        elif isinstance(v, list):
            out[k] = [resolve_value(x) for x in v]
        else:
            out[k] = resolve_value(v)
    return out


register_kafka_medallion_pipeline(spark, _resolve_conf_templates(pipeline_spec, spark))


