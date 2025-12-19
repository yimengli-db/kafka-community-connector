"""
Example: Generic Kafka medallion (bronze/silver) + gold fanout SDP/DLT pipeline.

In Databricks DLT, configure the required Spark confs / pipeline parameters and
then import & call `register_kafka_medallion_pipeline`.
"""

from pipeline.kafka_medallion_pipeline import register_kafka_medallion_pipeline

# NOTE: In a DLT notebook, `spark` is available. This example assumes that.

# NOTE: In a DLT notebook, `spark` is available. This example assumes that.
API_KEY = spark.conf.get("kafka.api.key")
API_SECRET = spark.conf.get("kafka.api.secret")

pipeline_spec = {
    "tables": {"bronze": "bronze_kafka_raw", "silver": "silver_kafka_parsed"},
    "source": {
        # Prefer passing secrets via pipeline config / spark.conf and templating them in here.
        "kafka_options": {
            "kafka.bootstrap.servers": "pkc-921jm.us-east-2.aws.confluent.cloud:9092",
            "kafka.security.protocol": "SASL_SSL",
            "kafka.sasl.mechanism": "PLAIN",
            "kafka.sasl.jaas.config": f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{API_KEY}" password="{API_SECRET}";',
            "kafka.ssl.endpoint.identification.algorithm": "https",
            "startingOffsets": "earliest",
        },
        # Provide either topics OR subscribe_pattern
        "topics": ["topic_0"],
        # Optional allowlist filter (applied to topics)
        "allowed_topics": [],
    },
    "fanout": {
        # Destination is based on a field inside the parsed Variant value.
        # Equivalent to: expr("parsed:entity_type::string")
        "key_field": "entity_type",
        "tables": [
            {
                "match": "order",
                "table": "gold_orders",
                "columns": [
                    {"name": "order_id", "expr": "parsed:order_id::string"},
                    {"name": "amount", "expr": "parsed:amount::double"},
                ],
            },
            {
                "match": "customer",
                "table": "gold_customers",
                "columns": [
                    {"name": "customer_id", "expr": "parsed:customer_id::string"},
                    {"name": "name", "expr": "parsed:name::string"},
                ],
            },
        ],
        # Rows with parse errors OR unmatched keys land here
        "dead_letter_table": "gold_dead_letter_queue",
    },
    "live_prefix": "LIVE",
    "table_properties": {"pipelines.autoOptimize.managed": "true"},
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


