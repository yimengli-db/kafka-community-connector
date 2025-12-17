"""
Databricks Delta Live Tables (Lakeflow) Pipeline
Implements a medallion architecture for Kafka ingestion:
- Bronze: Raw key/value from Kafka
- Silver: Parsed structured data
- Gold: Fan-out to domain-specific tables
"""

import pyspark
import pytz

from pyspark.sql.functions import concat, lit, col, to_json, struct
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from datetime import datetime

from pyspark import pipelines as dp
from pyspark.sql.functions import col, from_json, expr, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, BinaryType, LongType, TimestampType
from typing import Optional

# Configuration - these will be passed as pipeline parameters
KAFKA_BOOTSTRAP_SERVERS = spark.conf.get("kafka.bootstrap.servers")
KAFKA_TOPICS = spark.conf.get("kafka.topics")
API_KEY = spark.conf.get("kafka.api.key")
API_SECRET = spark.conf.get("kafka.api.secret")
ALLOWED_TOPICS = spark.conf.get("kafka.allowed_topics", "")
SCHEMA_REGISTRY_URL = spark.conf.get("schema.registry.url", "")
FANOUT_FIELD = spark.conf.get("fanout.field", "entity_type")

kafka_opts = {
  "kafka.bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
  "kafka.security.protocol":"SASL_SSL",
  "kafka.sasl.mechanism":"PLAIN",
  "kafka.sasl.jaas.config":f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{API_KEY}" password="{API_SECRET}";',
  "kafka.ssl.endpoint.identification.algorithm":"https",
  "startingOffsets": "earliest",
}

print(kafka_opts)

# =============================================================================
# BRONZE LAYER - Raw Kafka Ingestion
# =============================================================================

@dp.table(
    name="bronze_kafka_raw",
    comment="Raw Kafka messages with key and value as binary/string",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true"
    }
)
def bronze_kafka_raw():
    """
    Bronze table: Ingest raw Kafka messages
    Stores key, value, topic, partition, offset, timestamp as-is
    """
    # Parse topics
    topics_list = [t.strip() for t in KAFKA_TOPICS.split(",")]
    
    # Apply topic allowlist if specified
    if ALLOWED_TOPICS:
        allowed_list = [t.strip() for t in ALLOWED_TOPICS.split(",") if t.strip()]
        topics_list = [t for t in topics_list if t in allowed_list]
        print(f"Applied topic allowlist. Ingesting topics: {topics_list}")
    
    # Read from Kafka
    df = (
        spark.readStream
        .format("kafka")
        .options(**kafka_opts)
        #.option("subscribe", ",".join(topics_list))
        .option("subscribe", "topic_0")
        .load()
    )
    
    # Select and cast columns, keeping raw binary data
    return (
        df.select(
            col("key"),
            col("value"),
            col("topic"),
            col("partition"),
            col("offset"),
            col("timestamp").alias("kafka_timestamp"),
            current_timestamp().alias("ingestion_timestamp")
        )
    )

