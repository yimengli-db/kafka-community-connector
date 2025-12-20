from pyspark import pipelines as dp
from utilities import utils
from pyspark.sql.functions import col, when
from pyspark.sql.avro.functions import from_avro

# Kafka Schema Registry settings
schema_registry_url = "https://psrc-e0919.us-east-2.aws.confluent.cloud"
schema_registry_api_key = spark.conf.get("schema.registry.api.key")
schema_registry_api_secret = spark.conf.get("schema.registry.api.secret")

# Setup schema-registry options for Protobuf
schema_registry_options = {
    "schema.registry.address": schema_registry_url,
    "schema.registry.schema.evolution.mode" : "none",
    "mode": "PERMISSIVE", 
    "confluent.schema.registry.basic.auth.credentials.source": "USER_INFO",
    "confluent.schema.registry.basic.auth.user.info": f"{schema_registry_api_key}:{schema_registry_api_secret}"
}

# Define subjects based on RecordNameStrategy
player_subject = "com.example.events.Player-value"
match_subject  = "com.example.events.Match-value"

@dp.table
def bronze_kafka_deserialized():

    df = spark.read.table("bronze_kafka_raw") \
        .withColumn(
            "decoded_player",
            from_avro(
                col("value"),
                subject = player_subject,
                schemaRegistryAddress = schema_registry_url,
                options = schema_registry_options
                )
        ) \
        .withColumn(
            "decoded_match",
            from_avro(
                col("value"),
                subject = match_subject,
                schemaRegistryAddress = schema_registry_url,
                options = schema_registry_options
            )
        )
    
    return df

# dead letter queue
@dp.table
def dead_letter_queue_sr():
    return (
        spark.read.table("bronze_kafka_deserialized")
        .filter((col("decoded_player.entity_type").isNull()) | (col("decoded_match.entity_type").isNull()))
    
    )

@dp.table
def players_sr():
    return (
        spark.read.table("bronze_kafka_deserialized")
        .filter((col("decoded_player.entity_type") == "player"))
        .select("kafka_timestamp", "ingestion_timestamp", "decoded_player.*")
    
    )

@dp.table
def matches_sr():
    return (
        spark.read.table("bronze_kafka_deserialized")
        .filter((col("decoded_match.entity_type") == "match"))
        .select("kafka_timestamp", "ingestion_timestamp", "decoded_match.*")
    )
