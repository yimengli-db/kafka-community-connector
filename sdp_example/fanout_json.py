from pyspark import pipelines as dp
from pyspark.sql.functions import col, try_parse_json, expr
from utilities import utils

@dp.table(
    table_properties={
        "delta.feature.variantType-preview": "supported"
    }
)
def bronze_kafka_parsed():
    return (
        spark.read.table("bronze_kafka_raw")
        .withColumn("parsed", try_parse_json(col("value").cast("string")))
    )

# dead letter queue
@dp.table(
    table_properties={
        "delta.feature.variantType-preview": "supported"
    }
)
def dead_letter_queue():
    return (
        spark.read.table("bronze_kafka_parsed")
        .filter(col("parsed").isNull())
    )

@dp.table(
    table_properties={
        "delta.feature.variantType-preview": "supported"
    }
)
def players():
    return (
        spark.read.table("bronze_kafka_parsed")
        .filter(expr("parsed:entity_type::string = 'player'"))
        .withColumn("player_id", expr("parsed:player_id::string"))
        .withColumn("player_name", expr("parsed:player_name::string"))
        .select("player_id", "player_name")
    )

@dp.table(
    table_properties={
        "delta.feature.variantType-preview": "supported"
    }
)
def matches():
    return (
        spark.read.table("bronze_kafka_parsed")
        .filter(expr("parsed:entity_type::string = 'match'"))
        .withColumn("match_id", expr("parsed:match_id::string"))
        .withColumn("winner_id", expr("parsed:winner_id::string"))
        .withColumn("loser_id", expr("parsed:loser_id::string"))
        .withColumn("score", expr("parsed:score::string"))
        .select("match_id", "winner_id", "loser_id", "score")
    )

