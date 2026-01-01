# Kafka Pipeline Specification

This document describes the configuration spec for Kafka ingestion pipelines.

## Overview

The pipeline reads from Kafka topics and writes to Delta tables using DLT/SDP. It supports:
- Single table ingestion (one topic -> one table)
- Multiple table ingestion (multiple topics -> one table)
- Fanout routing (one (or more) topic(s) -> multiple tables based on a key)
- Schema parsing (JSON, Avro, Protobuf, or raw string) with schema registry support

---

## Spec Structure

```json
{
  "connection_name": "my_kafka",
  "sources": [ ... ],
  "table_properties": { ... }
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `connection_name` | string | Yes | Identifier for the Kafka connection |
| `sources` | array | Yes | List of source configurations (see below) |
| `table_properties` | object | No | DLT table properties applied to all tables |

---

## Source Configuration

Each source reads from Kafka and writes to one or more tables.

```json
{
  "kafka_options": { ... },
  "destination_table": "events",
  "schema_options": { ... },
  "fanout_options": { ... },
  "include_kafka_metadata": true
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `kafka_options` | object | Yes | Kafka consumer options (see [Databricks Kafka docs](https://docs.databricks.com/aws/en/connect/streaming/kafka)) |
| `destination_table` | string | Conditional | Target table name. Required if `fanout_options` is not set. |
| `schema_options` | object | No | How to parse key/value (see below) |
| `fanout_options` | object | No | Route records to multiple tables (see below) |
| `include_kafka_metadata` | bool | No | Include `__kafka_metadata` column (default: `true`) |

### kafka_options

Pass any options supported by Databricks's Kafka source. Common options:

```json
{
  "kafka.bootstrap.servers": "broker:9092",
  "subscribe": "topic1,topic2",
  "startingOffsets": "earliest"
}
```

See: [Databricks Kafka docs](https://docs.databricks.com/aws/en/connect/streaming/kafka)

---

## Schema Options

Parse the Kafka key and/or value into structured data.

```json
{
  "schema_options": {
    "key": { ... },
    "value": { ... }
  }
}
```

Each of `key` and `value` supports these formats:

### STRING

Cast binary to string. No schema required.

```json
{ "format": "STRING" }
```

### JSON

Parse JSON with a DDL schema.

```json
{
  "format": "JSON",
  "schema": "id INT, name STRING, timestamp TIMESTAMP"
}
```

### AVRO

Parse Avro using either an inline schema or Schema Registry.

**Inline schema:**
```json
{
  "format": "AVRO",
  "schema": "{\"type\": \"record\", \"name\": \"Event\", \"fields\": [...]}"
}
```

**Schema Registry:**
```json
{
  "format": "AVRO",
  "schema_registry_options": {
    "schema_registry_address": "https://sr.example.com",
    "subject": "events-value"
  }
}
```

### PROTOBUF

Parse Protobuf using either an inline schema or Schema Registry.

**Inline schema:**
```json
{
  "format": "PROTOBUF",
  "schema": "message Event { string id = 1; int64 timestamp = 2; }"
}
```

**Schema Registry:**
```json
{
  "format": "PROTOBUF",
  "schema_registry_options": {
    "schema_registry_address": "https://sr.example.com",
    "subject": "events-value"
  }
}
```

### Schema Registry Options

For Avro/Protobuf with Schema Registry, you can pass additional options. The `schema_registry_options` object accepts any options supported by Spark's `from_avro` or `from_protobuf` functions.

See: [Confluent Schema Registry](https://docs.confluent.io/platform/current/schema-registry/index.html)

---

## Fanout Options

Route records to different tables based on a field value. Useful when a single topic contains multiple entity types.

```json
{
  "fanout_options": {
    "key_expr": "value.entity_type",
    "routes": [
      { "match": "user", "table": "users" },
      { "match": "order", "table": "orders" }
    ],
    "default_table": "unmatched",
    "case_sensitive": false
  }
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `key_expr` | string | Yes | Spark SQL expression to extract routing key |
| `routes` | array | Yes | List of `{match, table}` pairs |
| `default_table` | string | No | Table for records that don't match any route |
| `case_sensitive` | bool | No | Case-sensitive matching (default: `false`) |

### key_expr Examples

The expression is evaluated against the parsed record. You can reference:
- Raw Kafka columns: `topic`, `partition`, `offset`, `timestamp`
- Parsed key/value fields: `value.entity_type`, `key.id`

```
"value.entity_type"           -- Field from parsed value struct
"topic"                       -- Route by Kafka topic name
"CONCAT(topic, '_', key.id)"  -- Combine multiple fields
```

---

## Examples

### Simple: One Topic → One Table

```json
{
  "connection_name": "my_kafka",
  "sources": [
    {
      "kafka_options": {
        "kafka.bootstrap.servers": "broker:9092",
        "subscribe": "events"
      },
      "destination_table": "events_raw"
    }
  ]
}
```

### Parsed JSON Value

```json
{
  "connection_name": "my_kafka",
  "sources": [
    {
      "kafka_options": {
        "kafka.bootstrap.servers": "broker:9092",
        "subscribe": "orders"
      },
      "destination_table": "orders",
      "schema_options": {
        "value": {
          "format": "JSON",
          "schema": "order_id STRING, amount DOUBLE, customer_id STRING"
        }
      }
    }
  ]
}
```

### Fanout by Entity Type

```json
{
  "connection_name": "my_kafka",
  "sources": [
    {
      "kafka_options": {
        "kafka.bootstrap.servers": "broker:9092",
        "subscribe": "events"
      },
      "schema_options": {
        "value": {
          "format": "JSON",
          "schema": "entity_type STRING, payload STRING"
        }
      },
      "fanout_options": {
        "key_expr": "value.entity_type",
        "routes": [
          { "match": "user", "table": "users" },
          { "match": "order", "table": "orders" },
          { "match": "product", "table": "products" }
        ],
        "default_table": "unmatched_events"
      }
    }
  ]
}
```

### Avro with Schema Registry

```json
{
  "connection_name": "my_kafka",
  "sources": [
    {
      "kafka_options": {
        "kafka.bootstrap.servers": "broker:9092",
        "subscribe": "avro-events"
      },
      "destination_table": "events",
      "schema_options": {
        "value": {
          "format": "AVRO",
          "schema_registry_options": {
            "schema_registry_address": "https://schema-registry.example.com",
            "subject": "avro-events-value"
          }
        }
      }
    }
  ]
}
```

