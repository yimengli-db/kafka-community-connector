"""
Helper utilities for fan-out patterns in SDP/DLT pipelines.

This module is intentionally small and pure-DataFrame so it can be reused by
pipeline builders (including `pipeline/kafka_medallion_pipeline.py`).
"""

from __future__ import annotations

from typing import Dict

from pyspark.sql import DataFrame
from pyspark.sql.functions import col


def fanout_by_field(df: DataFrame, field: str, mapping: Dict[str, str]) -> Dict[str, DataFrame]:
    """
    Split a DataFrame into multiple DataFrames by matching `field` against mapping keys.

    Args:
        df: input DataFrame
        field: column name to fan out on (e.g. "entity_type")
        mapping: mapping of {field_value: output_table_name}

    Returns:
        Dict of {output_table_name: filtered_df}
    """
    return {table_name: df.where(col(field) == value) for value, table_name in mapping.items()}

