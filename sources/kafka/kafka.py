from locale import dcgettext
from pyspark.sql.types import *
from datetime import datetime
from typing import Dict, List, Iterator


class LakeflowConnect:
    def __init__(self, options: dict) -> None:
        self.options = options

    def list_tables(self) -> List[str]:
        return [
            
        ]

    def get_table_schema(
        self, table_name: str, table_options: Dict[str, str]
    ) -> StructType:
        """
        Fetch the schema of a table.
        """
        

    def read_table_metadata(
        self, table_name: str, table_options: Dict[str, str]
    ) -> dict:
        """
        Fetch the metadata of a table.
        """

    def read_table(
        self, table_name: str, start_offset: dict, table_options: Dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Map table names to their API endpoints and response keys
        """

    def _read_incremental(self, table_name: str, config: dict, start_offset: dict):
        """Read data from incremental API endpoints"""
       

    def _read_paginated(self, table_name: str, config: dict, start_offset: dict):
        """Read data from paginated API endpoints"""
        