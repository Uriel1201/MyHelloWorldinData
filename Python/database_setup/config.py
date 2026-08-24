# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "pyarrow>=25.0.1",
#     "python-dotenv>=1.2.3",
# ]
# ///
import os
from dataclasses import dataclass

import pyarrow as pa
from dotenv import load_dotenv

load_dotenv("env")

URI_POSTGRESQL = os.getenv("URI_POSTGRESQL")
URI_MYSQL = os.getenv("URI_MYSQL")
ODB_DSN = os.getenv("ODB_DSN")
ODB_USER = os.getenv("ODB_USER")
ODB_PASSWORD = os.getenv("ODB_PASSWORD")


@dataclass
class MyArrowTable:
    table: pa.Table
    alias: str
