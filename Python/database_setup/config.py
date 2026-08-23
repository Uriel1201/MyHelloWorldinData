import os
from dotenv import load_dotenv
from dataclasses import dataclass
import pyarrow as pa

load_dotenv("/content/env")

URI_POSTGRESQL = os.getenv("URI_POSTGRESQL")
URI_MYSQL = os.getenv("URI_MYSQL")
ODB_DSN = os.getenv("ODB_DSN")
ODB_USER = os.getenv("ODB_USER")
ODB_PASSWORD = os.getenv("ODB_PASSWORD")
DATA_PATH = os.getenv("DATA_PATH", "data/csv/01")


@dataclass
class MyArrowTable:
    table: pa.Table
    alias: str
