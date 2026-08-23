import os
from dotenv import load_dotenv
from dataclasses import dataclass
import pyarrow as pa

load_dotenv()

URI_POSTGRESQL = os.getenv("URI_POSTGRESQL")
URI_MYSQL = os.getenv("URI_MYSQL")
ODB_DSN = os.getenv("ODB_DSN")
ODB_USER = os.getenv("ODB_USER")
ODB_PASSWORD = os.getenv("ODB_PASSWORD")


@dataclass
class MyArrowTable:
    table: pa.Table
    alias: str
